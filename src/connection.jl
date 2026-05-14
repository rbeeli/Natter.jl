function _parse_server_urls(urls; drop_empty::Bool=false)::Vector{String}
    servers = String[]
    for raw in urls
        url = strip(String(raw))
        if isempty(url)
            drop_empty && continue
            throw(ArgumentError("server URL cannot be empty"))
        end
        push!(servers, url)
    end
    isempty(servers) && throw(ArgumentError("at least one server URL is required"))
    servers
end

function _parse_options(url_or_urls; kwargs...)
    servers =
        if isnothing(url_or_urls)
            [DEFAULT_URL]
        elseif url_or_urls isa AbstractString
            _parse_server_urls(split(String(url_or_urls), ","; keepempty=false); drop_empty=true)
        elseif url_or_urls isa Union{AbstractVector,Tuple}
            _parse_server_urls(url_or_urls)
        else
            throw(ArgumentError("servers must be a string, vector, or tuple of strings"))
        end
    ConnectOptions(; servers, kwargs...)
end

_write_transport_for_options(io, opts::ConnectOptions) =
    max(0, opts.write_buffer_size) == 0 ? io : BufferedWriteIO(io)

function connect(url_or_urls=nothing; kwargs...)
    opts = _parse_options(url_or_urls; kwargs...)
    client = Client(
        opts,
        [Server(s) for s in opts.servers],
        nothing,
        nothing,
        ConnectionStatus.DISCONNECTED,
        ServerInfo(),
        nothing,
        nothing,
        nothing,
        ReentrantLock(),
        ReentrantLock(),
        Channel{Bool}(1),
        nothing,
        0,
        Dict{Int,Subscription}(),
        nothing,
        ReentrantLock(),
        IOBuffer(),
        0,
        PongWaiterQueue(),
        nothing,
        nothing,
        nothing,
        0,
        Stats(),
        MersenneTwister(rand(UInt)),
        0,
    )
    _connect_initial!(client)
    client
end

function _normalize_url(url::String)
    contains(url, "://") ? url : "nats://$url"
end

function _normalize_discovered_url(raw_url::AbstractString, base_url::Union{String,Nothing})::String
    url = String(raw_url)
    contains(url, "://") && return url
    isnothing(base_url) && return _normalize_url(url)

    base = URI(_normalize_url(base_url))
    scheme = isempty(base.scheme) ? "nats" : String(base.scheme)
    userinfo = isempty(base.userinfo) ? "" : "$(base.userinfo)@"
    "$scheme://$userinfo$url"
end

function _host_is_ip(host::AbstractString)::Bool
    value = String(host)
    if startswith(value, "[") && endswith(value, "]")
        value = value[2:end-1]
    end

    if occursin(':', value)
        try
            Sockets.IPv6(value)
            return true
        catch
            return false
        end
    end

    if occursin('.', value)
        parts = split(value, '.')
        length(parts) == 4 || return false
        for part in parts
            isempty(part) && return false
            all(isdigit, part) || return false
            parsed = tryparse(Int, part)
            isnothing(parsed) && return false
            0 <= parsed <= 255 || return false
        end
        return true
    end
    false
end

function _server_parts(url::String)
    uri = URI(_normalize_url(url))
    scheme = isempty(uri.scheme) ? "nats" : uri.scheme
    scheme in ("nats", "tls") || throw(UnsupportedFeatureError("transport scheme $scheme"))
    host = String(uri.host)
    isempty(host) && throw(ArgumentError("server host is missing in $url"))
    port = isempty(uri.port) ? 4222 : parse(Int, uri.port)
    user = nothing
    password = nothing
    if !isempty(uri.userinfo)
        pieces = split(uri.userinfo, ":"; limit=2)
        user = unescapeuri(String(first(pieces)))
        password = length(pieces) == 2 ? unescapeuri(String(last(pieces))) : nothing
    end
    scheme, host, port, user, password
end

_tls_hostname(server::Server, host::String)::String = something(server.tls_name, host)

function _current_tls_name_for_discovery(current_server::Union{Server,Nothing},
                                         base_url::Union{String,Nothing})::Union{String,Nothing}
    if !isnothing(current_server)
        !isnothing(current_server.tls_name) && return current_server.tls_name
        _, host, _, _, _ = _server_parts(current_server.url)
        return _host_is_ip(host) ? nothing : host
    end
    isnothing(base_url) && return nothing

    _, host, _, _, _ = _server_parts(base_url)
    _host_is_ip(host) ? nothing : host
end

function _discovery_uses_tls(opts::ConnectOptions, info::ServerInfo,
                             current_server::Union{Server,Nothing},
                             base_url::Union{String,Nothing})::Bool
    !isnothing(current_server) && !isnothing(current_server.tls_name) && return true
    source_url = !isnothing(current_server) ? current_server.url : base_url
    scheme = isnothing(source_url) ? "nats" : first(_server_parts(source_url))
    scheme == "tls" || opts.tls_required || opts.tls_first === true || info.tls_required === true
end

function _discovered_tls_name(url::String, current_server::Union{Server,Nothing},
                              base_url::Union{String,Nothing}, tls_active::Bool)::Union{String,Nothing}
    tls_active || return nothing
    _, host, _, _, _ = _server_parts(url)
    _host_is_ip(host) ? _current_tls_name_for_discovery(current_server, base_url) : nothing
end

function _tls_first_for_connection(opts::ConnectOptions, scheme::AbstractString)::Bool
    isnothing(opts.tls_first) ? scheme == "tls" : opts.tls_first
end

function _tls_authmode(opts::ConnectOptions)::Int
    opts.tls_verify ? MbedTLS.MBEDTLS_SSL_VERIFY_REQUIRED : MbedTLS.MBEDTLS_SSL_VERIFY_NONE
end

function _tls_config(opts::ConnectOptions)
    entropy = MbedTLS.Entropy()
    rng = MbedTLS.CtrDrbg()
    MbedTLS.seed!(rng, entropy)
    conf = MbedTLS.SSLConfig()
    MbedTLS.config_defaults!(conf)
    MbedTLS.rng!(conf, rng)
    MbedTLS.authmode!(conf, _tls_authmode(opts))
    if isnothing(opts.tls_ca_path)
        MbedTLS.ca_chain!(conf)
    else
        MbedTLS.ca_chain!(conf, MbedTLS.crt_parse_file(opts.tls_ca_path))
    end
    if !isnothing(opts.tls_cert_path) && !isnothing(opts.tls_key_path)
        MbedTLS.own_cert!(conf, MbedTLS.crt_parse_file(opts.tls_cert_path), MbedTLS.parse_keyfile(opts.tls_key_path))
    end
    conf
end

function _connect_tcp(host::String, port::Int, timeout::Real)
    ch = Channel{Union{Sockets.TCPSocket,Exception}}(1)
    timed_out = Threads.Atomic{Bool}(false)
    task = @async begin
        try
            sock = Sockets.connect(host, port)
            if timed_out[]
                close_errors = Any[]
                _close_resource!(close_errors, "close timed-out connect socket", sock)
                _warn_timeout_cleanup_errors("connect to $host:$port", close_errors)
            else
                put!(ch, sock)
            end
        catch err
            timed_out[] || put!(ch, err)
        end
    end
    result = timedwait(timeout; pollint=0.01) do
        isready(ch)
    end
    if result == :timed_out
        timed_out[] = true
        @async begin
            errors = Any[]
            _wait_task!(errors, "stop timed-out connect to $host:$port task", task;
                        interrupt=true, interrupt_first=true)
            if isready(ch)
                late = take!(ch)
                late isa Sockets.TCPSocket && _close_resource!(errors, "close timed-out connect socket", late)
            end
            _warn_timeout_cleanup_errors("connect to $host:$port", errors)
        end
        throw(TimeoutError("connect to $host:$port timed out"))
    end
    value = take!(ch)
    value isa Exception && throw(value)
    value
end

function _warn_timeout_cleanup_errors(operation::String, errors::Vector)
    for err in errors
        if err isa CleanupError
            @warn "Natter timeout cleanup failed" operation=err.operation cause=err.cause
        else
            @warn "Natter timeout cleanup failed" operation exception=err
        end
    end
    nothing
end

_cleanup_errors(result::Nothing) = Any[]
_cleanup_errors(result::AbstractVector) = Any[result...]
_cleanup_errors(result) = Any[result]

function _request_task_stop!(errors::Vector, operation::String, task::Union{Task,Nothing})::Bool
    (isnothing(task) || istaskdone(task) || task === current_task()) && return false
    try
        # Base.throwto yields to the target and can block the caller; scheduling
        # the exception requests interruption without adding another stuck task.
        schedule(task, InterruptException(); error=true)
        return true
    catch err
        istaskdone(task) && return false
        push!(errors, CleanupError("interrupt $operation", err))
        return false
    end
end

_task_interrupted_error(err)::Bool =
    err isa InterruptException ||
    (err isa TaskFailedException && err.task.result isa InterruptException)

function _record_stopped_task_error!(errors::Vector, operation::String, task::Task, interrupted::Bool)
    try
        wait(task)
    catch err
        interrupted && _task_interrupted_error(err) && return errors
        push!(errors, CleanupError(operation, err))
    end
    errors
end

function _wait_task!(errors::Vector, operation::String, task::Union{Task,Nothing};
                     timeout::Real=0.5, interrupt::Bool=false, interrupt_first::Bool=false)
    (isnothing(task) || istaskdone(task) || task === current_task()) && return errors

    interrupted = interrupt && interrupt_first && _request_task_stop!(errors, operation, task)
    result = timedwait(timeout; pollint=0.005) do
        istaskdone(task)
    end
    if result == :timed_out && interrupt && !interrupted
        interrupted = _request_task_stop!(errors, operation, task)
        result = timedwait(min(timeout, 0.5); pollint=0.005) do
            istaskdone(task)
        end
    end
    if result == :timed_out
        push!(errors, CleanupError(operation, TimeoutError("$operation timed out")))
        return errors
    end
    _record_stopped_task_error!(errors, operation, task, interrupted)
end

function _schedule_timeout_cleanup(operation::String, cleanup::Function,
                                   report_cleanup_errors::Function=errors -> _warn_timeout_cleanup_errors(operation, errors);
                                   task::Union{Task,Nothing}=nothing)
    @async begin
        errors = Any[]
        try
            append!(errors, _cleanup_errors(cleanup()))
        catch err
            push!(errors, CleanupError("timeout cleanup after $operation", err))
        end
        _wait_task!(errors, "stop timed-out $operation task", task; interrupt=true, interrupt_first=true)
        if !isempty(errors)
            try
                report_cleanup_errors(errors)
            catch err
                @warn "Natter timeout cleanup error reporter failed" operation exception=(err, catch_backtrace())
            end
        end
    end
    nothing
end

function _run_with_timeout(f::Function, operation::String, timeout::Real, cleanup::Function,
                           report_cleanup_errors::Function=errors -> _warn_timeout_cleanup_errors(operation, errors))
    if timeout <= 0
        _schedule_timeout_cleanup(operation, cleanup, report_cleanup_errors)
        throw(TimeoutError("$operation timed out"))
    end
    ch = Channel{Tuple{Bool,Any}}(1)
    timed_out = Threads.Atomic{Bool}(false)
    task = @async begin
        try
            value = f()
            timed_out[] || put!(ch, (true, value))
        catch err
            timed_out[] || put!(ch, (false, err))
        end
    end
    result = timedwait(timeout; pollint=0.01) do
        isready(ch)
    end
    if result == :timed_out
        timed_out[] = true
        _schedule_timeout_cleanup(operation, cleanup, report_cleanup_errors; task)
        throw(TimeoutError("$operation timed out"))
    end
    ok, value = take!(ch)
    ok || throw(value)
    value
end

_remaining_timeout(deadline::Float64)::Float64 = max(0.0, deadline - time())

function _write_timeout_error(operation::String)
    TimeoutError("$operation timed out")
end

function _write_timeout_matches(active, io)::Bool
    isnothing(active) && return false
    active === io && return true
    _underlying_transport(active) === _underlying_transport(io)
end

function _write_timeout_transports(client::Client, io)
    @lock client.lock begin
        write_io = client.write_io
        _write_timeout_matches(write_io, io) || return nothing, nothing, nothing
        client.read_io, write_io, client.socket
    end
end

function _abort_timed_out_write(client::Client, io, operation::String)
    read_io, write_io, sock = _write_timeout_transports(client, io)
    isnothing(read_io) && isnothing(write_io) && isnothing(sock) && return nothing
    # The timed-out writer is holding write_lock; closing the active transport is
    # the deadline breaker that lets normal reconnect/close cleanup take over.
    errors = _close_transport(read_io, write_io, sock)
    _report_cleanup_errors(client, errors)
    nothing
end

function _ensure_write_watchdog_locked!(client::Client)
    task = client.write_timeout_task
    if isnothing(task) || istaskdone(task)
        client.write_timeout_task = @async _write_watchdog_loop(client)
    end
    nothing
end

function _begin_write_timeout_locked!(client::Client, io, operation::String)::Int
    epoch = client.write_epoch[] + 1
    client.write_epoch[] = epoch
    client.write_timeout_io[] = io
    client.write_timeout_operation[] = operation
    client.write_deadline[] = time() + client.options.write_timeout
    _ensure_write_watchdog_locked!(client)
    epoch
end

function _finish_write_timeout!(client::Client, epoch::Int)::Bool
    @lock client.lock begin
        timed_out = client.write_timed_out_epoch[] == epoch
        if client.write_epoch[] == epoch
            client.write_deadline[] = Inf
            client.write_timeout_io[] = nothing
            client.write_timeout_operation[] = ""
        end
        timed_out
    end
end

function _write_watchdog_loop(client::Client)
    while true
        deadline = client.write_deadline[]
        observed_epoch = client.write_epoch[]
        if !isfinite(deadline)
            status(client) == ConnectionStatus.CLOSED && return nothing
            sleep(0.05)
            continue
        end

        remaining = deadline - time()
        if remaining > 0
            sleep(min(remaining, 0.05))
            continue
        end

        io = nothing
        operation = ""
        epoch = 0
        @lock client.lock begin
            if client.write_epoch[] == observed_epoch &&
               client.write_deadline[] == deadline &&
               isfinite(client.write_deadline[])
                epoch = observed_epoch
                client.write_timed_out_epoch[] = epoch
                io = client.write_timeout_io[]
                operation = client.write_timeout_operation[]
            end
        end
        isnothing(io) || _abort_timed_out_write(client, io, operation)
        @lock client.lock begin
            if epoch != 0 && client.write_epoch[] == epoch
                client.write_deadline[] = Inf
                client.write_timeout_io[] = nothing
                client.write_timeout_operation[] = ""
            end
        end
    end
end

function _run_transport_write(f::Function, client::Client, io, operation::String)
    epoch = @lock client.lock _begin_write_timeout_locked!(client, io, operation)
    try
        result = f()
        _finish_write_timeout!(client, epoch) && throw(_write_timeout_error(operation))
        return result
    catch err
        _finish_write_timeout!(client, epoch) && throw(_write_timeout_error(operation))
        rethrow()
    end
end

function _tls_wrap(sock, opts::ConnectOptions, hostname::String)
    conf = _tls_config(opts)
    ctx = MbedTLS.SSLContext()
    MbedTLS.setup!(ctx, conf)
    MbedTLS.set_bio!(ctx, sock)
    if isdefined(MbedTLS, :hostname!)
        getfield(MbedTLS, :hostname!)(ctx, hostname)
    end
    MbedTLS.handshake(ctx)
    ctx
end

function _throw_errors(errors::Vector)
    isempty(errors) && return nothing
    length(errors) == 1 ? throw(first(errors)) : throw(Base.CompositeException(errors))
end

function _record_error!(client::Client)
    _stat_add!(client.stats.errors)
    nothing
end

function _record_in!(client::Client, bytes::Int)
    _stat_add!(client.stats.in_msgs)
    _stat_add!(client.stats.in_bytes, bytes)
    nothing
end

function _record_out!(client::Client, bytes::Int)
    _stat_add!(client.stats.out_msgs)
    _stat_add!(client.stats.out_bytes, bytes)
    nothing
end

function _record_drop!(client::Client)
    _stat_add!(client.stats.dropped_msgs)
    nothing
end

function _record_reconnect!(client::Client)
    _stat_add!(client.stats.reconnects)
    nothing
end

function _signal_flusher_locked(client::Client)
    ch = @lock client.lock client.flush_signal
    isready(ch) || put!(ch, true)
    nothing
end

function _wake_flusher(client::Client)
    @lock client.write_lock _signal_flusher_locked(client)
    nothing
end

function _flush_buffered_writes(client::Client; allow_missing::Bool=false)
    @lock client.write_lock begin
        io = @lock client.lock client.write_io
        if isnothing(io)
            allow_missing && return false
            throw(ConnectionClosedError("connection transport is closed"))
        end
        _flush_buffered_writes_to_io(client, io)
    end
    true
end

function _flush_buffered_writes_to_io(client::Client, io::WriteIO) where {WriteIO<:IO}
    _flush_write_io(client, io)
    nothing
end

function _reserve_pending_bytes_locked!(client::Client, bytes::Int)
    projected = client.pending_bytes + bytes
    if projected > client.options.pending_size
        throw(OutboundBufferLimitError(client.options.pending_size, projected))
    end
    client.pending_bytes = projected
    nothing
end

function _reserve_pending_bytes!(client::Client, bytes::Int)
    @lock client.lock _reserve_pending_bytes_locked!(client, bytes)
    nothing
end

function _release_pending_bytes!(client::Client, bytes::Int)
    bytes <= 0 && return nothing
    @lock client.lock client.pending_bytes = max(0, client.pending_bytes - bytes)
    nothing
end

function _clear_pending_buffer_locked!(client::Client)
    empty!(client.pending)
    client.pending_bytes = 0
    nothing
end

function _clear_pending_buffer!(client::Client)
    @lock client.lock _clear_pending_buffer_locked!(client)
    nothing
end

_replayable_bytes(::IO) = 0
function _replayable_bytes(io::BufferedWriteIO)
    bytes = 0
    for (first, last) in io.replayable_ranges
        bytes += last - first + 1
    end
    bytes
end

function _flush_write_io(client::Client, io)
    replayed = _replayable_bytes(io)
    _run_transport_write(client, io, "transport flush") do
        flush(io)
    end
    _release_pending_bytes!(client, replayed)
    nothing
end

function _flush_write_io(client::Client, io::BufferedWriteIO)
    _ensure_open(io)
    replayed = _replayable_bytes(io)
    n = position(io.buffer)
    transport = _underlying_transport(io)
    _run_transport_write(client, transport, "transport flush") do
        n > 0 && _write_buffered_bytes(transport, io.buffer.data, n)
        flush(transport)
    end
    truncate(io.buffer, 0)
    seekstart(io.buffer)
    empty!(io.replayable_ranges)
    _release_pending_bytes!(client, replayed)
    nothing
end

function _flush_or_signal_locked(client::Client, io; force_flush::Bool=false)
    buffered = _buffered_bytes(io)
    threshold = max(0, client.options.write_buffer_size)
    if force_flush || (buffered > 0 && threshold == 0) || (threshold > 0 && buffered >= threshold)
        _flush_write_io(client, io)
    elseif buffered > 0
        _signal_flusher_locked(client)
    end
    nothing
end

function _write_raw(client::Client, data::Union{AbstractString,Vector{UInt8}}; force_flush::Bool=false)
    @lock client.write_lock begin
        io = @lock client.lock client.write_io
        isnothing(io) && throw(ConnectionClosedError("connection transport is closed"))
        _write_raw_to_io(client, io, data; force_flush)
    end
    nothing
end

function _write_raw_to_io(client::Client, io::WriteIO, data::Union{AbstractString,Vector{UInt8}};
                          force_flush::Bool=false) where {WriteIO<:IO}
    _write_raw_data_to_io(client, io, data)
    _flush_or_signal_locked(client, io; force_flush)
    nothing
end

function _write_raw_data_to_io(client::Client, io::BufferedWriteIO, data::Union{AbstractString,Vector{UInt8}})
    write(io, data)
    nothing
end

function _write_raw_data_to_io(client::Client, io::IO, data::Union{AbstractString,Vector{UInt8}})
    _run_transport_write(client, io, "transport write") do
        write(io, data)
    end
    nothing
end

function _close_resource!(errors::Vector, operation::String, resource)
    isnothing(resource) && return errors
    try
        close(resource)
    catch err
        push!(errors, CleanupError(operation, err))
    end
    errors
end

function _close_transport(read_io, write_io, sock)
    errors = Any[]
    seen = Any[]
    for (operation, io) in (("close read transport", read_io), ("close write transport", write_io), ("close socket", sock))
        isnothing(io) && continue
        transport = _underlying_transport(io)
        any(x -> x === transport, seen) && continue
        push!(seen, transport)
        _close_resource!(errors, operation, transport)
    end
    errors
end

function _take_transport!(client::Client; preserve_replayable::Bool=false)
    replayable = UInt8[]
    dropped_replayable = 0
    transports = @lock client.write_lock begin
        read_io, write_io, sock, preserve = @lock client.lock begin
            read_io = client.read_io
            write_io = client.write_io
            sock = client.socket
            preserve = preserve_replayable && client.status != ConnectionStatus.CLOSED
            client.read_io = nothing
            client.reader = nothing
            client.write_io = nothing
            client.socket = nothing
            read_io, write_io, sock, preserve
        end
        if preserve && !isnothing(write_io)
            replayable = _take_replayable_writes!(write_io)
        elseif !isnothing(write_io)
            dropped_replayable = length(_take_replayable_writes!(write_io))
        end
        read_io, write_io, sock
    end
    isempty(replayable) || _prepend_pending!(client, replayable; already_counted=true)
    _release_pending_bytes!(client, dropped_replayable)
    transports
end

function _report_cleanup_errors(client::Client, errors::Vector)
    for err in errors
        _report_error(client, err)
        if err isa CleanupError
            @warn "Natter cleanup failed" operation=err.operation cause=err.cause
        else
            @warn "Natter cleanup failed" exception=err
        end
    end
    nothing
end

function _close_transport!(client::Client)
    errors = _close_transport(_take_transport!(client)...)
    _throw_errors(errors)
    nothing
end

function _close_transport_report_errors!(client::Client; preserve_replayable::Bool=false)
    errors = _close_transport(_take_transport!(client; preserve_replayable)...)
    _report_cleanup_errors(client, errors)
    nothing
end

function _close_transport_report_errors!(client::Client, read_io, write_io, sock)
    errors = _close_transport(read_io, write_io, sock)
    _report_cleanup_errors(client, errors)
    nothing
end

function _notify_pong_waiters!(client::Client, value::Bool)
    errors = Any[]
    @lock client.lock begin
        for waiter in client.pongs
            try
                _resolve_pong_waiter_locked!(waiter, value)
            catch err
                push!(errors, CleanupError("notify flush waiter", err))
            end
        end
        empty!(client.pongs)
    end
    errors
end

function _notify_request_waiters!(client::Client, err::Exception; clear_mux::Bool=false)
    errors = Any[]
    mux = @lock client.lock begin
        mux = @atomic client.request_mux
        clear_mux && (@atomic client.request_mux = nothing)
        mux
    end
    if !isnothing(mux)
        lock(mux.condition)
        try
            waiters = collect(values(mux.waiters))
            empty!(mux.waiters)
            for waiter in waiters
                try
                    _resolve_request_waiter_locked!(waiter, err, mux.condition)
                catch notify_err
                    push!(errors, CleanupError("notify request waiter", notify_err))
                end
            end
            notify(mux.condition; all=true)
        finally
            unlock(mux.condition)
        end
    end
    errors
end

function _terminal_disconnect!(client::Client, generation::Int, err::Exception)
    subs = Subscription[]
    request_mux = nothing
    terminal = @lock client.lock begin
        if client.generation == generation &&
           client.status in (ConnectionStatus.CONNECTED, ConnectionStatus.RECONNECTING, ConnectionStatus.DISCONNECTED)
            _store_status_locked!(client, ConnectionStatus.DISCONNECTED)
            client.current_server = nothing
            client.connected_url = nothing
            client.flusher_task = nothing
            client.reader_task = nothing
            client.ping_task = nothing
            client.reconnect_task = nothing
            client.pings_out = 0

            subs = collect(values(client.subscriptions))
            empty!(client.subscriptions)
            @atomic client.subscription_snapshot = Vector{Union{Subscription{typeof(client)},Nothing}}()
            reader = client.reader
            isnothing(reader) || empty!(reader.subject_cache)

            mux = @atomic client.request_mux
            if !isnothing(mux)
                request_mux = mux
                @atomic client.request_mux = nothing
            end
            true
        else
            false
        end
    end
    terminal || return false

    for sub in subs
        @lock sub.lock begin
            sub.closed = true
            sub.server_active = false
            _notify_subscription_waiters_locked(sub; all=true)
        end
    end
    _wake_flusher(client)
    errors = Any[]
    append!(errors, _notify_pong_waiters!(client, false))
    if !isnothing(request_mux)
        lock(request_mux.condition)
        try
            request_waiters = collect(values(request_mux.waiters))
            empty!(request_mux.waiters)
            for waiter in request_waiters
                try
                    _resolve_request_waiter_locked!(waiter, ConnectionClosedError("connection is disconnected"),
                                                    request_mux.condition)
                catch notify_err
                    push!(errors, CleanupError("notify request waiter", notify_err))
                end
            end
            notify(request_mux.condition; all=true)
        finally
            unlock(request_mux.condition)
        end
    end
    for sub in subs
        _close_subscription_channel!(errors, sub)
    end
    _report_cleanup_errors(client, errors)
    _close_transport_report_errors!(client)
    _clear_pending_buffer!(client)
    _report_error(client, err)
    true
end

function _trim_stale_pong_waiters_locked!(client::Client)
    limit = max(0, client.options.max_stale_pong_waiters)
    stale = count(waiter -> !waiter.active && !waiter.ready, client.pongs)
    stale <= limit && return nothing

    remove = stale - limit
    _filter_pong_waiter_queue!(client.pongs) do waiter
        if remove > 0 && !waiter.active && !waiter.ready
            remove -= 1
            return false
        end
        true
    end
    nothing
end

function _take_client_tasks!(client::Client)
    @lock client.lock begin
        tasks = (
            ("stop reader task", client.reader_task),
            ("stop ping task", client.ping_task),
            ("stop reconnect task", client.reconnect_task),
            ("stop flusher task", client.flusher_task),
            ("stop write watchdog task", client.write_timeout_task),
        )
        client.reader_task = nothing
        client.ping_task = nothing
        client.reconnect_task = nothing
        client.flusher_task = nothing
        client.write_timeout_task = nothing
        client.write_deadline[] = time()
        tasks
    end
end

function _stop_client_tasks!(client::Client; timeout::Real=0.5)
    tasks = _take_client_tasks!(client)
    errors = Any[]
    for (operation, task) in tasks
        _wait_task!(errors, operation, task; timeout, interrupt=true)
    end
    errors
end

function _connect_auth_fields(opts::ConnectOptions, url_user, url_pass)
    option_has_token = !isnothing(opts.token)
    option_has_userpass = !isnothing(opts.user) || !isnothing(opts.password)
    option_has_nkey_jwt = _connect_option_has_nkey_jwt(opts)
    option_has_auth = option_has_token || option_has_userpass || option_has_nkey_jwt
    url_has_token = !isnothing(url_user) && isnothing(url_pass)
    url_has_userpass = !isnothing(url_pass)
    url_has_auth = url_has_token || url_has_userpass
    has_token = option_has_token || url_has_token
    has_userpass = option_has_userpass || url_has_userpass
    if has_token && has_userpass
        throw(ArgumentError("token authentication cannot be combined with user/password authentication"))
    end
    if option_has_auth && url_has_auth
        throw(ArgumentError("authentication credentials must be provided either in options or URL userinfo, not both"))
    end
    !isnothing(opts.nkey) && _nkey_decode_public(opts.nkey)
    !isnothing(opts.nkey_seed) && _validate_nkey_seed(opts.nkey_seed)

    token = !isnothing(opts.token) ? opts.token : (isnothing(url_user) || !isnothing(url_pass) ? nothing : url_user)
    user = !isnothing(opts.user) ? opts.user : (!isnothing(url_pass) ? url_user : nothing)
    password = !isnothing(opts.password) ? opts.password : url_pass
    if isnothing(user) != isnothing(password)
        throw(ArgumentError("user and password must be provided together"))
    end
    (token=token, user=user, password=password)
end

function _connect_command(client::Client, info::ServerInfo, url_user, url_pass)
    opts = client.options
    hdrs = info.headers === true
    body = Dict{String,Any}(
        "verbose" => opts.verbose,
        "pedantic" => opts.pedantic,
        "lang" => "julia",
        "version" => CLIENT_VERSION,
        "protocol" => 1,
        "headers" => hdrs,
        "no_responders" => hdrs,
        "echo" => !opts.no_echo,
    )
    isnothing(opts.name) || (body["name"] = opts.name)
    auth = _connect_auth_fields(opts, url_user, url_pass)
    isnothing(auth.token) || (body["auth_token"] = auth.token)
    if !isnothing(auth.user)
        body["user"] = auth.user
        body["pass"] = auth.password
    end
    nkey_jwt_auth = _connect_nkey_jwt_fields(opts, info)
    isnothing(nkey_jwt_auth.jwt) || (body["jwt"] = nkey_jwt_auth.jwt)
    isnothing(nkey_jwt_auth.nkey) || (body["nkey"] = nkey_jwt_auth.nkey)
    isnothing(nkey_jwt_auth.sig) || (body["sig"] = nkey_jwt_auth.sig)
    "CONNECT $(JSON3.write(body))$CRLF"
end

function _connect_once!(client::Client, server::Server; mark_connected::Bool=true, generation::Union{Nothing,Int}=nothing)
    scheme, host, port, url_user, url_pass = _server_parts(server.url)
    tls_host = _tls_hostname(server, host)
    _connect_auth_fields(client.options, url_user, url_pass)
    deadline::Float64 = time() + client.options.connect_timeout
    sock = _connect_tcp(host, port, _remaining_timeout(deadline))
    read_io = sock
    write_io = sock
    reader = ProtocolReader(read_io)
    cleanup = () -> _close_transport(read_io, write_io, sock)
    report_timeout_cleanup = errors -> _report_cleanup_errors(client, errors)
    try
        tls_active::Bool = false
        if _tls_first_for_connection(client.options, scheme)
            tls = _run_with_timeout("TLS handshake", _remaining_timeout(deadline), cleanup, report_timeout_cleanup) do
                _tls_wrap(sock, client.options, tls_host)
            end
            read_io = tls
            write_io = tls
            reader = ProtocolReader(read_io)
            tls_active = true
        end
        frame = _run_with_timeout("connect INFO read", _remaining_timeout(deadline), cleanup, report_timeout_cleanup) do
            _read_control_or_msg(reader, client.options)
        end
        frame.op == :INFO || throw(ProtocolError("expected INFO during connect"))
        info = _protocol_info(frame)
        wants_tls::Bool = !tls_active && (scheme == "tls" || client.options.tls_required || info.tls_required === true)
        if wants_tls
            available = something(info.tls_available, info.tls_required === true)
            available == true || throw(ProtocolError("TLS requested but server did not advertise TLS availability"))
            tls = _run_with_timeout("TLS handshake", _remaining_timeout(deadline), cleanup, report_timeout_cleanup) do
                _tls_wrap(sock, client.options, tls_host)
            end
            read_io = tls
            write_io = tls
            reader = ProtocolReader(read_io)
            tls_active = true
        end
        _run_with_timeout("connect command write", _remaining_timeout(deadline), cleanup, report_timeout_cleanup) do
            write(write_io, _connect_command(client, info, url_user, url_pass))
            write(write_io, "PING$CRLF")
            flush(write_io)
        end
        while true
            frame = _run_with_timeout("connect PONG read", _remaining_timeout(deadline), cleanup, report_timeout_cleanup) do
                _read_control_or_msg(reader, client.options)
            end
            op = frame.op
            if op == :PONG
                break
            elseif op == :PING
                _run_with_timeout("connect PONG write", _remaining_timeout(deadline), cleanup, report_timeout_cleanup) do
                    write(write_io, "PONG$CRLF")
                    flush(write_io)
                end
            elseif op == :ERR
                _throw_server_err(_protocol_err(frame))
            elseif op == :OK
                continue
            else
                throw(ProtocolError("unexpected $(op) during connect"))
            end
        end
        accepted = @lock client.write_lock begin
            @lock client.lock begin
                if !isnothing(generation) && (client.generation != generation || client.status == ConnectionStatus.CLOSED)
                    false
                else
                    client.current_server = server
                    client.connected_url = server.url
                    client.info = info
                    _sync_server_info_cache_locked!(client)
                    client.socket = sock
                    client.read_io = read_io
                    client.reader = reader
                    client.write_io = _write_transport_for_options(write_io, client.options)
                    mark_connected && _store_status_locked!(client, ConnectionStatus.CONNECTED)
                    client.pings_out = 0
                    server.last_auth_error = nothing
                    true
                end
            end
        end
        if !accepted
            _close_transport_report_errors!(client, read_io, write_io, sock)
            read_io = nothing
            write_io = nothing
            reader = nothing
            sock = nothing
            throw(ConnectionClosedError("connection state changed while connecting"))
        end
        _merge_discovered_servers!(client, info)
        return nothing
    catch err
        err isa TimeoutError || _close_transport_report_errors!(client, read_io, write_io, sock)
        rethrow()
    end
end

function _connect_initial!(client::Client)
    generation = @lock client.lock begin
        _store_status_locked!(client, ConnectionStatus.CONNECTING)
        _bump_generation_locked!(client)
    end
    last_err = nothing
    for server in client.servers
        try
            _connect_once!(client, server; generation)
            _start_background_tasks!(client, generation)
            return client
        catch err
            last_err = err
            _report_error(client, err)
        end
    end
    @lock client.lock _store_status_locked!(client, ConnectionStatus.DISCONNECTED)
    isnothing(last_err) ? throw(NoServersError()) : throw(last_err)
end

function _start_flusher_task!(client::Client, generation::Int=(@lock client.lock client.generation))
    assigned = @lock client.lock begin
        existing = client.flusher_task
        if client.generation == generation &&
           client.status in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING, ConnectionStatus.RECONNECTING) &&
           (isnothing(existing) || istaskdone(existing))
            :start
        else
            :skip
        end
    end
    assigned == :start || return nothing
    flusher_task = @async _flusher_loop(client, generation)
    @lock client.lock begin
        if client.generation == generation &&
           client.status in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING, ConnectionStatus.RECONNECTING)
            client.flusher_task = flusher_task
        end
    end
    nothing
end

function _start_background_tasks!(client::Client, generation::Int=(@lock client.lock client.generation))
    _start_flusher_task!(client, generation)
    reader_task = @async _reader_loop(client, generation)
    ping_task = @async _ping_loop(client, generation)
    assigned = @lock client.lock begin
        if client.generation == generation && client.status in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING)
            client.reader_task = reader_task
            client.ping_task = ping_task
            true
        else
            false
        end
    end
    assigned || return nothing
    nothing
end

function _flusher_loop(client::Client, generation::Int)
    while _generation_matches(client, generation) &&
          status(client) in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING, ConnectionStatus.RECONNECTING)
        ch = @lock client.lock client.flush_signal
        try
            take!(ch)
        catch err
            err isa InvalidStateException && return
            _report_error(client, err)
            return
        end
        latency = max(0.0, client.options.write_buffer_latency)
        latency > 0 ? sleep(latency) : yield()
        while isready(ch)
            try
                take!(ch)
            catch err
                err isa InvalidStateException && return
                _report_error(client, err)
                return
            end
        end
        _generation_matches(client, generation) &&
            status(client) in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING, ConnectionStatus.RECONNECTING) || return
        try
            _flush_buffered_writes(client; allow_missing=true)
        catch err
            status(client) == ConnectionStatus.CLOSED && return
            _report_error(client, err)
            _trigger_reconnect(client, err)
            return
        end
    end
end

function _report_error(client::Client, err)
    _record_error!(client)
    try
        client.options.error_cb(err)
    catch callback_err
        @warn "Natter error callback failed" exception=(callback_err, catch_backtrace())
    end
end

function _server_err(message::AbstractString)::NatterError
    text = String(message)
    lower = lowercase(text)
    if startswith(lower, "permissions violation")
        return PermissionViolationError(text)
    elseif startswith(lower, "authorization violation")
        return AuthorizationError(text)
    elseif startswith(lower, "user authentication expired")
        return AuthenticationExpiredError(text)
    elseif startswith(lower, "user authentication revoked")
        return AuthenticationRevokedError(text)
    elseif startswith(lower, "account authentication expired")
        return AccountAuthenticationExpiredError(text)
    else
        return ProtocolError(text)
    end
end

_throw_server_err(message::AbstractString) = throw(_server_err(message))

_same_auth_error(left::AuthenticationError, right::AuthenticationError)::Bool =
    typeof(left) === typeof(right)

function _record_auth_error!(client::Client, server::Server, err::AuthenticationError)::Bool
    @lock client.lock begin
        abort = !isnothing(server.last_auth_error) && _same_auth_error(server.last_auth_error, err)
        server.last_auth_error = err
        abort
    end
end

function _record_current_auth_error!(client::Client, err::AuthenticationError)::Bool
    server = @lock client.lock client.current_server
    isnothing(server) && return false
    _record_auth_error!(client, server, err)
end

function _handle_server_err!(client::Client, generation::Int, message::AbstractString)::Bool
    err = _server_err(message)
    if err isa PermissionViolationError
        _report_error(client, err)
        return false
    elseif err isa AuthenticationError
        if _record_current_auth_error!(client, err)
            _terminal_disconnect!(client, generation, err)
        else
            _report_error(client, err)
            _trigger_reconnect(client, err)
        end
        return true
    else
        _report_error(client, err)
        _trigger_reconnect(client, err)
        return true
    end
end

function _merge_discovered_servers!(client::Client, info::ServerInfo)
    urls = info.connect_urls
    isnothing(urls) && return
    added = false
    @lock client.lock begin
        base_url = client.connected_url
        current_server = client.current_server
        tls_active = _discovery_uses_tls(client.options, info, current_server, base_url)
        discovered_urls = Set{String}()
        normalized_servers = Vector{Tuple{String,Union{String,Nothing}}}()
        discovered_tls_names = Dict{String,Union{String,Nothing}}()
        for raw in urls
            url = _normalize_discovered_url(raw, base_url)
            if !(url in discovered_urls)
                tls_name = _discovered_tls_name(url, current_server, base_url, tls_active)
                push!(discovered_urls, url)
                push!(normalized_servers, (url, tls_name))
                discovered_tls_names[url] = tls_name
            end
        end
        # Configured seed servers are sticky; discovered routes track the latest INFO snapshot.
        filter!(client.servers) do server
            !server.discovered || server === current_server || server.url in discovered_urls
        end
        for server in client.servers
            if server.discovered && server !== current_server && haskey(discovered_tls_names, server.url)
                server.tls_name = discovered_tls_names[server.url]
            end
        end
        existing = Set(s.url for s in client.servers)
        for (url, tls_name) in normalized_servers
            if !(url in existing)
                push!(client.servers, Server(url; discovered=true, tls_name))
                push!(existing, url)
                added = true
            end
        end
    end
    if added
        try client.options.discovered_server_cb() catch err _report_error(client, err) end
    end
end

function _generation_matches(client::Client, generation::Int)
    _load_generation(client) == generation
end

function _sleep_interruptibly(client::Client, generation::Int, seconds::Real)
    deadline = time() + max(0.0, seconds)
    while time() < deadline
        _generation_matches(client, generation) || return false
        status(client) in (ConnectionStatus.CLOSED, ConnectionStatus.DISCONNECTED) && return false
        sleep(min(0.05, max(0.0, deadline - time())))
    end
    _generation_matches(client, generation) &&
        !(status(client) in (ConnectionStatus.CLOSED, ConnectionStatus.DISCONNECTED))
end

function _reader_loop(client::Client, generation::Int)
    reader = @lock client.lock client.reader
    if isnothing(reader)
        read_io = @lock client.lock client.read_io
        isnothing(read_io) && return
        reader = ProtocolReader(read_io)
        @lock client.lock client.reader = reader
    end
    _reader_loop_with_reader(client, generation, reader)
    nothing
end

function _reader_loop_with_reader(client::Client, generation::Int,
                                  reader::ProtocolReader{ReadIO}) where {ReadIO}
    while _generation_matches(client, generation) && status(client) in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING)
        try
            frame = _read_control_or_msg(reader, client.options)
            op = frame.op
            if op == :MSG
                msg = _protocol_msg(frame)
                _dispatch_msg(client, msg)
            elseif op == :PING
                _send_raw(client, "PONG$CRLF"; force_flush=true)
            elseif op == :PONG
                _notify_pong(client)
            elseif op == :INFO
                info = _protocol_info(frame)
                @lock client.lock begin
                    _merge_server_info!(client.info, info)
                    _sync_server_info_cache_locked!(client)
                end
                _merge_discovered_servers!(client, info)
                if info.ldm
                    _trigger_reconnect(client, ProtocolError("server entered lame duck mode"))
                    return
                end
            elseif op == :ERR
                _handle_server_err!(client, generation, _protocol_err(frame)) && return
            end
        catch err
            status(client) in (ConnectionStatus.CLOSED, ConnectionStatus.DRAINING) && return
            _report_error(client, err)
            _trigger_reconnect(client, err)
            return
        end
    end
    nothing
end

function _ping_loop(client::Client, generation::Int)
    while _generation_matches(client, generation) && status(client) == ConnectionStatus.CONNECTED
        _sleep_interruptibly(client, generation, client.options.ping_interval) || return
        _generation_matches(client, generation) && status(client) == ConnectionStatus.CONNECTED || return
        too_many = @lock client.lock begin
            client.pings_out += 1
            client.pings_out > client.options.max_outstanding_pings
        end
        if too_many
            _trigger_reconnect(client, TimeoutError("too many outstanding pings"))
            return
        end
        try
            _send_raw(client, "PING$CRLF"; force_flush=true)
        catch err
            _report_error(client, err)
            _trigger_reconnect(client, err)
            return
        end
    end
end

function _notify_pong(client::Client)
    waiter = nothing
    notify_err = nothing
    @lock client.lock begin
        client.pings_out = 0
        if !isempty(client.pongs)
            waiter = popfirst!(client.pongs)
        end
        if !isnothing(waiter)
            try
                _resolve_pong_waiter_locked!(waiter, true)
            catch err
                notify_err = err
            end
        end
    end
    isnothing(notify_err) || _report_error(client, CleanupError("notify flush waiter", notify_err))
    nothing
end

function _trigger_reconnect(client::Client, reason)
    opts = client.options
    should_start = false
    generation = 0
    notify_subs = Subscription[]
    @lock client.lock begin
        if client.status == ConnectionStatus.CONNECTED
            _bump_generation_locked!(client)
            generation = client.generation
            _store_status_locked!(client, opts.allow_reconnect ? ConnectionStatus.RECONNECTING : ConnectionStatus.DISCONNECTED)
            client.flusher_task = nothing
            should_start = opts.allow_reconnect
            notify_subs = collect(values(client.subscriptions))
        end
    end
    generation == 0 && return nothing
    for sub in notify_subs
        @lock sub.lock begin
            sub.server_active = false
            _notify_subscription_waiters_locked(sub; all=true)
        end
    end
    if should_start
        _wake_flusher(client)
        _report_cleanup_errors(client, _notify_request_waiters!(client, ConnectionReconnectingError()))
        _report_cleanup_errors(client, _notify_pong_waiters!(client, false))
        _close_transport_report_errors!(client; preserve_replayable=true)
        try opts.disconnected_cb() catch err _report_error(client, err) end
        should_spawn = @lock client.lock client.generation == generation && client.status == ConnectionStatus.RECONNECTING
        should_spawn || return nothing
        reconnect_task = @async _reconnect_loop(client, generation)
        assigned = @lock client.lock begin
            if client.generation == generation && client.status == ConnectionStatus.RECONNECTING
                client.reconnect_task = reconnect_task
                true
            else
                false
            end
        end
        assigned || return nothing
    else
        _terminal_disconnect!(client, generation, reason)
    end
    nothing
end

function _recover_after_write_failure!(client::Client, err)
    st, generation = @lock client.lock (client.status, client.generation)
    if st == ConnectionStatus.CONNECTED
        if client.options.allow_reconnect
            _report_error(client, err)
            should_reconnect = @lock client.lock begin
                client.status == ConnectionStatus.CONNECTED && client.generation == generation
            end
            should_reconnect || return false
            _trigger_reconnect(client, err)
            return @lock client.lock begin
                client.status == ConnectionStatus.RECONNECTING && client.generation == generation + 1
            end
        else
            _trigger_reconnect(client, err)
            return false
        end
    elseif st in (ConnectionStatus.RECONNECTING, ConnectionStatus.CONNECTING)
        return true
    else
        return false
    end
end

function _reconnect_loop(client::Client, generation::Int)
    opts = client.options
    delay = opts.reconnect_wait
    attempts = 0
    while _generation_matches(client, generation) && status(client) == ConnectionStatus.RECONNECTING
        attempts += 1
        if opts.max_reconnect_attempts >= 0 && attempts > opts.max_reconnect_attempts
            _terminal_disconnect!(client, generation, NoServersError())
            return
        end
        servers = @lock client.lock begin
            servers = copy(client.servers)
            shuffle!(client.rng, servers)
            servers
        end
        if isempty(servers)
            _terminal_disconnect!(client, generation, NoServersError())
            return
        end
        for server in servers
            _generation_matches(client, generation) && status(client) == ConnectionStatus.RECONNECTING || return
            try
                _connect_once!(client, server; mark_connected=false, generation)
                _start_flusher_task!(client, generation)
                _record_reconnect!(client)
                _replay_subscriptions(client)
                _flush_pending_buffer(client; generation=generation)
                committed = @lock client.lock begin
                    if client.generation == generation && client.status == ConnectionStatus.RECONNECTING
                        _store_status_locked!(client, ConnectionStatus.CONNECTED)
                        true
                    else
                        false
                    end
                end
                committed || begin
                    _close_transport_report_errors!(client)
                    return
                end
                _replay_subscriptions(client)
                _flush_pending_buffer(client; generation=generation)
                _flush_buffered_writes(client)
                _start_background_tasks!(client, generation)
                try opts.reconnected_cb() catch err _report_error(client, err) end
                return
            catch err
                if err isa AuthenticationError && _record_auth_error!(client, server, err)
                    _terminal_disconnect!(client, generation, err)
                    return
                end
                notify_subs = Subscription[]
                @lock client.lock begin
                    if client.generation == generation && client.status != ConnectionStatus.CLOSED
                        _store_status_locked!(client, ConnectionStatus.RECONNECTING)
                        notify_subs = collect(values(client.subscriptions))
                    end
                end
                for sub in notify_subs
                    @lock sub.lock begin
                        sub.server_active = false
                        _notify_subscription_waiters_locked(sub; all=true)
                    end
                end
                _close_transport_report_errors!(client; preserve_replayable=true)
                _report_error(client, err)
            end
        end
        wait_time = @lock client.lock max(0.0, delay + rand(client.rng) * opts.reconnect_jitter)
        _sleep_interruptibly(client, generation, wait_time) || return
        delay = min(opts.reconnect_max_wait, delay * 2)
    end
end

function _replay_subscriptions(client::Client)
    subs = @lock client.lock collect(values(client.subscriptions))
    for sub in subs
        present = @lock client.lock get(client.subscriptions, sub.sid, nothing) === sub
        state = @lock sub.lock begin
            if !present || sub.closed || sub.server_active
                nothing
            else
                remaining = sub.max_msgs > 0 ? sub.max_msgs - sub.received : nothing
                !isnothing(remaining) && remaining <= 0 ? :close : (sub.sid, sub.subject, sub.queue, remaining)
            end
        end
        isnothing(state) && continue
        if state === :close
            _close_subscription_locally!(sub; throw_errors=false)
            continue
        end
        sid, subject, queue, remaining = state
        _write_raw(client, _sub_cmd(subject, queue, sid))
        isnothing(remaining) || _write_raw(client, _unsub_cmd(sid, remaining))
        present = @lock client.lock get(client.subscriptions, sid, nothing) === sub
        active = @lock sub.lock begin
            if present && !sub.closed
                sub.server_active = true
                true
            else
                false
            end
        end
        active || _write_raw(client, _unsub_cmd(sid))
    end
end

function _prepend_pending_locked!(client::Client, data::Vector{UInt8}; already_counted::Bool=false)
    bytes = length(data)
    already_counted || _reserve_pending_bytes_locked!(client, bytes)
    _prepend_pending_chunk!(client.pending, data)
    nothing
end

function _prepend_pending!(client::Client, data::Vector{UInt8}; already_counted::Bool=false)
    @lock client.lock _prepend_pending_locked!(client, data; already_counted=already_counted)
    nothing
end

function _restore_pending_after_replay_failure!(client::Client, data::Vector{UInt8}, generation::Int)
    @lock client.lock begin
        if client.generation == generation && client.status in (ConnectionStatus.RECONNECTING, ConnectionStatus.CONNECTED)
            _prepend_pending_locked!(client, data; already_counted=true)
        else
            client.pending_bytes = max(0, client.pending_bytes - length(data))
        end
    end
    nothing
end

function _pending_replay_batch_size(client::Client)::Int
    threshold = max(0, client.options.write_buffer_size)
    threshold > 0 ? threshold : DEFAULT_WRITE_BUFFER_SIZE
end

function _flush_pending_buffer(client::Client; generation::Union{Int,Nothing}=nothing)
    while true
        data = UInt8[]
        replay_generation = 0
        @lock client.lock begin
            replay_generation = isnothing(generation) ? client.generation : generation
            data = _pop_pending_batch!(client.pending, _pending_replay_batch_size(client))
        end
        isempty(data) && return
        try
            _write_raw(client, data; force_flush=true)
            _release_pending_bytes!(client, length(data))
        catch err
            _restore_pending_after_replay_failure!(client, data, replay_generation)
            rethrow()
        end
    end
end
