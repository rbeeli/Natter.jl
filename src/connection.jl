function _parse_options(url_or_urls; kwargs...)
    servers =
        if isnothing(url_or_urls)
            [DEFAULT_URL]
        elseif url_or_urls isa AbstractString
            String.(split(String(url_or_urls), ","; keepempty=false))
        elseif url_or_urls isa AbstractVector
            String.(url_or_urls)
        else
            throw(ArgumentError("servers must be a string or vector of strings"))
        end
    isempty(servers) && throw(ArgumentError("at least one server URL is required"))
    ConnectOptions(; servers, kwargs...)
end

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
        0,
        Dict{Int,Subscription}(),
        nothing,
        ReentrantLock(),
        IOBuffer(),
        0,
        PongWaiter[],
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
        user = String(first(pieces))
        password = length(pieces) == 2 ? String(last(pieces)) : nothing
    end
    scheme, host, port, user, password
end

function _tls_first_for_connection(opts::ConnectOptions, scheme::AbstractString)::Bool
    isnothing(opts.tls_first) ? scheme == "tls" : opts.tls_first
end

function _tls_authmode(opts::ConnectOptions)::Int
    opts.tls_verify ? MbedTLS.MBEDTLS_SSL_VERIFY_REQUIRED : MbedTLS.MBEDTLS_SSL_VERIFY_NONE
end

function _connect_tcp(host::String, port::Int, timeout::Real)
    ch = Channel{Any}(1)
    task = @async begin
        try
            put!(ch, Sockets.connect(host, port))
        catch err
            put!(ch, err)
        end
    end
    result = timedwait(timeout; pollint=0.01) do
        isready(ch)
    end
    if result == :timed_out
        @async begin
            try
                wait(task)
            catch err
                @debug "Natter connect cleanup task failed" exception=(err, catch_backtrace())
            end
            if isready(ch)
                late = take!(ch)
                late isa Sockets.TCPSocket && close(late)
            end
        end
        throw(TimeoutError("connect to $host:$port timed out"))
    end
    value = take!(ch)
    value isa Exception && throw(value)
    value
end

function _schedule_timeout_cleanup(operation::String, cleanup::Function)
    @async begin
        try
            cleanup()
        catch err
            @debug "Natter timeout cleanup failed" operation exception=(err, catch_backtrace())
        end
    end
    nothing
end

function _run_with_timeout(f::Function, operation::String, timeout::Real, cleanup::Function)
    if timeout <= 0
        _schedule_timeout_cleanup(operation, cleanup)
        throw(TimeoutError("$operation timed out"))
    end
    ch = Channel{Tuple{Bool,Any}}(1)
    task = @async begin
        try
            put!(ch, (true, f()))
        catch err
            put!(ch, (false, err))
        end
    end
    result = timedwait(timeout; pollint=0.01) do
        isready(ch)
    end
    if result == :timed_out
        _schedule_timeout_cleanup(operation, cleanup)
        @async begin
            try
                wait(task)
            catch err
                @debug "Natter timed operation task failed after timeout" operation exception=(err, catch_backtrace())
            end
        end
        throw(TimeoutError("$operation timed out"))
    end
    ok, value = take!(ch)
    ok || throw(value)
    value
end

_remaining_timeout(deadline::Float64)::Float64 = max(0.0, deadline - time())

function _tls_wrap(sock, opts::ConnectOptions, hostname::String)
    entropy = MbedTLS.Entropy()
    rng = MbedTLS.CtrDrbg()
    MbedTLS.seed!(rng, entropy)
    conf = MbedTLS.SSLConfig()
    MbedTLS.config_defaults!(conf)
    MbedTLS.rng!(conf, rng)
    MbedTLS.authmode!(conf, _tls_authmode(opts))
    if !isnothing(opts.tls_ca_path)
        MbedTLS.ca_chain!(conf, MbedTLS.crt_parse_file(opts.tls_ca_path))
    end
    if !isnothing(opts.tls_cert_path) && !isnothing(opts.tls_key_path)
        MbedTLS.own_cert!(conf, MbedTLS.crt_parse_file(opts.tls_cert_path), MbedTLS.parse_keyfile(opts.tls_key_path))
    end
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
    @lock client.lock client.stats.errors += 1
    nothing
end

function _record_out!(client::Client, bytes::Int)
    @lock client.lock begin
        client.stats.out_msgs += 1
        client.stats.out_bytes += bytes
    end
    nothing
end

function _record_drop!(client::Client)
    @lock client.lock client.stats.dropped_msgs += 1
    nothing
end

function _record_reconnect!(client::Client)
    @lock client.lock client.stats.reconnects += 1
    nothing
end

function _write_raw(client::Client, data::Union{AbstractString,Vector{UInt8}})
    @lock client.write_lock begin
        io = @lock client.lock client.write_io
        isnothing(io) && throw(ConnectionClosedError("connection transport is closed"))
        write(io, data)
        flush(io)
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
        any(x -> x === io, seen) && continue
        push!(seen, io)
        _close_resource!(errors, operation, io)
    end
    errors
end

function _take_transport!(client::Client)
    @lock client.write_lock begin
        @lock client.lock begin
            read_io = client.read_io
            write_io = client.write_io
            sock = client.socket
            client.read_io = nothing
            client.reader = nothing
            client.write_io = nothing
            client.socket = nothing
            read_io, write_io, sock
        end
    end
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

function _close_transport_report_errors!(client::Client)
    errors = _close_transport(_take_transport!(client)...)
    _report_cleanup_errors(client, errors)
    nothing
end

function _close_transport_report_errors!(client::Client, read_io, write_io, sock)
    errors = _close_transport(read_io, write_io, sock)
    _report_cleanup_errors(client, errors)
    nothing
end

function _notify_pong_waiters!(client::Client, value::Bool)
    waiters = @lock client.lock begin
        waiters = copy(client.pongs)
        empty!(client.pongs)
        waiters
    end
    errors = Any[]
    for waiter in waiters
        ch = waiter.channel
        isnothing(ch) && continue
        try
            isopen(ch) && put!(ch, value)
        catch err
            push!(errors, CleanupError("notify flush waiter", err))
        end
    end
    errors
end

function _notify_request_waiters!(client::Client, err::Exception; clear_mux::Bool=false)
    waiters = @lock client.lock begin
        mux = client.request_mux
        if isnothing(mux)
            Channel{Any}[]
        else
            waiters = collect(values(mux.waiters))
            empty!(mux.waiters)
            clear_mux && (client.request_mux = nothing)
            waiters
        end
    end
    errors = Any[]
    for ch in waiters
        try
            isopen(ch) && put!(ch, err)
        catch notify_err
            push!(errors, CleanupError("notify request waiter", notify_err))
        end
    end
    errors
end

function _trim_stale_pong_waiters_locked!(client::Client)
    limit = max(0, client.options.max_stale_pong_waiters)
    stale = count(waiter -> isnothing(waiter.channel), client.pongs)
    stale <= limit && return nothing

    remove = stale - limit
    filter!(client.pongs) do waiter
        if remove > 0 && isnothing(waiter.channel)
            remove -= 1
            return false
        end
        true
    end
    nothing
end

function _wait_task!(errors::Vector, operation::String, task::Union{Task,Nothing}; timeout::Real=0.5)
    (isnothing(task) || istaskdone(task) || task === current_task()) && return errors
    result = timedwait(timeout; pollint=0.005) do
        istaskdone(task)
    end
    result == :timed_out && push!(errors, CleanupError(operation, TimeoutError("$operation timed out")))
    errors
end

function _take_client_tasks!(client::Client)
    @lock client.lock begin
        tasks = (
            ("stop reader task", client.reader_task),
            ("stop ping task", client.ping_task),
            ("stop reconnect task", client.reconnect_task),
        )
        client.reader_task = nothing
        client.ping_task = nothing
        client.reconnect_task = nothing
        tasks
    end
end

function _stop_client_tasks!(client::Client; timeout::Real=0.5)
    tasks = _take_client_tasks!(client)
    errors = Any[]
    for (operation, task) in tasks
        _wait_task!(errors, operation, task; timeout)
    end
    errors
end

function _connect_command(client::Client, info::ServerInfo, url_user, url_pass)
    opts = client.options
    body = Dict{String,Any}(
        "verbose" => opts.verbose,
        "pedantic" => opts.pedantic,
        "lang" => "julia",
        "version" => CLIENT_VERSION,
        "protocol" => 1,
        "headers" => true,
        "no_responders" => true,
        "echo" => !opts.no_echo,
    )
    isnothing(opts.name) || (body["name"] = opts.name)
    token = !isnothing(opts.token) ? opts.token : (isnothing(url_user) || !isnothing(url_pass) ? nothing : url_user)
    user = !isnothing(opts.user) ? opts.user : (!isnothing(url_pass) ? url_user : nothing)
    pass = !isnothing(opts.password) ? opts.password : url_pass
    isnothing(token) || (body["auth_token"] = token)
    if !isnothing(user) && !isnothing(pass)
        body["user"] = user
        body["pass"] = pass
    end
    "CONNECT $(JSON3.write(body))$CRLF"
end

function _connect_once!(client::Client, server::Server; mark_connected::Bool=true, generation::Union{Nothing,Int}=nothing)
    scheme, host, port, url_user, url_pass = _server_parts(server.url)
    deadline::Float64 = time() + client.options.connect_timeout
    sock = _connect_tcp(host, port, _remaining_timeout(deadline))
    read_io = sock
    write_io = sock
    reader = ProtocolReader{_read_transport_type(client)}(read_io)
    cleanup = () -> _close_transport(read_io, write_io, sock)
    try
        tls_active::Bool = false
        if _tls_first_for_connection(client.options, scheme)
            tls = _run_with_timeout("TLS handshake", _remaining_timeout(deadline), cleanup) do
                _tls_wrap(sock, client.options, host)
            end
            read_io = tls
            write_io = tls
            reader = ProtocolReader{_read_transport_type(client)}(read_io)
            tls_active = true
        end
        op, data = _run_with_timeout("connect INFO read", _remaining_timeout(deadline), cleanup) do
            _read_control_or_msg(reader, client.options)
        end
        op == :INFO || throw(ProtocolError("expected INFO during connect"))
        info = data::ServerInfo
        wants_tls::Bool = !tls_active && (scheme == "tls" || client.options.tls_required || info.tls_required === true)
        if wants_tls
            available = something(info.tls_available, info.tls_required === true)
            available == true || throw(ProtocolError("TLS requested but server did not advertise TLS availability"))
            tls = _run_with_timeout("TLS handshake", _remaining_timeout(deadline), cleanup) do
                _tls_wrap(sock, client.options, host)
            end
            read_io = tls
            write_io = tls
            reader = ProtocolReader{_read_transport_type(client)}(read_io)
            tls_active = true
        end
        _run_with_timeout("connect command write", _remaining_timeout(deadline), cleanup) do
            write(write_io, _connect_command(client, info, url_user, url_pass))
            write(write_io, "PING$CRLF")
            flush(write_io)
        end
        while true
            op, data = _run_with_timeout("connect PONG read", _remaining_timeout(deadline), cleanup) do
                _read_control_or_msg(reader, client.options)
            end
            if op == :PONG
                break
            elseif op == :PING
                _run_with_timeout("connect PONG write", _remaining_timeout(deadline), cleanup) do
                    write(write_io, "PONG$CRLF")
                    flush(write_io)
                end
            elseif op == :ERR
                _throw_server_err(data)
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
                    client.socket = sock
                    client.read_io = read_io
                    client.reader = reader
                    client.write_io = write_io
                    mark_connected && (client.status = ConnectionStatus.CONNECTED)
                    client.pings_out = 0
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
        client.status = ConnectionStatus.CONNECTING
        client.generation += 1
        client.generation
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
    @lock client.lock client.status = ConnectionStatus.DISCONNECTED
    isnothing(last_err) ? throw(NoServersError()) : throw(last_err)
end

function _start_background_tasks!(client::Client, generation::Int=(@lock client.lock client.generation))
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

function _report_error(client::Client, err)
    _record_error!(client)
    try
        client.options.error_cb(err)
    catch callback_err
        @warn "Natter error callback failed" exception=(callback_err, catch_backtrace())
    end
end

function _throw_server_err(message::AbstractString)
    lower = lowercase(String(message))
    if occursin("authorization", lower)
        throw(AuthorizationError(String(message)))
    else
        throw(ProtocolError(String(message)))
    end
end

function _merge_discovered_servers!(client::Client, info::ServerInfo)
    urls = info.connect_urls
    isnothing(urls) && return
    added = false
    @lock client.lock begin
        existing = Set(s.url for s in client.servers)
        base_url = client.connected_url
        for raw in urls
            url = _normalize_discovered_url(raw, base_url)
            if !(url in existing)
                push!(client.servers, Server(url; discovered=true))
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
    @lock client.lock client.generation == generation
end

function _sleep_interruptibly(client::Client, generation::Int, seconds::Real)
    deadline = time() + max(0.0, seconds)
    while time() < deadline
        _generation_matches(client, generation) || return false
        status(client) == ConnectionStatus.CLOSED && return false
        sleep(min(0.05, max(0.0, deadline - time())))
    end
    _generation_matches(client, generation) && status(client) != ConnectionStatus.CLOSED
end

function _reader_loop(client::Client, generation::Int)
    reader = @lock client.lock client.reader
    if isnothing(reader)
        read_io = @lock client.lock client.read_io
        isnothing(read_io) && return
        reader = ProtocolReader{_read_transport_type(client)}(read_io)
        @lock client.lock client.reader = reader
    end
    while _generation_matches(client, generation) && status(client) in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING)
        try
            op, data = _read_control_or_msg(reader, client.options)
            if op == :MSG
                msg = data::Msg
                msg.client = client
                _dispatch_msg(client, msg)
            elseif op == :PING
                _send_raw(client, "PONG$CRLF")
            elseif op == :PONG
                _notify_pong(client)
            elseif op == :INFO
                info = data::ServerInfo
                @lock client.lock _merge_server_info!(client.info, info)
                _merge_discovered_servers!(client, info)
                if info.ldm
                    _trigger_reconnect(client, ProtocolError("server entered lame duck mode"))
                    return
                end
            elseif op == :ERR
                err = ProtocolError(String(data))
                _report_error(client, err)
                _trigger_reconnect(client, err)
                return
            end
        catch err
            status(client) in (ConnectionStatus.CLOSED, ConnectionStatus.DRAINING) && return
            _report_error(client, err)
            _trigger_reconnect(client, err)
            return
        end
    end
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
            _send_raw(client, "PING$CRLF")
        catch err
            _report_error(client, err)
            _trigger_reconnect(client, err)
            return
        end
    end
end

function _notify_pong(client::Client)
    waiter = nothing
    @lock client.lock begin
        client.pings_out = 0
        if !isempty(client.pongs)
            waiter = popfirst!(client.pongs)
        end
    end
    if !isnothing(waiter) && !isnothing(waiter.channel)
        try
            put!(waiter.channel, true)
        catch err
            _report_error(client, CleanupError("notify flush waiter", err))
        end
    end
    nothing
end

function _trigger_reconnect(client::Client, reason)
    opts = client.options
    should_start = false
    generation = 0
    @lock client.lock begin
        if client.status == ConnectionStatus.CONNECTED
            client.generation += 1
            generation = client.generation
            client.status = opts.allow_reconnect ? ConnectionStatus.RECONNECTING : ConnectionStatus.DISCONNECTED
            should_start = opts.allow_reconnect
            for sub in values(client.subscriptions)
                sub.server_active = false
            end
        end
    end
    generation == 0 && return nothing
    request_err = opts.allow_reconnect ? ConnectionReconnectingError() : ConnectionClosedError("connection is disconnected")
    _report_cleanup_errors(client, _notify_request_waiters!(client, request_err))
    _report_cleanup_errors(client, _notify_pong_waiters!(client, false))
    _close_transport_report_errors!(client)
    if should_start
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
        _report_error(client, reason)
    end
    nothing
end

function _recover_after_write_failure!(client::Client, err)
    st = status(client)
    if st == ConnectionStatus.CONNECTED
        if client.options.allow_reconnect
            _report_error(client, err)
            _trigger_reconnect(client, err)
            return true
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
            @lock client.lock client.status = ConnectionStatus.DISCONNECTED
            _report_error(client, NoServersError())
            return
        end
        servers = @lock client.lock begin
            servers = copy(client.servers)
            shuffle!(client.rng, servers)
            servers
        end
        for server in servers
            _generation_matches(client, generation) && status(client) == ConnectionStatus.RECONNECTING || return
            try
                _connect_once!(client, server; mark_connected=false, generation)
                _record_reconnect!(client)
                _replay_subscriptions(client)
                _flush_pending_buffer(client)
                committed = @lock client.lock begin
                    if client.generation == generation && client.status == ConnectionStatus.RECONNECTING
                        client.status = ConnectionStatus.CONNECTED
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
                _flush_pending_buffer(client)
                _start_background_tasks!(client, generation)
                try opts.reconnected_cb() catch err _report_error(client, err) end
                return
            catch err
                @lock client.lock begin
                    if client.generation == generation && client.status != ConnectionStatus.CLOSED
                        client.status = ConnectionStatus.RECONNECTING
                        for sub in values(client.subscriptions)
                            sub.server_active = false
                        end
                    end
                end
                _close_transport_report_errors!(client)
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
        state = @lock client.lock begin
            if get(client.subscriptions, sub.sid, nothing) !== sub || sub.closed || sub.server_active
                nothing
            else
                remaining = sub.max_msgs > 0 ? max(0, sub.max_msgs - sub.received) : nothing
                remaining == 0 ? nothing : (sub.sid, sub.subject, sub.queue, remaining)
            end
        end
        isnothing(state) && continue
        sid, subject, queue, remaining = state
        _write_raw(client, _sub_cmd(subject, queue, sid))
        isnothing(remaining) || _write_raw(client, _unsub_cmd(sid, remaining))
        active = @lock client.lock begin
            if get(client.subscriptions, sid, nothing) === sub && !sub.closed
                sub.server_active = true
                true
            else
                false
            end
        end
        active || _write_raw(client, _unsub_cmd(sid))
    end
end

function _prepend_pending!(client::Client, data::Vector{UInt8})
    @lock client.lock begin
        existing = take!(client.pending)
        client.pending = IOBuffer()
        write(client.pending, data)
        write(client.pending, existing)
        client.pending_bytes = length(data) + length(existing)
    end
    nothing
end

function _flush_pending_buffer(client::Client)
    data = UInt8[]
    @lock client.lock begin
        data = take!(client.pending)
        client.pending_bytes = 0
    end
    isempty(data) && return
    try
        _write_raw(client, data)
    catch err
        _prepend_pending!(client, data)
        rethrow()
    end
end
