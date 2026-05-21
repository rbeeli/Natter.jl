function _socket_connecting(sock::Sockets.TCPSocket)::Bool
    lock(sock.cond)
    try
        sock.status == Base.StatusConnecting
    finally
        unlock(sock.cond)
    end
end

function _resolve_connect_address(host::String, port::Int, timeout::Real,
                                  cancel_token::MaybeCancellationToken=nothing;
                                  resolver=Sockets.getaddrinfo)
    operation = "connect to $host:$port"
    _throw_if_cancelled(cancel_token)
    timeout = Float64(timeout)
    timeout > 0 || throw(TimeoutError("$operation timed out"))

    ch = Channel{Tuple{Bool,Any}}(1)
    timed_out = Threads.Atomic{Bool}(false)
    task = _spawn_sticky(:dns_resolution) do
        try
            value = resolver(host)
            timed_out[] || put!(ch, (true, value))
        catch err
            timed_out[] || put!(ch, (false, err))
        end
    end
    result = timedwait(timeout; pollint=0.01) do
        isready(ch) || iscancelled(cancel_token)
    end
    if !isready(ch)
        timed_out[] = true
        _schedule_timeout_cleanup("$operation DNS resolution", () -> nothing; task)
        iscancelled(cancel_token) && throw(CancelledError("$operation cancelled"))
        result == :timed_out && throw(TimeoutError("$operation timed out"))
    end
    ok, value = take!(ch)
    ok || throw(value)
    _throw_if_cancelled(cancel_token)
    value
end

function _connect_tcp(host::String, port::Int, timeout::Real, tcp_nodelay::Bool,
                      cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    timeout = Float64(timeout)
    timeout > 0 || throw(TimeoutError("connect to $host:$port timed out"))
    deadline = time() + timeout
    addr = _resolve_connect_address(host, port, _remaining_timeout(deadline),
                                    cancel_token)
    sock = Sockets.TCPSocket()
    connected = false
    try
        Sockets.connect!(sock, addr, port)
    catch
        errors = Any[]
        _close_resource!(errors, "close failed connect socket", sock)
        _warn_timeout_cleanup_errors("connect to $host:$port", errors)
        rethrow()
    end
    try
        wait_result = timedwait(_remaining_timeout(deadline); pollint=0.01) do
            !_socket_connecting(sock) || iscancelled(cancel_token)
        end
        if iscancelled(cancel_token)
            errors = Any[]
            _close_resource!(errors, "close cancelled connect socket", sock)
            _warn_timeout_cleanup_errors("connect to $host:$port", errors)
            throw(CancelledError("connect to $host:$port cancelled"))
        end
        if wait_result == :timed_out
            errors = Any[]
            _close_resource!(errors, "close timed-out connect socket", sock)
            _warn_timeout_cleanup_errors("connect to $host:$port", errors)
            throw(TimeoutError("connect to $host:$port timed out"))
        end
        Sockets.wait_connected(sock)
        Sockets.nagle(sock, !tcp_nodelay)
        connected = true
        return sock
    finally
        if !connected
            errors = Any[]
            _close_resource!(errors, "close failed connect socket", sock)
            _warn_timeout_cleanup_errors("connect to $host:$port", errors)
        end
    end
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
    # Interrupting migratable tasks can trip Julia runtime edge cases. Only
    # tasks deliberately created as sticky are force-interrupted; spawned tasks
    # are expected to stop through cooperative close/generation signals.
    task.sticky || return false
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

_task_wait_timeout(timeout::Real, deadline)::Float64 =
    isnothing(deadline) ? Float64(timeout) : min(Float64(timeout), _remaining_timeout(deadline))

function _wait_task!(errors::Vector, operation::String, task::Union{Task,Nothing};
                     timeout::Real=0.5, interrupt::Bool=false, interrupt_first::Bool=false,
                     deadline=nothing)
    (isnothing(task) || istaskdone(task) || task === current_task()) && return errors

    interrupted = interrupt && interrupt_first && _request_task_stop!(errors, operation, task)
    result = timedwait(_task_wait_timeout(timeout, deadline); pollint=0.005) do
        istaskdone(task)
    end
    if result == :timed_out && interrupt && !interrupted
        interrupted = _request_task_stop!(errors, operation, task)
        if !isnothing(deadline) && _remaining_timeout(deadline) <= 0
            push!(errors, CleanupError(operation, TimeoutError("$operation timed out")))
            return errors
        end
        grace = isnothing(deadline) ? min(timeout, 0.5) : min(0.5, _remaining_timeout(deadline))
        result = timedwait(grace; pollint=0.005) do
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
    _spawn_sticky(:timeout_cleanup) do
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
    task = _spawn_sticky(:timeout_operation) do
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

function _run_interruptible_io_with_timeout(f::Function, operation::String, timeout::Real, cleanup::Function,
                                            report_cleanup_errors::Function=errors -> _warn_timeout_cleanup_errors(operation, errors))
    if timeout <= 0
        _schedule_timeout_cleanup(operation, cleanup, report_cleanup_errors)
        throw(TimeoutError("$operation timed out"))
    end

    timed_out = Threads.Atomic{Bool}(false)
    timer = Timer(Float64(timeout)) do _
        timed_out[] = true
        errors = Any[]
        try
            append!(errors, _cleanup_errors(cleanup()))
        catch err
            push!(errors, CleanupError("timeout cleanup after $operation", err))
        end
        if !isempty(errors)
            try
                report_cleanup_errors(errors)
            catch err
                @warn "Natter timeout cleanup error reporter failed" operation exception=(err, catch_backtrace())
            end
        end
    end
    try
        value = f()
        timed_out[] && throw(TimeoutError("$operation timed out"))
        value
    catch err
        timed_out[] && throw(TimeoutError("$operation timed out"))
        rethrow()
    finally
        close(timer)
    end
end

_remaining_timeout(deadline::Float64)::Float64 = max(0.0, deadline - time())

function _remaining_timeout_or_throw(deadline::Float64, operation::AbstractString;
                                     cancel_token::MaybeCancellationToken=nothing)::Float64
    _throw_if_cancelled(cancel_token)
    remaining = _remaining_timeout(deadline)
    remaining > 0 || throw(TimeoutError("$operation timed out"))
    remaining
end

function _wait_write_lock_signal_locked(condition::Base.GenericCondition{ReentrantLock},
                                        timeout::Real;
                                        cancel_token::MaybeCancellationToken=nothing)::Bool
    _throw_if_cancelled(cancel_token)
    seconds = Float64(timeout)
    seconds > 0 || return false
    registration = _register_cancellation_waiter(cancel_token, condition)
    timed_out = Ref(false)
    timer = isfinite(seconds) ? Timer(seconds) do _
        lock(condition)
        try
            timed_out[] = true
            notify(condition; all=true)
        finally
            unlock(condition)
        end
    end : nothing
    try
        while !timed_out[]
            wait(condition)
            _throw_if_cancelled(cancel_token)
            timed_out[] && return false
            return true
        end
        false
    finally
        _deregister_cancellation_waiter!(cancel_token, registration)
        isnothing(timer) || close(timer)
    end
end

function _lock_write!(client::Client, operation::String, deadline,
                      cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    if isnothing(deadline) && isnothing(cancel_token)
        lock(client.write_lock)
        return nothing
    end

    Threads.atomic_add!(client.write_waiters, 1)
    try
        lock(client.write_condition)
        try
            while !trylock(client.write_lock)
                _throw_if_cancelled(cancel_token)
                if isnothing(deadline)
                    _wait_write_lock_signal_locked(client.write_condition, Inf; cancel_token)
                else
                    remaining = _remaining_timeout(deadline)
                    remaining <= 0 && throw(TimeoutError("$operation timed out"))
                    _wait_write_lock_signal_locked(client.write_condition, remaining; cancel_token) ||
                        throw(TimeoutError("$operation timed out"))
                end
            end
        finally
            unlock(client.write_condition)
        end
    finally
        Threads.atomic_sub!(client.write_waiters, 1)
    end
    nothing
end

function _unlock_write!(client::Client)
    if client.write_waiters[] <= 0
        unlock(client.write_lock)
        return nothing
    end
    lock(client.write_condition)
    try
        unlock(client.write_lock)
        notify(client.write_condition; all=true)
    finally
        unlock(client.write_condition)
    end
    nothing
end

function _with_write_lock(f::Function, client::Client, operation::String; deadline=nothing,
                          cancel_token::MaybeCancellationToken=nothing)
    _lock_write!(client, operation, deadline, cancel_token)
    try
        return f()
    finally
        _unlock_write!(client)
    end
end

function _write_timeout_error(operation::String)
    TimeoutError("$operation timed out")
end

@inline _write_watchdog_enabled(::IO) = true
@inline _write_watchdog_enabled(::IOBuffer) = false
@inline _write_watchdog_enabled(client::Client, io)::Bool =
    isfinite(client.options.write_timeout) && _write_watchdog_enabled(io)

function _write_timeout_matches(active, io)::Bool
    isnothing(active) && return false
    active === io && return true
    _underlying_transport(active) === _underlying_transport(io)
end

function _write_timeout_transports(client::Client, io)
    @lock client.lock begin
        write_io = @atomic client.write_io
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
        client.write_timeout_task = _spawn_control(:write_watchdog) do
            _write_watchdog_loop(client)
        end
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
    _write_watchdog_enabled(client, io) || return f()
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
    hostname_is_ip = _host_is_ip(hostname)
    verify_ip_san = opts.tls_verify && hostname_is_ip
    authmode = verify_ip_san ? MbedTLS.MBEDTLS_SSL_VERIFY_OPTIONAL : _tls_authmode(opts)
    conf = _tls_config(opts, authmode)
    ctx = MbedTLS.SSLContext()
    MbedTLS.setup!(ctx, conf)
    MbedTLS.set_bio!(ctx, sock)
    if !hostname_is_ip && isdefined(MbedTLS, :hostname!)
        getfield(MbedTLS, :hostname!)(ctx, hostname)
    end
    MbedTLS.handshake(ctx)
    if verify_ip_san
        _tls_verify_peer_chain!(ctx)
        _tls_verify_ip_san!(ctx, hostname)
    end
    ctx
end

function _throw_errors(errors::Vector)
    isempty(errors) && return nothing
    length(errors) == 1 ? throw(first(errors)) : throw(Base.CompositeException(errors))
end

function _record_error!(client::Client)
    client.options.record_stats || return nothing
    _stat_add!(client.stats.errors)
    nothing
end

function _record_in!(client::Client, bytes::Int)
    client.options.record_stats || return nothing
    _stat_add!(client.stats.in_msgs)
    _stat_add!(client.stats.in_bytes, bytes)
    nothing
end

function _record_out!(client::Client, bytes::Int)
    client.options.record_stats || return nothing
    _stat_add!(client.stats.out_msgs)
    _stat_add!(client.stats.out_bytes, bytes)
    nothing
end

function _record_drop!(client::Client)
    client.options.record_stats || return nothing
    _stat_add!(client.stats.dropped_msgs)
    nothing
end

function _record_reconnect!(client::Client)
    client.options.record_stats || return nothing
    _stat_add!(client.stats.reconnects)
    nothing
end

function _signal_flusher(client::Client)
    _notify_flush_signal(@atomic client.flush_signal)
    nothing
end

function _flush_buffered_writes(client::Client; allow_missing::Bool=false, deadline=nothing,
                                cancel_token::MaybeCancellationToken=nothing)
    _with_write_lock(client, "flush buffered writes"; deadline, cancel_token) do
        io = @atomic client.write_io
        if isnothing(io)
            allow_missing && return false
            throw(ConnectionClosedError("connection transport is closed"))
        end
        _flush_write_io(client, io)
    end
    true
end

function _reserve_pending_bytes_locked!(client::Client, bytes::Int)
    bytes <= 0 && return nothing
    limit = client.options.pending_size
    while true
        current = @atomic client.pending_bytes
        projected = current + bytes
        projected > limit && throw(OutboundBufferLimitError(limit, projected))
        replaced = @atomicreplace client.pending_bytes current => projected
        replaced.success && return nothing
    end
end

_reconnect_buffer_enabled(client::Client)::Bool = client.options.pending_size > 0

function _reserve_pending_bytes!(client::Client, bytes::Int)
    _reserve_pending_bytes_locked!(client, bytes)
    nothing
end

function _release_pending_bytes!(client::Client, bytes::Int)
    bytes <= 0 && return nothing
    while true
        current = @atomic client.pending_bytes
        updated = max(0, current - bytes)
        replaced = @atomicreplace client.pending_bytes current => updated
        replaced.success && return nothing
    end
end

function _clear_pending_buffer_locked!(client::Client)
    empty!(client.pending)
    @atomic client.pending_bytes = 0
    nothing
end

function _clear_pending_buffer!(client::Client)
    @lock client.lock _clear_pending_buffer_locked!(client)
    nothing
end

_replayable_bytes(::IO) = 0
_replayable_bytes(io::BufferedWriteIO) = io.replayable_bytes

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
    try
        _run_transport_write(client, transport, "transport flush") do
            n > 0 && _write_buffered_bytes(transport, io.buffer.data, n)
            flush(transport)
        end
    catch
        _release_pending_bytes!(client, _take_replayable_bytes!(io))
        rethrow()
    end
    truncate(io.buffer, 0)
    seekstart(io.buffer)
    empty!(io.replayable_entries)
    io.replayable_bytes = 0
    _release_pending_bytes!(client, replayed)
    nothing
end

_should_flush_write_io(client::Client, io; force_flush::Bool=false)::Bool =
    _should_flush_write_io(client, io, force_flush)

function _should_flush_write_io(client::Client, io, force_flush::Bool)::Bool
    buffered = _buffered_bytes(io)
    threshold = max(0, client.options.write_buffer_size)
    force_flush || (buffered > 0 && threshold == 0) || (threshold > 0 && buffered >= threshold)
end

_flush_or_signal_locked(client::Client, io; force_flush::Bool=false) =
    _flush_or_signal_locked(client, io, force_flush, false)

function _flush_or_signal_locked(client::Client, io, force_flush::Bool,
                                 had_buffered::Bool=false)
    buffered = _buffered_bytes(io)
    if _should_flush_write_io(client, io, force_flush)
        _flush_write_io(client, io)
    elseif buffered > 0 && !had_buffered
        _signal_flusher(client)
    end
    nothing
end

@enum _RawWriteMode begin
    _RAW_WRITE_CONNECTED
    _RAW_WRITE_DRAIN
    _RAW_WRITE_RECONNECT_REPLAY
end

function _throw_raw_write_status(st::ConnectionStatus.T)
    st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
    st == ConnectionStatus.DISCONNECTED && throw(ConnectionClosedError("connection is disconnected"))
    st == ConnectionStatus.DRAINING && throw(ConnectionDrainingError())
    throw(ConnectionReconnectingError())
end

function _ensure_raw_write_status(st::ConnectionStatus.T, mode::_RawWriteMode)
    if mode == _RAW_WRITE_CONNECTED
        st == ConnectionStatus.CONNECTED && return nothing
    elseif mode == _RAW_WRITE_DRAIN
        (st == ConnectionStatus.CONNECTED || st == ConnectionStatus.DRAINING) && return nothing
    elseif mode == _RAW_WRITE_RECONNECT_REPLAY
        st == ConnectionStatus.RECONNECTING && return nothing
    end
    _throw_raw_write_status(st)
end

function _write_raw(client::Client, data::Union{AbstractString,Vector{UInt8}}; force_flush::Bool=false,
                    deadline=nothing, write_mode::_RawWriteMode=_RAW_WRITE_CONNECTED,
                    cancel_token::MaybeCancellationToken=nothing)
    _writer_barrier!(client; deadline, cancel_token)
    _with_write_lock(client, "write protocol command"; deadline, cancel_token) do
        st = status(client)
        io = @atomic client.write_io
        reconnect_pending = client.write_reconnect_pending[]
        reconnect_pending && st == ConnectionStatus.CONNECTED &&
            write_mode != _RAW_WRITE_RECONNECT_REPLAY && throw(ConnectionReconnectingError())
        _ensure_raw_write_status(st, write_mode)
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

function _close_resource!(errors::Vector, operation::String, resource; deadline=nothing)
    isnothing(resource) && return errors
    close_resource() = (close(resource); nothing)
    try
        if isnothing(deadline)
            close_resource()
        else
            remaining = _remaining_timeout(deadline)
            if remaining <= 0
                _schedule_timeout_cleanup(operation, close_resource)
                throw(TimeoutError("$operation timed out"))
            end
            _run_with_timeout(close_resource, operation, remaining, () -> nothing)
        end
    catch err
        push!(errors, CleanupError(operation, err))
    end
    errors
end

function _close_transport(read_io, write_io, sock; deadline=nothing)
    errors = Any[]
    seen = Any[]
    for (operation, io) in (("close read transport", read_io), ("close write transport", write_io), ("close socket", sock))
        isnothing(io) && continue
        transport = _underlying_transport(io)
        any(x -> x === transport, seen) && continue
        push!(seen, transport)
        _close_resource!(errors, operation, transport; deadline)
    end
    errors
end

function _take_transport_fields_locked!(client::Client)
    read_io = client.read_io
    write_io = @atomic client.write_io
    sock = client.socket
    client.read_io = nothing
    client.reader = nothing
    @atomic client.write_io = nothing
    client.socket = nothing
    read_io, write_io, sock
end

function _take_transport!(client::Client; preserve_replayable::Bool=false, deadline=nothing)
    replayable = _PendingEntry[]
    dropped_replayable = 0
    transports = _with_write_lock(client, "close transport"; deadline) do
        read_io, write_io, sock, preserve = @lock client.lock begin
            read_io, write_io, sock = _take_transport_fields_locked!(client)
            preserve = preserve_replayable && client.status != ConnectionStatus.CLOSED
            read_io, write_io, sock, preserve
        end
        if preserve && !isnothing(write_io)
            replayable = _take_replayable_writes!(write_io)
        elseif !isnothing(write_io)
            dropped_replayable = _take_replayable_bytes!(write_io)
        end
        read_io, write_io, sock
    end
    isempty(replayable) || _prepend_pending!(client, replayable; already_counted=true)
    _release_pending_bytes!(client, dropped_replayable)
    transports
end

function _abort_transport_for_blocked_write_lock!(client::Client; deadline=nothing)
    read_io, write_io, sock = @lock client.lock _take_transport_fields_locked!(client)
    errors = _close_transport(read_io, write_io, sock; deadline)
    _report_cleanup_errors(client, errors)
    nothing
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

function _close_transport!(client::Client; deadline=nothing)
    errors = _close_transport(_take_transport!(client; deadline)...; deadline)
    _throw_errors(errors)
    nothing
end

function _close_transport_report_errors!(client::Client; preserve_replayable::Bool=false, deadline=nothing)
    errors = _close_transport(_take_transport!(client; preserve_replayable, deadline)...; deadline)
    _report_cleanup_errors(client, errors)
    nothing
end

function _close_transport_report_errors!(client::Client, read_io, write_io, sock)
    errors = _close_transport(read_io, write_io, sock)
    _report_cleanup_errors(client, errors)
    nothing
end
