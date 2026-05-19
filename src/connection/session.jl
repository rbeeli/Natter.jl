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

function _queue_ping_marker_locked!(client::Client)::PongWaiter
    # Non-user keepalive PINGs still need a queue entry so their PONG cannot
    # satisfy a later user flush.
    waiter = PongWaiter(Base.Threads.Condition(client.lock), true, false, false)
    push!(client.pongs, waiter)
    waiter
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
            empty!(mux.deadline_queue)
            for waiter in waiters
                try
                    _resolve_request_waiter_locked!(waiter, err, mux.condition)
                catch notify_err
                    push!(errors, CleanupError("notify request waiter", notify_err))
                end
            end
            notify(mux.condition; all=true)
            _notify_request_timeout_task_locked(mux)
        finally
            unlock(mux.condition)
        end
    end
    errors
end

function _terminal_disconnect!(client::Client, generation::Int, err::Exception)
    subs = Subscription[]
    request_mux = nothing
    terminal_server = nothing
    terminal_url = nothing
    terminal = @lock client.lock begin
        if client.generation == generation &&
           client.status in (ConnectionStatus.CONNECTED, ConnectionStatus.RECONNECTING, ConnectionStatus.DISCONNECTED)
            _store_status_locked!(client, ConnectionStatus.DISCONNECTED)
            terminal_server = client.current_server
            terminal_url = client.connected_url
            client.current_server = nothing
            client.connected_url = nothing
            client.flusher_task = nothing
            client.writer_task = nothing
            client.reader_task = nothing
            client.ping_task = nothing
            client.reconnect_task = nothing
            client.pings_out = 0

            subs = collect(values(client.subscriptions))
            empty!(client.subscriptions)
            @atomic client.subscription_snapshot =
                Dict{Int,_SubscriptionSnapshotEntry{typeof(client)}}()
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

    _notify_client_lifecycle_watchers!(client, ConnectionClosedError("connection is disconnected"))

    for sub in subs
        @lock sub.lock begin
            sub.closed = true
            sub.server_active = false
            _notify_subscription_waiters_locked(sub; all=true)
        end
    end
    _signal_flusher(client)
    _deactivate_writer_queue!(client; close=true, clear=true)
    _signal_writer(client)
    errors = Any[]
    append!(errors, _notify_pong_waiters!(client, false))
    if !isnothing(request_mux)
        lock(request_mux.condition)
        try
            request_waiters = collect(values(request_mux.waiters))
            empty!(request_mux.waiters)
            empty!(request_mux.deadline_queue)
            for waiter in request_waiters
                try
                    _resolve_request_waiter_locked!(waiter, ConnectionClosedError("connection is disconnected"),
                                                    request_mux.condition)
                catch notify_err
                    push!(errors, CleanupError("notify request waiter", notify_err))
                end
            end
            notify(request_mux.condition; all=true)
            _notify_request_timeout_task_locked(request_mux)
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
    _emit_connection_event(client, ConnectionEventKind.TERMINAL_DISCONNECT;
                           server=terminal_server, url=terminal_url, err, generation)
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
            ("stop writer task", client.writer_task),
            ("stop write watchdog task", client.write_timeout_task),
        )
        client.reader_task = nothing
        client.ping_task = nothing
        client.reconnect_task = nothing
        client.flusher_task = nothing
        client.writer_task = nothing
        client.write_timeout_task = nothing
        client.write_deadline[] = time()
        tasks
    end
end

function _stop_client_tasks!(client::Client; timeout::Real=0.5, deadline=nothing)
    tasks = _take_client_tasks!(client)
    errors = Any[]
    for (operation, task) in tasks
        _wait_task!(errors, operation, task; timeout, interrupt=true, deadline=deadline)
    end
    errors
end

function _connect_command(client::Client, server::Server, info::ServerInfo, url_user, url_pass;
                          attempt::Int, reconnect::Bool)
    opts = client.options
    hdrs = info.headers === true
    if opts.no_echo && !(something(info.proto, 0) >= 1)
        throw(UnsupportedFeatureError("no_echo requires server protocol 1 support"))
    end
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
    request = AuthRequest(server, server.url, info.nonce, info, attempt, reconnect)
    auth = _resolve_connect_auth(opts, request, url_user, url_pass)
    isnothing(auth.token) || (body["auth_token"] = _secret_to_string(auth.token))
    if !isnothing(auth.user)
        body["user"] = auth.user
        body["pass"] = _secret_to_string(auth.password)
    end
    isnothing(auth.jwt) || (body["jwt"] = auth.jwt)
    isnothing(auth.nkey) || (body["nkey"] = auth.nkey)
    isnothing(auth.sig) || (body["sig"] = auth.sig)
    "CONNECT $(JSON3.write(body))$CRLF"
end

function _connect_once!(client::Client, server::Server; mark_connected::Bool=true,
                        generation::Union{Nothing,Int}=nothing, attempt::Int=1,
                        reconnect::Bool=!isnothing(generation),
                        cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    scheme, host, port, url_user, url_pass = _server_parts(server.url)
    tls_host = _tls_server_name(client.options, server, host)
    deadline::Float64 = time() + client.options.connect_timeout
    sock = _connect_tcp(host, port, _remaining_timeout(deadline), client.options.tcp_nodelay,
                        cancel_token)
    read_io = sock
    write_io = sock
    reader = ProtocolReader(read_io; read_size=client.options.read_buffer_size,
                            shrink_threshold=client.options.read_buffer_shrink_threshold)
    cleanup = () -> _close_transport(read_io, write_io, sock)
    report_timeout_cleanup = errors -> _report_cleanup_errors(client, errors)
    try
        tls_active::Bool = false
        if _tls_first_for_connection(client.options, scheme)
            _throw_if_cancelled(cancel_token)
            tls = _run_interruptible_io_with_timeout("TLS handshake", _remaining_timeout(deadline), cleanup, report_timeout_cleanup) do
                _tls_wrap(sock, client.options, tls_host)
            end
            read_io = tls
            write_io = tls
            reader = ProtocolReader(read_io; read_size=client.options.read_buffer_size,
                                    shrink_threshold=client.options.read_buffer_shrink_threshold)
            tls_active = true
        end
        _throw_if_cancelled(cancel_token)
        frame = _run_interruptible_io_with_timeout("connect INFO read", _remaining_timeout(deadline), cleanup, report_timeout_cleanup) do
            _read_control_or_msg(reader, client.options)
        end
        frame isa InfoFrame || throw(ProtocolError("expected INFO during connect"))
        info = _protocol_info(frame)
        wants_tls::Bool = !tls_active && (scheme == "tls" || client.options.tls_required || info.tls_required === true)
        if wants_tls
            _throw_if_cancelled(cancel_token)
            available = something(info.tls_available, info.tls_required === true)
            available == true || throw(ProtocolError("TLS requested but server did not advertise TLS availability"))
            tls = _run_interruptible_io_with_timeout("TLS handshake", _remaining_timeout(deadline), cleanup, report_timeout_cleanup) do
                _tls_wrap(sock, client.options, tls_host)
            end
            read_io = tls
            write_io = tls
            reader = ProtocolReader(read_io; read_size=client.options.read_buffer_size,
                                    shrink_threshold=client.options.read_buffer_shrink_threshold)
            tls_active = true
        end
        _throw_if_cancelled(cancel_token)
        connect_cmd = _run_with_timeout("connect auth resolution", _remaining_timeout(deadline), cleanup, report_timeout_cleanup) do
            _connect_command(client, server, info, url_user, url_pass; attempt, reconnect)
        end
        _throw_if_cancelled(cancel_token)
        _run_interruptible_io_with_timeout("connect command write", _remaining_timeout(deadline), cleanup, report_timeout_cleanup) do
            write(write_io, connect_cmd)
            write(write_io, "PING$CRLF")
            flush(write_io)
        end
        while true
            _throw_if_cancelled(cancel_token)
            frame = _run_interruptible_io_with_timeout("connect PONG read", _remaining_timeout(deadline), cleanup, report_timeout_cleanup) do
                _read_control_or_msg(reader, client.options)
            end
            if frame isa PongFrame
                break
            elseif frame isa PingFrame
                _throw_if_cancelled(cancel_token)
                _run_interruptible_io_with_timeout("connect PONG write", _remaining_timeout(deadline), cleanup, report_timeout_cleanup) do
                    write(write_io, "PONG$CRLF")
                    flush(write_io)
                end
            elseif frame isa ErrFrame
                throw(_server_err(_protocol_err(frame)))
            elseif frame isa OkFrame
                continue
            else
                throw(ProtocolError("unexpected $(_protocol_op(frame)) during connect"))
            end
        end
        accepted = _with_write_lock(client, "activate transport") do
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
                    @atomic client.write_io = _write_transport_for_options(write_io, client.options)
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
        mark_connected && _emit_connection_event(client, ConnectionEventKind.CONNECTED; server,
                                                 url=server.url, attempt)
        return nothing
    catch err
        err isa TimeoutError || _close_transport_report_errors!(client, read_io, write_io, sock)
        rethrow()
    end
end

function _try_connect_initial_servers!(client::Client, generation::Int, attempt_counter::Base.RefValue{Int};
                                       cancel_token::MaybeCancellationToken=nothing)
    last_err = nothing
    servers = _server_attempt_order!(client)
    for server in servers
        _throw_if_cancelled(cancel_token)
        attempt_counter[] += 1
        try
            _connect_once!(client, server; generation, attempt=attempt_counter[],
                           reconnect=false, cancel_token)
            _start_background_tasks!(client, generation)
            return nothing
        catch err
            err isa CancelledError && rethrow()
            if err isa AuthenticationError && _record_auth_error!(client, server, err)
                _report_error(client, err)
                throw(err)
            end
            last_err = err
            _report_error(client, err)
        end
    end
    isnothing(last_err) ? NoServersError() : last_err
end

function _sleep_before_initial_retry!(client::Client, generation::Int, retry_attempt::Int,
                                      delay::Float64, cancel_token::MaybeCancellationToken)::Float64
    default_wait = @lock client.lock max(0.0, delay + rand(client.rng) * client.options.reconnect_jitter)
    delay_event = _connection_event(client, ConnectionEventKind.RECONNECT_DELAY;
                                    attempt=retry_attempt, delay=default_wait,
                                    generation)
    wait_time = _resolve_reconnect_delay(client, delay_event, default_wait)
    _emit_connection_event(client, ConnectionEventKind.RECONNECT_DELAY;
                           attempt=retry_attempt, delay=wait_time, generation)
    _sleep_interruptibly(client, generation, wait_time, cancel_token) ||
        throw(ConnectionClosedError("connection state changed while connecting"))
    min(client.options.reconnect_max_wait, delay * 2)
end

function _connect_initial!(client::Client; cancel_token::MaybeCancellationToken=nothing)
    generation = @lock client.lock begin
        _store_status_locked!(client, ConnectionStatus.CONNECTING)
        _bump_generation_locked!(client)
    end
    opts = client.options
    attempt_counter = Ref(0)
    retry_attempt = 0
    delay = opts.reconnect_wait
    try
        while true
            last_err = _try_connect_initial_servers!(client, generation, attempt_counter; cancel_token)
            isnothing(last_err) && return client

            if !opts.retry_on_initial_connect ||
               (opts.max_reconnect_attempts >= 0 && retry_attempt >= opts.max_reconnect_attempts)
                throw(last_err)
            end

            retry_attempt += 1
            delay = _sleep_before_initial_retry!(client, generation, retry_attempt, delay, cancel_token)
        end
    catch err
        @lock client.lock begin
            if client.generation == generation && client.status == ConnectionStatus.CONNECTING
                _store_status_locked!(client, ConnectionStatus.DISCONNECTED)
            end
        end
        rethrow()
    end
end

function _start_flusher_task!(client::Client, generation::Int=(@lock client.lock client.generation))
    assigned, flush_signal = @lock client.lock begin
        existing = client.flusher_task
        if client.generation == generation &&
           client.status in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING, ConnectionStatus.RECONNECTING) &&
           (isnothing(existing) || istaskdone(existing))
            (:start, @atomic client.flush_signal)
        else
            (:skip, nothing)
        end
    end
    assigned == :start || return nothing
    flusher_task = _spawn_control(:flusher) do
        _flusher_loop(client, generation, flush_signal)
    end
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
    _start_writer_task!(client, generation)
    reader_task = _spawn_control(:reader) do
        _reader_loop(client, generation)
    end
    ping_task = _spawn_control(:ping) do
        _ping_loop(client, generation)
    end
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

function _flusher_loop(client::Client, generation::Int, signal::FlushSignal)
    while _generation_matches(client, generation) &&
          status(client) in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING, ConnectionStatus.RECONNECTING)
        try
            _wait_flush_signal(signal)
        catch err
            err isa InvalidStateException && return
            _report_error(client, err)
            return
        end
        latency = max(0.0, client.options.write_buffer_latency)
        latency > 0 ? sleep(latency) : yield()
        try
            _consume_flush_signal!(signal)
        catch err
            err isa InvalidStateException && return
            _report_error(client, err)
            return
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
