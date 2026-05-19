function _report_error(client::Client, err)
    _record_error!(client)
    try
        client.options.error_cb(err)
    catch callback_err
        @warn "Natter error callback failed" exception=(callback_err, catch_backtrace())
    end
end

function _connection_event(client::Client, kind::ConnectionEventKind.T; server=nothing,
                           url=nothing, attempt::Int=0, delay=nothing, err=nothing,
                           generation=nothing)::ConnectionEvent
    st, current_server, current_url, current_generation = @lock client.lock begin
        (client.status, client.current_server, client.connected_url, client.generation)
    end
    event_server = isnothing(server) ? current_server : server
    event_url = isnothing(url) ? current_url : String(url)
    event_delay = isnothing(delay) ? nothing : Float64(delay)
    event_error =
        if isnothing(err)
            nothing
        elseif err isa Exception
            err
        else
            ErrorException(string(err))
        end
    event_generation = isnothing(generation) ? current_generation : Int(generation)
    ConnectionEvent(kind, st, event_server, event_url, attempt, event_delay,
                    event_error, event_generation)
end

function _emit_connection_event(client::Client, kind::ConnectionEventKind.T; kwargs...)::ConnectionEvent
    event = _connection_event(client, kind; kwargs...)
    try
        client.options.event_cb(event)
    catch err
        _report_error(client, err)
    end
    event
end

function _resolve_reconnect_delay(client::Client, event::ConnectionEvent,
                                  default_delay::Float64)::Float64
    value = try
        client.options.reconnect_delay_cb(event)
    catch err
        _report_error(client, err)
        return default_delay
    end
    isnothing(value) && return default_delay
    if value isa Real && !(value isa Bool)
        seconds = Float64(value)
        isfinite(seconds) && seconds >= 0 && return seconds
    end
    _report_error(client, ArgumentError("reconnect_delay_cb must return nothing or a non-negative finite number of seconds"))
    default_delay
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
        _emit_connection_event(client, ConnectionEventKind.DISCOVERED_SERVERS)
    end
end

function _generation_matches(client::Client, generation::Int)
    _load_generation(client) == generation
end

function _sleep_interruptibly(client::Client, generation::Int, seconds::Real,
                              cancel_token::MaybeCancellationToken=nothing)
    deadline = time() + max(0.0, seconds)
    while time() < deadline
        _throw_if_cancelled(cancel_token)
        _generation_matches(client, generation) || return false
        status(client) in (ConnectionStatus.CLOSED, ConnectionStatus.DISCONNECTED) && return false
        sleep(min(0.05, max(0.0, deadline - time())))
    end
    _throw_if_cancelled(cancel_token)
    _generation_matches(client, generation) &&
        !(status(client) in (ConnectionStatus.CLOSED, ConnectionStatus.DISCONNECTED))
end

function _reader_loop(client::Client, generation::Int)
    reader = @lock client.lock client.reader
    if isnothing(reader)
        read_io = @lock client.lock client.read_io
        isnothing(read_io) && return
        reader = ProtocolReader(read_io; read_size=client.options.read_buffer_size,
                                shrink_threshold=client.options.read_buffer_shrink_threshold)
        @lock client.lock client.reader = reader
    end
    _reader_loop_with_reader(client, generation, reader)
    nothing
end

struct _ReaderMsgDispatcher{C<:Client}
    client::C
end

@inline function (dispatcher::_ReaderMsgDispatcher)(msg::_ProtocolMsg)
    _dispatch_msg(dispatcher.client, msg)
    nothing
end

function _reader_loop_with_reader(client::Client, generation::Int,
                                  reader::ProtocolReader{ReadIO}) where {ReadIO}
    route_resolver = _ReaderMsgRouteResolver(client)
    msg_dispatcher = _ReaderMsgDispatcher(client)
    while _generation_matches(client, generation) && status(client) in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING)
        try
            frame = _read_control_or_msg_dispatch(reader, client.options, route_resolver,
                                                  msg_dispatcher)
            isnothing(frame) && continue
            if frame isa PingFrame
                _send_raw(client, "PONG$CRLF"; force_flush=true)
            elseif frame isa PongFrame
                _notify_pong(client)
            elseif frame isa InfoFrame
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
            elseif frame isa ErrFrame
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
        too_many, marker = @lock client.lock begin
            client.pings_out += 1
            if client.pings_out > client.options.max_outstanding_pings
                (true, nothing)
            else
                (false, _queue_ping_marker_locked!(client))
            end
        end
        if too_many
            _trigger_reconnect(client, TimeoutError("too many outstanding pings"))
            return
        end
        try
            _send_raw(client, "PING$CRLF"; force_flush=true)
        catch err
            @lock client.lock begin
                isnothing(marker) || _remove_pong_waiter_locked!(client, marker)
            end
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

function _mark_reconnect_pending!(client::Client)::Bool
    @lock client.lock begin
        client.status == ConnectionStatus.CONNECTED || return false
        client.write_reconnect_pending[] && return false
        client.write_reconnect_pending[] = true
        true
    end
end

function _begin_reconnect_transition!(client::Client)
    opts = client.options
    should_start = false
    generation = 0
    notify_subs = Subscription[]
    replayable = _PendingEntry[]
    dropped_replayable = 0
    transports = (nothing, nothing, nothing)

    _with_write_lock(client, "begin reconnect") do
        read_io, write_io, sock, preserve = @lock client.lock begin
            if client.status == ConnectionStatus.CONNECTED && client.write_reconnect_pending[]
                _bump_generation_locked!(client)
                generation = client.generation
                should_start = opts.allow_reconnect
                _store_status_locked!(client, should_start ? ConnectionStatus.RECONNECTING : ConnectionStatus.DISCONNECTED)
                client.flusher_task = nothing
                client.writer_task = nothing
                notify_subs = collect(values(client.subscriptions))
                read_io, write_io, sock = _take_transport_fields_locked!(client)
                read_io, write_io, sock, should_start
            else
                client.write_reconnect_pending[] = false
                nothing, nothing, nothing, false
            end
        end
        transports = (read_io, write_io, sock)
        if preserve && !isnothing(write_io)
            replayable = _take_replayable_writes!(write_io)
        elseif !isnothing(write_io)
            dropped_replayable = _take_replayable_bytes!(write_io)
        end
    end

    isempty(replayable) || _prepend_pending!(client, replayable; already_counted=true)
    _deactivate_writer_queue!(client; clear=true)
    _release_pending_bytes!(client, dropped_replayable)
    generation, should_start, notify_subs, transports
end

function _trigger_reconnect(client::Client, reason)
    _mark_reconnect_pending!(client) || return nothing
    generation, should_start, notify_subs, transports = _begin_reconnect_transition!(client)
    generation == 0 && return nothing
    for sub in notify_subs
        @lock sub.lock begin
            sub.server_active = false
            _notify_subscription_waiters_locked(sub; all=true)
        end
    end
    if should_start
        _notify_client_lifecycle_watchers!(client, ConnectionReconnectingError())
        _signal_flusher(client)
        _report_cleanup_errors(client, _notify_pong_waiters!(client, false))
        _close_transport_report_errors!(client, transports...)
        _emit_connection_event(client, ConnectionEventKind.DISCONNECTED; err=reason,
                               generation)
        should_spawn = @lock client.lock client.generation == generation && client.status == ConnectionStatus.RECONNECTING
        should_spawn || return nothing
        reconnect_task = _spawn_control(:reconnect) do
            _reconnect_loop(client, generation)
        end
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
        _close_transport_report_errors!(client, transports...)
        _terminal_disconnect!(client, generation, reason)
    end
    nothing
end

function _recover_after_write_failure!(client::Client, err)
    st, generation, reconnect_pending =
        @lock client.lock (client.status, client.generation, client.write_reconnect_pending[])
    reconnect_pending && st == ConnectionStatus.CONNECTED && return true
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
        servers = _server_attempt_order!(client)
        if isempty(servers)
            _terminal_disconnect!(client, generation, NoServersError())
            return
        end
        for server in servers
            _generation_matches(client, generation) && status(client) == ConnectionStatus.RECONNECTING || return
            _emit_connection_event(client, ConnectionEventKind.RECONNECT_ATTEMPT;
                                   server, url=server.url, attempt=attempts,
                                   generation)
            try
                _connect_once!(client, server; mark_connected=false, generation,
                               attempt=attempts, reconnect=true)
                _start_flusher_task!(client, generation)
                _start_writer_task!(client, generation)
                _record_reconnect!(client)
                committed = false
                lock(client.subscription_replay_lock)
                try
                    _replay_subscriptions_unlocked(client; reconnect_replay=true)
                    _flush_pending_buffer(client; generation=generation, reconnect_replay=true)
                    committed = @lock client.lock begin
                        if client.generation == generation && client.status == ConnectionStatus.RECONNECTING
                            _store_status_locked!(client, ConnectionStatus.CONNECTED)
                            true
                        else
                            false
                        end
                    end
                    if committed
                        _replay_subscriptions_unlocked(client)
                    end
                finally
                    unlock(client.subscription_replay_lock)
                end
                committed || begin
                    _close_transport_report_errors!(client)
                    return
                end
                _flush_pending_buffer(client; generation=generation)
                _flush_buffered_writes(client)
                _start_background_tasks!(client, generation)
                _emit_connection_event(client, ConnectionEventKind.RECONNECTED;
                                       server, url=server.url, attempt=attempts,
                                       generation)
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
        default_wait = @lock client.lock max(0.0, delay + rand(client.rng) * opts.reconnect_jitter)
        delay_event = _connection_event(client, ConnectionEventKind.RECONNECT_DELAY;
                                        attempt=attempts, delay=default_wait,
                                        generation)
        wait_time = _resolve_reconnect_delay(client, delay_event, default_wait)
        _emit_connection_event(client, ConnectionEventKind.RECONNECT_DELAY;
                               attempt=attempts, delay=wait_time, generation)
        _sleep_interruptibly(client, generation, wait_time) || return
        delay = min(opts.reconnect_max_wait, delay * 2)
    end
end

function _replay_subscriptions_unlocked(client::Client; reconnect_replay::Bool=false)
    write_mode = reconnect_replay ? _RAW_WRITE_RECONNECT_REPLAY : _RAW_WRITE_CONNECTED
    subs = @lock client.lock collect(values(client.subscriptions))
    for sub in subs
        present = @lock client.lock get(client.subscriptions, sub.sid, nothing) === sub
        state = @lock sub.lock begin
            if !present || sub.closed || sub.server_active
                nothing
            else
                delivered_base = sub.delivered
                remaining = sub.max_msgs > 0 ? sub.max_msgs - delivered_base : 0
                sub.max_msgs > 0 && remaining <= 0 ? :close :
                (sub.sid, sub.subject, sub.queue, remaining, delivered_base)
            end
        end
        isnothing(state) && continue
        if state === :close
            _close_subscription_locally!(sub; throw_errors=false)
            continue
        end
        sid, subject, queue, remaining, delivered_base = state
        _write_raw(client, _subscription_setup_cmd(subject, queue, sid, remaining); write_mode)
        present = @lock client.lock get(client.subscriptions, sid, nothing) === sub
        active = @lock sub.lock begin
            if present && !sub.closed
                sub.server_delivered_base = delivered_base
                sub.server_active = true
                true
            else
                false
            end
        end
        active || _write_raw(client, _unsub_cmd(sid); write_mode)
    end
end

function _replay_subscriptions(client::Client; reconnect_replay::Bool=false)
    lock(client.subscription_replay_lock)
    try
        _replay_subscriptions_unlocked(client; reconnect_replay)
    finally
        unlock(client.subscription_replay_lock)
    end
end

function _prepend_pending_locked!(client::Client, data::Vector{UInt8}; already_counted::Bool=false)
    bytes = length(data)
    already_counted || _reserve_pending_bytes_locked!(client, bytes)
    _prepend_pending_chunk!(client.pending, data)
    nothing
end

function _prepend_pending_locked!(client::Client, entries::Vector{_PendingEntry}; already_counted::Bool=false)
    bytes = _pending_entries_size(entries)
    already_counted || _reserve_pending_bytes_locked!(client, bytes)
    _prepend_pending_chunks!(client.pending, entries)
    nothing
end

function _prepend_pending!(client::Client, data::Vector{UInt8}; already_counted::Bool=false)
    @lock client.lock _prepend_pending_locked!(client, data; already_counted=already_counted)
    nothing
end

function _prepend_pending!(client::Client, entries::Vector{_PendingEntry}; already_counted::Bool=false)
    @lock client.lock _prepend_pending_locked!(client, entries; already_counted=already_counted)
    nothing
end

function _restore_pending_after_replay_failure!(client::Client, entries::Vector{_PendingEntry}, generation::Int)
    bytes = _pending_entries_size(entries)
    @lock client.lock begin
        if client.generation == generation && client.status in (ConnectionStatus.RECONNECTING, ConnectionStatus.CONNECTED)
            _prepend_pending_locked!(client, entries; already_counted=true)
        else
            _release_pending_bytes!(client, bytes)
        end
    end
    nothing
end

function _pending_replay_batch_size(client::Client)::Int
    threshold = max(0, client.options.write_buffer_size)
    threshold > 0 ? threshold : DEFAULT_WRITE_BUFFER_SIZE
end

function _write_pending_entries(client::Client, entries::Vector{_PendingEntry};
                                write_mode::_RawWriteMode)
    _with_write_lock(client, "write pending replay") do
        st = status(client)
        io = @atomic client.write_io
        reconnect_pending = client.write_reconnect_pending[]
        reconnect_pending && st == ConnectionStatus.CONNECTED &&
            write_mode != _RAW_WRITE_RECONNECT_REPLAY && throw(ConnectionReconnectingError())
        _ensure_raw_write_status(st, write_mode)
        isnothing(io) && throw(ConnectionClosedError("connection transport is closed"))

        data = if length(entries) == 1
            entries[1].data
        else
            _pending_entries_write_bytes!(client.write_scratch, entries)
        end
        _write_raw_data_to_io(client, io, data)
        _flush_or_signal_locked(client, io, true)
    end
    nothing
end

function _flush_pending_buffer(client::Client; generation::Union{Int,Nothing}=nothing,
                               reconnect_replay::Bool=false)
    write_mode = reconnect_replay ? _RAW_WRITE_RECONNECT_REPLAY : _RAW_WRITE_CONNECTED
    while true
        entries = _PendingEntry[]
        replay_generation = 0
        @lock client.lock begin
            replay_generation = isnothing(generation) ? client.generation : generation
            entries = _pop_pending_batch!(client.pending, _pending_replay_batch_size(client))
        end
        isempty(entries) && return
        try
            _validate_pending_replay_for_client(client, entries)
            _write_pending_entries(client, entries; write_mode)
            _release_pending_bytes!(client, _pending_entries_size(entries))
        catch err
            _restore_pending_after_replay_failure!(client, entries, replay_generation)
            rethrow()
        end
    end
end
