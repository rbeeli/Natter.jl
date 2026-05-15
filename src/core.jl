function _ensure_usable_status_for_publish(st::ConnectionStatus.T)
    st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
    st == ConnectionStatus.DRAINING && throw(ConnectionDrainingError())
    st == ConnectionStatus.DISCONNECTED && throw(ConnectionClosedError("connection is disconnected"))
    nothing
end

function _send_raw(client::Client, data::Union{AbstractString,Vector{UInt8}}; buffer_on_reconnect::Bool=false,
                   force_flush::Bool=false, deadline=nothing)
    st = status(client)
    if st in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING)
        write_mode = st == ConnectionStatus.DRAINING ? _RAW_WRITE_DRAIN : _RAW_WRITE_CONNECTED
        try
            _write_raw(client, data; force_flush, deadline, write_mode)
        catch err
            if st == ConnectionStatus.CONNECTED && _recover_after_write_failure!(client, err)
                if buffer_on_reconnect
                    _enqueue_pending(client, data)
                    return nothing
                end
                throw(ConnectionReconnectingError())
            end
            rethrow()
        end
    elseif st in (ConnectionStatus.RECONNECTING, ConnectionStatus.CONNECTING)
        if buffer_on_reconnect
            _enqueue_pending(client, data)
        else
            throw(ConnectionReconnectingError())
        end
    elseif st == ConnectionStatus.DISCONNECTED
        throw(ConnectionClosedError("connection is disconnected"))
    else
        throw(ConnectionClosedError())
    end
    nothing
end

_should_write_publish_direct(frame_size::Int, threshold::Int)::Bool =
    threshold <= 0 || frame_size >= threshold

function _write_publish_frame(client::Client, io::IO, frame::_AbstractPublishFrame; replayable::Bool=false,
                              threshold::Int=0, frame_size::Int=_serialized_size(frame))
    _write_pub_frame_direct_timed(client, io, frame)
    false
end

function _write_publish_frame(client::Client, io::BufferedWriteIO, frame::_AbstractPublishFrame; replayable::Bool=false,
                              threshold::Int=0, frame_size::Int=_serialized_size(frame))
    _ensure_open(io)
    if _should_write_publish_direct(frame_size, threshold)
        _flush_write_io(client, io)
        transport = _underlying_transport(io)
        _write_pub_frame_direct_timed(client, transport, frame; force_flush=true)
        return false
    end

    start = position(io.buffer) + 1
    entry_count = length(io.replayable_entries)
    bytes = frame_size
    write_frame = frame
    if replayable
        _reserve_pending_bytes!(client, bytes)
        try
            write_frame = _snapshot_publish_frame(frame)
            push!(io.replayable_entries, _PendingPublish(write_frame, bytes))
        catch
            _release_pending_bytes!(client, bytes)
            rethrow()
        end
    end
    try
        _write_pub_frame(io, write_frame)
    catch
        truncate(io.buffer, start - 1)
        seekend(io.buffer)
        resize!(io.replayable_entries, entry_count)
        replayable && _release_pending_bytes!(client, bytes)
        rethrow()
    end
    replayable
end

function _write_pub_frame_direct_timed(client::Client, io::IO, frame::_AbstractPublishFrame; force_flush::Bool=false)
    _run_transport_write(client, io, "publish write") do
        _write_pub_frame_direct(io, frame, client.write_scratch)
        force_flush && flush(io)
    end
    nothing
end

function _write_publish(client::Client, frame::_AbstractPublishFrame; force_flush::Bool=false,
                        replayable::Bool=false, frame_size::Int=_serialized_size(frame))::Bool
    captured = false
    @lock client.write_lock begin
        st, io = @lock client.lock (client.status, client.write_io)
        if !(st == ConnectionStatus.CONNECTED || st == ConnectionStatus.DRAINING)
            st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
            st == ConnectionStatus.DISCONNECTED && throw(ConnectionClosedError("connection is disconnected"))
            throw(ConnectionReconnectingError())
        end
        isnothing(io) && throw(ConnectionClosedError("connection transport is closed"))
        captured = _write_publish_to_io(client, io, frame, force_flush, replayable, frame_size)
    end
    captured
end

function _write_publish_to_io(client::Client, io::WriteIO, frame::_AbstractPublishFrame, force_flush::Bool,
                              replayable::Bool, frame_size::Int) where {WriteIO<:IO}
    threshold = max(0, client.options.write_buffer_size)
    captured = _write_publish_frame(client, io, frame; replayable, threshold, frame_size)
    _flush_or_signal_locked(client, io; force_flush)
    captured
end

function _throw_not_connected_for_request(st::ConnectionStatus.T)
    st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
    st == ConnectionStatus.DISCONNECTED && throw(ConnectionClosedError("connection is disconnected"))
    st == ConnectionStatus.DRAINING && throw(ConnectionDrainingError())
    throw(ConnectionReconnectingError())
end

function _ensure_connected_for_request(client::Client)
    st = status(client)
    st == ConnectionStatus.CONNECTED && return nothing
    _throw_not_connected_for_request(st)
end

function _send_publish(client::Client, frame::_AbstractPublishFrame; buffer_on_reconnect::Bool=true,
                       force_flush::Bool=false,
                       frame_size::Int=_serialized_size(frame),
                       st::ConnectionStatus.T=status(client))
    if st in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING)
        replayable_captured = false
        try
            replayable_captured = _write_publish(client, frame; force_flush,
                                                 replayable=buffer_on_reconnect, frame_size)
        catch err
            err isa OutboundBufferLimitError && rethrow()
            if st == ConnectionStatus.CONNECTED && _recover_after_write_failure!(client, err)
                if buffer_on_reconnect
                    replayable_captured || _enqueue_pending(client, frame)
                    return nothing
                end
                throw(ConnectionReconnectingError())
            end
            rethrow()
        end
    elseif st in (ConnectionStatus.RECONNECTING, ConnectionStatus.CONNECTING)
        if buffer_on_reconnect
            _enqueue_pending(client, frame)
        else
            throw(ConnectionReconnectingError())
        end
    elseif st == ConnectionStatus.DISCONNECTED
        throw(ConnectionClosedError("connection is disconnected"))
    elseif st == ConnectionStatus.DRAINING
        throw(ConnectionDrainingError())
    else
        throw(ConnectionClosedError())
    end
    nothing
end

function _send_subscription_now!(sub::Subscription)
    sid, subject, queue, max_msgs = @lock sub.lock (sub.sid, sub.subject, sub.queue, sub.max_msgs)
    try
        _write_raw(sub.client, _sub_cmd(subject, queue, sid))
        if max_msgs > 0
            _write_raw(sub.client, _unsub_cmd(sid, max_msgs))
        end
        @lock sub.lock sub.server_active = true
        return true
    catch err
        if _recover_after_write_failure!(sub.client, err)
            @lock sub.lock sub.server_active = false
            return false
        end
        rethrow()
    end
end

function _delete_subscription_locked!(client::Client, sid::Int, sub::Subscription)
    if get(client.subscriptions, sid, nothing) === sub
        delete!(client.subscriptions, sid)
        _set_subscription_snapshot_locked!(client, sid, nothing)
    end
    reader = client.reader
    isnothing(reader) || delete!(reader.subject_cache, sid)
    nothing
end

function _close_subscription_channel!(errors::Vector, sub::Subscription)
    @lock sub.lock begin
        if isopen(sub.messages)
            _close_resource!(errors, "close subscription channel $(sub.sid)", sub.messages)
        end
        _notify_subscription_waiters_locked(sub; all=true)
    end
    errors
end

function _close_subscription_locally!(sub::Subscription; throw_errors::Bool=true)
    already_closed = @lock sub.lock begin
        was_closed = sub.closed
        if !was_closed
            sub.closed = true
            sub.server_active = false
            _notify_subscription_waiters_locked(sub; all=true)
        end
        was_closed
    end
    already_closed && return nothing
    @lock sub.client.lock begin
        _delete_subscription_locked!(sub.client, sub.sid, sub)
    end

    errors = Any[]
    _close_subscription_channel!(errors, sub)
    if throw_errors
        _throw_errors(errors)
    else
        _report_cleanup_errors(sub.client, errors)
    end
    nothing
end

_pending_size(data::Vector{UInt8}) = length(data)
_pending_size(data::AbstractString) = ncodeunits(data)
_pending_size(frame::_AbstractPublishFrame) = _serialized_size(frame)

_write_pending(io::IO, data::Union{AbstractString,Vector{UInt8}}) = write(io, data)
_write_pending(io::IO, frame::_AbstractPublishFrame) = _write_pub_frame(io, frame)

_pending_chunk(data::Vector{UInt8}) = copy(data)
function _pending_chunk(data::AbstractString)
    bytes = Vector{UInt8}(undef, ncodeunits(data))
    copyto!(bytes, 1, codeunits(data), 1, length(bytes))
    bytes
end
_pending_chunk(frame::_AbstractPublishFrame) =
    _PendingPublish(_snapshot_publish_frame(frame), _serialized_size(frame))

function _ensure_pending_enqueue_allowed_locked(client::Client)
    st = client.status
    st in (ConnectionStatus.RECONNECTING, ConnectionStatus.CONNECTING) && return nothing
    st == ConnectionStatus.DISCONNECTED && throw(ConnectionClosedError("connection is disconnected"))
    st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
    st == ConnectionStatus.DRAINING && throw(ConnectionDrainingError())
    throw(ConnectionReconnectingError())
end

function _enqueue_pending(client::Client, data)
    @lock client.lock begin
        _ensure_pending_enqueue_allowed_locked(client)
        bytes = _pending_size(data)
        _reserve_pending_bytes_locked!(client, bytes)
        try
            _push_pending_chunk!(client.pending, _pending_chunk(data))
        catch
            client.pending_bytes = max(0, client.pending_bytes - bytes)
            rethrow()
        end
    end
end

function _prepare_publish_frame(subject::AbstractString, data, reply::Union{AbstractString,Nothing}, headers)
    _publish_frame(subject, reply, data, headers)
end

prepare_publish(subject::AbstractString, data=nothing; reply::Union{AbstractString,Nothing}=nothing,
                headers=nothing)::PublishFrame =
    PublishFrame(subject, reply, data, headers)

function _validate_publish_frame_for_client(client::Client, frame::_AbstractPublishFrame)
    total = _pub_payload_size(frame)
    max_payload = _client_max_payload(client)
    headers_supported = _client_headers_supported(client)
    !isempty(frame.headers) && !headers_supported && throw(UnsupportedFeatureError("headers are not supported by the connected server"))
    total > max_payload && throw(MaxPayloadError(max_payload, total))
    nothing
end

_validate_pending_entry_for_client(client::Client, entry::_PendingBytes) = nothing
_validate_pending_entry_for_client(client::Client, entry::_PendingPublish) =
    _validate_publish_frame_for_client(client, entry.frame)

function _validate_pending_replay_for_client(client::Client, entries::AbstractVector{<:_PendingEntry})
    for entry in entries
        _validate_pending_entry_for_client(client, entry)
    end
    nothing
end

function _publish_prepared(client::Client, frame::_AbstractPublishFrame; buffer_on_reconnect::Bool=true,
                           force_flush::Bool=false)
    frame_size = _serialized_size(frame)
    total = _pub_payload_size(frame)
    st = status(client)
    max_payload = _client_max_payload(client)
    headers_supported = _client_headers_supported(client)
    _ensure_usable_status_for_publish(st)
    !isempty(frame.headers) && !headers_supported && throw(UnsupportedFeatureError("headers are not supported by the connected server"))
    total > max_payload && throw(MaxPayloadError(max_payload, total))
    _send_publish(client, frame; buffer_on_reconnect, force_flush, frame_size, st)
    _record_out!(client, length(frame.payload))
    nothing
end

function _publish(client::Client, subject::AbstractString, data=nothing; reply::Union{AbstractString,Nothing}=nothing,
                  headers=nothing, buffer_on_reconnect::Bool=true, force_flush::Bool=false)
    _publish_prepared(client, _prepare_publish_frame(subject, data, reply, headers);
                      buffer_on_reconnect, force_flush)
end

function _publish_frame_unchecked(client::Client, frame::_AbstractPublishFrame; buffer_on_reconnect::Bool=true,
                                  force_flush::Bool=false)
    frame_size = _serialized_size(frame)
    st = status(client)
    _ensure_usable_status_for_publish(st)
    _send_publish(client, frame; buffer_on_reconnect, force_flush, frame_size, st)
    _record_out!(client, length(frame.payload))
    nothing
end

function _publish_unchecked(client::Client, subject::String, payload::AbstractVector{UInt8};
                            buffer_on_reconnect::Bool=true, force_flush::Bool=false)
    _publish_frame_unchecked(client, _publish_frame(subject, nothing, payload, EMPTY_BYTES);
                             buffer_on_reconnect, force_flush)
end

function publish(client::Client, subject::AbstractString, data=nothing; reply::Union{AbstractString,Nothing}=nothing, headers=nothing)
    _publish(client, subject, data; reply, headers)
end

function publish(client::Client, frame::PublishFrame)
    _publish_prepared(client, frame)
end

function _validate_subscription_limits(max_msgs::Int, pending_msgs_limit::Int, pending_bytes_limit::Int)
    _validate_core_max_msgs(max_msgs)
    pending_msgs_limit > 0 || throw(ArgumentError("pending_msgs_limit must be positive"))
    pending_bytes_limit > 0 || throw(ArgumentError("pending_bytes_limit must be positive"))
    nothing
end

function _subscribe(client::Client, subject::AbstractString; queue::Union{AbstractString,Nothing}=nothing, callback=nothing,
                    max_msgs::Int=0, pending_msgs_limit::Int=client.options.sub_pending_msgs_limit,
                    pending_bytes_limit::Int=client.options.sub_pending_bytes_limit,
                    _control_handler::_SubscriptionControlHandler=_NoSubscriptionControlHandler(),
                    require_connected::Bool=false)
    subject = _validate_subject(subject)
    queue = _validate_queue(queue)
    _validate_subscription_limits(max_msgs, pending_msgs_limit, pending_bytes_limit)
    send_now = false
    sub = @lock client.lock begin
        st = client.status
        require_connected && st != ConnectionStatus.CONNECTED && _throw_not_connected_for_request(st)
        st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
        st == ConnectionStatus.DRAINING && throw(ConnectionDrainingError())
        st == ConnectionStatus.DISCONNECTED && throw(ConnectionClosedError("connection is disconnected"))
        client.sid += 1
        sid = client.sid
        ch = MsgQueue{Msg}(pending_msgs_limit)
        sub_lock = ReentrantLock()
        condition = Base.Threads.Condition(sub_lock)
        sub = Subscription(client, sid, subject, queue, callback, sub_lock, ch, condition, _control_handler, pending_msgs_limit, pending_bytes_limit, 0, 0, max_msgs, false, nothing, false, 0)
        client.subscriptions[sid] = sub
        _set_subscription_snapshot_locked!(client, sid, sub)
        send_now = st == ConnectionStatus.CONNECTED
        sub
    end
    if send_now
        try
            _send_subscription_now!(sub)
        catch err
            cleanup_errors = Any[]
            @lock client.lock begin
                _delete_subscription_locked!(client, sub.sid, sub)
            end
            @lock sub.lock begin
                sub.closed = true
                _notify_subscription_waiters_locked(sub; all=true)
            end
            _close_subscription_channel!(cleanup_errors, sub)
            isempty(cleanup_errors) ? rethrow() : throw(Base.CompositeException(vcat(Any[err], cleanup_errors)))
        end
    end
    if !isnothing(callback)
        sub.processor = @async _subscription_processor(sub, callback)
    end
    sub
end

function subscribe(client::Client, subject::AbstractString; kwargs...)
    _subscribe(client, subject; kwargs...)
end

function subscribe(callback, client::Client, subject::AbstractString; kwargs...)
    subscribe(client, subject; callback, kwargs...)
end

function _subscription_processor(sub::Subscription, callback::Callback) where {Callback}
    while true
        msg, control_handler = @lock sub.lock begin
            while !isready(sub.messages) && !sub.closed
                wait(sub.condition)
            end
            !isready(sub.messages) && return nothing
            msg = take!(sub.messages)
            msg_bytes = _msg_pending_bytes(msg)
            sub.pending_bytes = max(0, sub.pending_bytes - msg_bytes)
            sub.processing += 1
            msg, sub.control_handler
        end
        isnothing(msg) && return
        _maybe_reply_to_subscription_flow_control!(sub, control_handler)
        try
            callback(msg)
        catch err
            _report_error(sub.client, err)
        finally
            @lock sub.lock begin
                sub.processing = max(0, sub.processing - 1)
                _notify_subscription_waiters_locked(sub; all=true)
            end
        end
    end
end

_handle_subscription_control(::_NoSubscriptionControlHandler, _sub::Subscription, _msg::Msg)::Bool = false
_record_subscription_data_received!(::_SubscriptionControlHandler) = nothing
_maybe_reply_to_subscription_flow_control!(::Subscription, ::_SubscriptionControlHandler) = nothing

function _request_mux_token(prefix::String, subject::String)::Union{SubString{String},Nothing}
    prefix_len = ncodeunits(prefix)
    subject_len = ncodeunits(subject)
    subject_len > prefix_len + 1 || return nothing
    startswith(subject, prefix) || return nothing
    codeunit(subject, prefix_len + 1) == UInt8('.') || return nothing
    token_start = prefix_len + 2
    token_start <= subject_len || return nothing
    @inbounds for i in token_start:subject_len
        codeunit(subject, i) == UInt8('.') && return nothing
    end
    SubString(subject, token_start, subject_len)
end

function _handle_subscription_control(::_RequestMuxControlHandler, sub::Subscription, msg::Msg)::Bool
    client = sub.client
    waiter = nothing
    mux = @atomic client.request_mux
    drop = false
    notify_err = nothing
    if isnothing(mux) || mux.sub !== sub
        drop = true
    else
        lock(mux.condition)
        try
            (@atomic client.request_mux) === mux || (drop = true)
            if !drop
                token = _request_mux_token(mux.prefix, msg.subject)
                if isnothing(token)
                    drop = true
                else
                    waiter = pop!(mux.waiters, token, nothing)
                    drop = isnothing(waiter)
                    if !drop
                        try
                            _resolve_request_waiter_locked!(waiter, msg, mux.condition)
                        catch err
                            waiter = nothing
                            notify_err = err
                        end
                    end
                end
            end
        finally
            unlock(mux.condition)
        end
    end
    if drop
        _record_drop!(client)
    end
    isnothing(notify_err) || _report_error(client, CleanupError("deliver request reply $(msg.subject)", notify_err))
    true
end

function _dispatch_msg(client::Client, msg::Msg)
    msg_bytes = _msg_pending_bytes(msg)
    control_handler = _NoSubscriptionControlHandler()
    sub = _lookup_subscription(client, msg.sid)
    if isnothing(sub)
        _record_drop!(client)
        return
    end
    closed = @lock sub.lock begin
        if sub.closed
            true
        else
            control_handler = sub.control_handler
            false
        end
    end
    if closed
        _record_drop!(client)
        return
    end

    if _handle_subscription_control(control_handler, sub, msg)
        _record_in!(client, length(msg.data))
        return
    end

    should_close = false
    report_slow = false
    accepted = @lock sub.lock begin
        if sub.closed
            report_slow = false
            false
        elseif sub.pending_bytes + msg_bytes > sub.pending_bytes_limit || Base.n_avail(sub.messages) >= sub.pending_msgs_limit
            report_slow = true
            false
        else
            sub.received += 1
            sub.pending_bytes += msg_bytes
            should_close = sub.max_msgs > 0 && sub.received >= sub.max_msgs
            put!(sub.messages, msg)
            _notify_subscription_waiters_locked(sub)
            true
        end
    end
    if !accepted
        _record_drop!(client)
        if report_slow
            _report_error(client, SlowConsumerError(msg.subject, msg.sid, "subscription pending limits exceeded"))
        end
        return
    end
    _record_in!(client, length(msg.data))
    _record_subscription_data_received!(control_handler)
    if should_close
        _close_subscription_locally!(sub; throw_errors=false)
    end
end

function _take_subscription_msg_if_ready!(sub::Subscription)::Union{Msg,Nothing}
    msg = nothing
    control_handler = _NoSubscriptionControlHandler()
    @lock sub.lock begin
        isready(sub.messages) || return nothing
        msg = take!(sub.messages)
        msg_bytes = _msg_pending_bytes(msg)
        sub.pending_bytes = max(0, sub.pending_bytes - msg_bytes)
        control_handler = sub.control_handler
    end
    isnothing(msg) && return nothing
    _maybe_reply_to_subscription_flow_control!(sub, control_handler)
    _notify_subscription_waiters!(sub; all=true)
    msg
end

function _ensure_sync_subscription(sub::Subscription)
    sub.has_callback && throw(ArgumentError("next requires a subscription without a callback"))
    nothing
end

function next(sub::Subscription; timeout::Real=1.0)
    _ensure_sync_subscription(sub)
    timeout = _positive_timeout_seconds("timeout", timeout)
    deadline = time() + timeout
    while true
        msg = _take_subscription_msg_if_ready!(sub)
        !isnothing(msg) && return msg

        wait_result = @lock sub.lock begin
            if sub.closed && !isready(sub.messages)
                :closed
            else
                ready = _wait_until_condition_locked(sub.condition, _remaining_timeout(deadline)) do
                    isready(sub.messages) || sub.closed
                end
                ready ? :retry : :timeout
            end
        end
        wait_result === :closed && throw(ConnectionClosedError("subscription is closed"))
        wait_result === :timeout && throw(TimeoutError("next message timed out"))
    end
end

function _unsubscribe_target(received::Int, additional::Int)
    received > typemax(Int) - additional &&
        throw(ArgumentError("max_msgs is too large for subscription message count"))
    received + additional
end

function unsubscribe(sub::Subscription; max_msgs::Int=0)
    _validate_core_max_msgs(max_msgs)
    st = status(sub.client)
    closed, active, sid, target, previous_max = @lock sub.lock begin
        if sub.closed
            (true, false, sub.sid, 0, sub.max_msgs)
        else
            target = 0
            previous_max = sub.max_msgs
            if max_msgs > 0
                st in (ConnectionStatus.CLOSED, ConnectionStatus.DISCONNECTED) &&
                    throw(ConnectionClosedError("connection is disconnected"))
                st == ConnectionStatus.DRAINING && throw(ConnectionDrainingError())
                target = _unsubscribe_target(sub.received, max_msgs)
                sub.max_msgs = target
            end
            (false, sub.server_active, sub.sid, target, previous_max)
        end
    end
    closed && return nothing
    if st == ConnectionStatus.CONNECTED && active
        try
            _write_raw(sub.client, _unsub_cmd(sid, target))
        catch err
            if !_recover_after_write_failure!(sub.client, err)
                if max_msgs > 0
                    @lock sub.lock begin
                        if !sub.closed && sub.max_msgs == target
                            sub.max_msgs = previous_max
                        end
                    end
                end
                rethrow()
            end
            @lock sub.lock begin
                if !sub.closed
                    sub.server_active = false
                end
            end
        end
    end
    if max_msgs == 0
        _close_subscription_locally!(sub)
    end
    nothing
end

close(sub::Subscription) = unsubscribe(sub)

_drain_deadline(timeout::Real)::Float64 = time() + _positive_timeout_seconds("timeout", timeout)
_drain_timed_out(err)::Bool =
    err isa TimeoutError ||
    (err isa CleanupError && _drain_timed_out(err.cause)) ||
    (err isa Base.CompositeException && any(_drain_timed_out, err.exceptions))
_drain_timed_out(errors::Vector)::Bool = any(_drain_timed_out, errors)

function _drain(sub::Subscription, deadline::Float64)
    closed, active, sid = @lock sub.lock (sub.closed, sub.server_active, sub.sid)
    closed && return nothing
    status(sub.client) in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING) || throw(ConnectionReconnectingError())
    if active
        try
            _write_raw(sub.client, _unsub_cmd(sid); deadline=deadline, write_mode=_RAW_WRITE_DRAIN)
        catch err
            _drain_timed_out(err) && rethrow()
            _recover_after_write_failure!(sub.client, err) || rethrow()
            throw(ConnectionReconnectingError())
        end
    end
    _flush(sub.client; timeout=_remaining_timeout(deadline), deadline=deadline)
    ready = @lock sub.lock begin
        _wait_until_condition_locked(sub.condition, _remaining_timeout(deadline)) do
            !isready(sub.messages) && sub.processing == 0
        end
    end
    ready || throw(TimeoutError("subscription drain timed out"))
    @lock sub.lock begin
        sub.closed = true
        sub.server_active = false
        _notify_subscription_waiters_locked(sub; all=true)
    end
    @lock sub.client.lock begin
        _delete_subscription_locked!(sub.client, sid, sub)
    end
    errors = Any[]
    _close_subscription_channel!(errors, sub)
    _throw_errors(errors)
    nothing
end

function drain(sub::Subscription; timeout::Real=sub.client.options.drain_timeout)
    _drain(sub, _drain_deadline(timeout))
end

function _remove_pong_waiter_locked!(client::Client, waiter::PongWaiter)
    _filter_pong_waiter_queue!(w -> w !== waiter, client.pongs)
    nothing
end

function _wait_pong_waiter!(waiter::PongWaiter, timeout::Real)
    lock(waiter.condition)
    try
        ready = _wait_until_condition_locked(waiter.condition, timeout) do
            waiter.ready
        end
        if !ready
            waiter.active = false
            return :timed_out
        end
        waiter.value
    finally
        unlock(waiter.condition)
    end
end

function _flush(client::Client; timeout::Real=10.0, deadline=nothing)
    wait_timeout = isnothing(deadline) ? _positive_timeout_seconds("timeout", timeout) :
                   min(Float64(timeout), _remaining_timeout(deadline))
    st = status(client)
    st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
    st in (ConnectionStatus.RECONNECTING, ConnectionStatus.CONNECTING, ConnectionStatus.DISCONNECTED) && throw(ConnectionReconnectingError())
    waiter = PongWaiter(Base.Threads.Condition(client.lock))
    @lock client.lock push!(client.pongs, waiter)
    try
        _send_raw(client, "PING$CRLF"; force_flush=true, deadline)
    catch err
        @lock client.lock _remove_pong_waiter_locked!(client, waiter)
        rethrow()
    end
    result = _wait_pong_waiter!(waiter, wait_timeout)
    if result == :timed_out
        # Keep a bounded tombstone so a late PONG is consumed by its original
        # flush and cannot make a later flush appear complete.
        @lock client.lock begin
            _trim_stale_pong_waiters_locked!(client)
        end
        throw(TimeoutError("flush timed out"))
    end
    result || throw(status(client) == ConnectionStatus.CLOSED ? ConnectionClosedError() : ConnectionReconnectingError())
    nothing
end

flush(client::Client; timeout::Real=10.0) = _flush(client; timeout)

ping(client::Client; timeout::Real=10.0) = flush(client; timeout)

function drain(client::Client; timeout::Real=client.options.drain_timeout)
    deadline = _drain_deadline(timeout)
    notify_subs = Subscription[]
    @lock client.lock begin
        client.status == ConnectionStatus.CLOSED && return nothing
        client.status == ConnectionStatus.CONNECTED || throw(ConnectionReconnectingError())
        _store_status_locked!(client, ConnectionStatus.DRAINING)
        notify_subs = collect(values(client.subscriptions))
    end
    for sub in notify_subs
        _notify_subscription_waiters!(sub; all=true)
    end
    errors = Any[]
    subs = @lock client.lock collect(values(client.subscriptions))
    for sub in subs
        try
            _drain(sub, deadline)
        catch err
            push!(errors, err)
            _report_error(client, err)
            _drain_timed_out(err) && break
        end
    end
    if !_drain_timed_out(errors)
        try
            _flush(client; timeout=_remaining_timeout(deadline), deadline=deadline)
        catch err
            push!(errors, err)
            _report_error(client, err)
        end
    end
    try
        _close_client!(client; throw_errors=true, callback_timeout=_remaining_timeout(deadline),
                       deadline=deadline)
    catch err
        push!(errors, err)
    end
    _throw_errors(errors)
    nothing
end

function _client_task_close_timeout(client::Client)::Float64
    min(5.0, max(0.5, client.options.connect_timeout + 0.2))
end

function _close_client!(client::Client; throw_errors::Bool=false, callback_timeout=nothing,
                        deadline=nothing)
    callback_wait = isnothing(callback_timeout) ? client.options.close_callback_timeout :
                    _connect_option_nonnegative_float("callback_timeout", callback_timeout)
    already = false
    subs = Subscription[]
    @lock client.lock begin
        already = client.status == ConnectionStatus.CLOSED
        if !already
            _store_status_locked!(client, ConnectionStatus.CLOSED)
            _bump_generation_locked!(client)
            subs = collect(values(client.subscriptions))
            empty!(client.subscriptions)
            @atomic client.subscription_snapshot = Vector{Union{Subscription{typeof(client)},Nothing}}()
            reader = client.reader
            isnothing(reader) || empty!(reader.subject_cache)
        else
            _clear_pending_buffer_locked!(client)
        end
    end
    already && return nothing
    for sub in subs
        @lock sub.lock begin
            sub.closed = true
            sub.server_active = false
            _notify_subscription_waiters_locked(sub; all=true)
        end
    end
    errors = Any[]
    try
        _wake_flusher(client)
    catch err
        push!(errors, CleanupError("wake flusher", err))
    end
    append!(errors, _notify_pong_waiters!(client, false))
    append!(errors, _notify_request_waiters!(client, ConnectionClosedError(); clear_mux=true))
    for sub in subs
        _close_subscription_channel!(errors, sub)
    end
    try
        append!(errors, _close_transport(_take_transport!(client; deadline)...))
    catch err
        push!(errors, err)
        if _drain_timed_out(err)
            _abort_transport_for_blocked_write_lock!(client)
        end
    end
    _clear_pending_buffer!(client)
    append!(errors, _stop_client_tasks!(client; timeout=_client_task_close_timeout(client), deadline=deadline))
    for sub in subs
        _wait_task!(errors, "stop subscription processor $(sub.sid)", sub.processor;
                    timeout=callback_wait, deadline=deadline)
    end
    try
        client.options.closed_cb()
    catch err
        callback_err = CleanupError("closed callback", err)
        push!(errors, callback_err)
    end
    if throw_errors
        _throw_errors(errors)
    else
        _report_cleanup_errors(client, errors)
    end
    nothing
end

close(client::Client; throw_errors::Bool=false, callback_timeout=nothing) =
    _close_client!(client; throw_errors=throw_errors, callback_timeout=callback_timeout)

function new_inbox(client::Client; prefix::AbstractString=client.options.inbox_prefix)
    suffix = @lock client.lock randstring(client.rng, NUID_ALPHABET, 22)
    "$prefix.$suffix"
end

function _request_mux_active_locked(client::Client)
    mux = @atomic client.request_mux
    if isnothing(mux)
        return nothing
    end
    active = @lock mux.sub.lock !mux.sub.closed && mux.sub.server_active
    active ? mux : nothing
end

function _close_inactive_request_mux_subscription!(client::Client, sub::Subscription)
    errors = Any[]
    @lock client.lock begin
        _delete_subscription_locked!(client, sub.sid, sub)
    end
    @lock sub.lock begin
        sub.closed = true
        sub.server_active = false
        _notify_subscription_waiters_locked(sub; all=true)
    end
    _close_subscription_channel!(errors, sub)
    _report_cleanup_errors(client, errors)
    nothing
end

function _ensure_request_mux(client::Client)
    _ensure_connected_for_request(client)
    mux = @lock client.lock _request_mux_active_locked(client)
    isnothing(mux) || return mux

    @lock client.request_mux_lock begin
        _ensure_connected_for_request(client)
        mux = @lock client.lock _request_mux_active_locked(client)
        isnothing(mux) || return mux

        prefix = new_inbox(client)
        sub = _subscribe(client, "$prefix.*"; _control_handler=_RequestMuxControlHandler(), require_connected=true)
        active = status(client) == ConnectionStatus.CONNECTED && (@lock sub.lock !sub.closed && sub.server_active)
        if !active
            _close_inactive_request_mux_subscription!(client, sub)
            throw(ConnectionReconnectingError())
        end

        mux_lock = ReentrantLock()
        mux = RequestMux(prefix, sub, Dict{String,RequestWaiter{typeof(client)}}(),
                         Base.Threads.Condition(mux_lock), nothing)
        assigned = @lock client.lock begin
            sub_active = @lock sub.lock !sub.closed && sub.server_active
            if client.status == ConnectionStatus.CONNECTED && sub_active
                @atomic client.request_mux = mux
                true
            else
                false
            end
        end
        if !assigned
            _close_inactive_request_mux_subscription!(client, sub)
            throw(ConnectionReconnectingError())
        end
        mux
    end
end

function _request_timeout_loop(client::C, mux::RequestMux{C}) where {C<:Client}
    lock(mux.condition)
    try
        while true
            (@atomic client.request_mux) === mux || (mux.timeout_task = nothing; return nothing)
            isempty(mux.waiters) && (mux.timeout_task = nothing; return nothing)
            now = time()
            nearest = Inf
            expired = nothing
            for (token, waiter) in mux.waiters
                if !waiter.active
                    isnothing(expired) && (expired = String[])
                    push!(expired, token)
                    continue
                end
                if waiter.deadline <= now
                    isnothing(expired) && (expired = String[])
                    push!(expired, token)
                else
                    nearest = min(nearest, waiter.deadline)
                end
            end
            if !isnothing(expired)
                for token in expired
                    waiter = pop!(mux.waiters, token, nothing)
                    if !isnothing(waiter) && waiter.active
                        _resolve_request_waiter_locked!(waiter, TimeoutError("request timed out"), mux.condition)
                    end
                end
            end
            isempty(mux.waiters) && (mux.timeout_task = nothing; return nothing)
            delay = nearest - now
            if !isfinite(delay)
                wait(mux.condition)
            elseif delay <= 0
                continue
            else
                timer = Timer(delay) do _
                    lock(mux.condition)
                    try
                        notify(mux.condition; all=true)
                    finally
                        unlock(mux.condition)
                    end
                end
                try
                    wait(mux.condition)
                finally
                    close(timer)
                end
            end
        end
    finally
        unlock(mux.condition)
    end
end

function _ensure_request_timeout_task_locked!(client::C, mux::RequestMux{C}) where {C<:Client}
    task = mux.timeout_task
    if isnothing(task) || istaskdone(task)
        mux.timeout_task = @async _request_timeout_loop(client, mux)
    end
    nothing
end

function _register_request_waiter!(client::C, mux::RequestMux{C}, timeout::Real) where {C<:Client}
    timeout = _positive_timeout_seconds("timeout", timeout)
    st = status(client)
    st == ConnectionStatus.CONNECTED || _throw_not_connected_for_request(st)
    sub_active = @lock mux.sub.lock !mux.sub.closed && mux.sub.server_active
    (@atomic client.request_mux) === mux && sub_active || throw(ConnectionReconnectingError())
    lock(mux.condition)
    try
        sub_active = @lock mux.sub.lock !mux.sub.closed && mux.sub.server_active
        (@atomic client.request_mux) === mux && sub_active || throw(ConnectionReconnectingError())
        deadline = time() + Float64(timeout)
        while true
            token = randstring(NUID_ALPHABET, 22)
            if !haskey(mux.waiters, token)
                waiter = RequestWaiter{C}(deadline)
                mux.waiters[token] = waiter
                _ensure_request_timeout_task_locked!(client, mux)
                notify(mux.condition; all=true)
                return token, waiter
            end
        end
    finally
        unlock(mux.condition)
    end
end

function _remove_request_waiter!(client::C, mux::RequestMux{C}, token::String,
                                 waiter::RequestWaiter{C}) where {C<:Client}
    lock(mux.condition)
    try
        waiter.active = false
        if (@atomic client.request_mux) === mux && get(mux.waiters, token, nothing) === waiter
            delete!(mux.waiters, token)
        end
        notify(mux.condition; all=true)
    finally
        unlock(mux.condition)
    end
    nothing
end

function _wait_request_reply(mux::RequestMux, waiter::RequestWaiter, timeout::Real)::Msg
    deadline = waiter.deadline
    lock(mux.condition)
    value = try
        while !waiter.ready
            remaining = deadline - time()
            if remaining <= 0
                waiter.active = false
                throw(TimeoutError("request timed out"))
            end
            wait(mux.condition)
        end
        waiter.value
    finally
        unlock(mux.condition)
    end
    value isa Exception && throw(value)
    value::Msg
end

function _request_raw(client::Client, subject::AbstractString, data=nothing; timeout::Real=1.0, headers=nothing)
    timeout = _positive_timeout_seconds("timeout", timeout)
    request_frame = _prepare_publish_frame(subject, data, nothing, headers)
    _ensure_connected_for_request(client)
    _validate_publish_frame_for_client(client, request_frame)
    mux = _ensure_request_mux(client)
    token, waiter = _register_request_waiter!(client, mux, timeout)
    reply = "$(mux.prefix).$token"
    try
        _validate_publish_subject(reply)
        frame = _PublishFrame(request_frame.subject, reply, request_frame.payload, request_frame.headers)
        _publish_frame_unchecked(client, frame; buffer_on_reconnect=false, force_flush=true)
        return _wait_request_reply(mux, waiter, timeout)
    finally
        _remove_request_waiter!(client, mux, token, waiter)
    end
end

function request(client::Client, subject::AbstractString, data=nothing; timeout::Real=1.0, headers=nothing)
    msg = _request_raw(client, subject, data; timeout, headers)
    code = _status_header(msg)
    if code == 503
        throw(NoRespondersError(String(subject)))
    elseif !isnothing(code) && code >= 400
        throw(ProtocolError("request failed with status $code $(_status_description(msg))"))
    end
    msg
end
