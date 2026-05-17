function _ensure_usable_status_for_publish(st::ConnectionStatus.T)
    st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
    st == ConnectionStatus.DRAINING && throw(ConnectionDrainingError())
    st == ConnectionStatus.DISCONNECTED && throw(ConnectionClosedError("connection is disconnected"))
    nothing
end

function _send_raw(client::Client, data::Union{AbstractString,Vector{UInt8}}; buffer_on_reconnect::Bool=false,
                   force_flush::Bool=false, deadline=nothing,
                   cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    st = status(client)
    can_buffer_reconnect = buffer_on_reconnect && _reconnect_buffer_enabled(client)
    if st in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING)
        try
            _write_raw(client, data; force_flush, deadline, write_mode=_RAW_WRITE_DRAIN,
                       cancel_token)
        catch err
            if st == ConnectionStatus.CONNECTED && _recover_after_write_failure!(client, err)
                if can_buffer_reconnect
                    _enqueue_pending(client, data)
                    return nothing
                end
                throw(ConnectionReconnectingError())
            end
            rethrow()
        end
    elseif st in (ConnectionStatus.RECONNECTING, ConnectionStatus.CONNECTING)
        if can_buffer_reconnect
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
    replayable_bytes = io.replayable_bytes
    if replayable
        _reserve_pending_bytes!(client, frame_size)
        try
            _write_pub_frame(io, frame)
            written = position(io.buffer) - start + 1
            written == frame_size || throw(AssertionError("buffered publish size mismatch"))
            entry = _ReplayableEntry(start, frame_size, _pub_payload_size(frame), length(frame.headers))
            push!(io.replayable_entries, entry)
            io.replayable_bytes += frame_size
        catch
            truncate(io.buffer, start - 1)
            seekend(io.buffer)
            resize!(io.replayable_entries, entry_count)
            io.replayable_bytes = replayable_bytes
            _release_pending_bytes!(client, frame_size)
            rethrow()
        end
    else
        try
            _write_pub_frame(io, frame)
        catch
            truncate(io.buffer, start - 1)
            seekend(io.buffer)
            rethrow()
        end
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
        st = status(client)
        io = @atomic client.write_io
        if !(st == ConnectionStatus.CONNECTED || st == ConnectionStatus.DRAINING)
            st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
            st == ConnectionStatus.DISCONNECTED && throw(ConnectionClosedError("connection is disconnected"))
            throw(ConnectionReconnectingError())
        end
        captured = _write_publish_to_active_io(client, io, frame, force_flush, replayable, frame_size)
    end
    captured
end

@noinline function _write_publish_to_active_io(client::Client, io::Union{Nothing,DefaultWriteTransportIO},
                                               frame::_AbstractPublishFrame, force_flush::Bool,
                                               replayable::Bool, frame_size::Int)::Bool
    io === nothing && throw(ConnectionClosedError("connection transport is closed"))

    # connect() clients keep a union-typed field so reconnect can swap plain,
    # TLS, and buffered transports. Split that small default union here so the
    # hot frame writer below is compiled for concrete IO types.
    if io isa Sockets.TCPSocket
        return _write_publish_to_io(client, io, frame, force_flush, replayable, frame_size)
    elseif io isa MbedTLS.SSLContext
        return _write_publish_to_io(client, io, frame, force_flush, replayable, frame_size)
    elseif io isa BufferedWriteIO{Sockets.TCPSocket}
        return _write_publish_to_io(client, io, frame, force_flush, replayable, frame_size)
    else
        return _write_publish_to_io(client, io::BufferedWriteIO{MbedTLS.SSLContext},
                                    frame, force_flush, replayable, frame_size)
    end
end

function _write_publish_to_active_io(client::Client, io, frame::_AbstractPublishFrame,
                                     force_flush::Bool, replayable::Bool, frame_size::Int)::Bool
    io === nothing && throw(ConnectionClosedError("connection transport is closed"))
    _write_publish_to_io(client, io, frame, force_flush, replayable, frame_size)
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
    can_buffer_reconnect = buffer_on_reconnect && _reconnect_buffer_enabled(client)
    if st in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING)
        replayable_captured = false
        try
            replayable_captured = _write_publish(client, frame; force_flush,
                                                 replayable=can_buffer_reconnect, frame_size)
        catch err
            err isa OutboundBufferLimitError && rethrow()
            if st == ConnectionStatus.CONNECTED && _recover_after_write_failure!(client, err)
                if can_buffer_reconnect
                    replayable_captured || _enqueue_pending(client, frame)
                    return nothing
                end
                throw(ConnectionReconnectingError())
            end
            rethrow()
        end
    elseif st in (ConnectionStatus.RECONNECTING, ConnectionStatus.CONNECTING)
        if can_buffer_reconnect
            _enqueue_pending(client, frame)
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

_pending_chunk(data::Vector{UInt8}) = copy(data)
function _pending_chunk(data::AbstractString)
    bytes = Vector{UInt8}(undef, ncodeunits(data))
    copyto!(bytes, 1, codeunits(data), 1, length(bytes))
    bytes
end
_pending_chunk(frame::_AbstractPublishFrame) =
    _pending_publish_entry(frame, _serialized_size(frame))

function _ensure_pending_enqueue_allowed_locked(client::Client)
    st = client.status
    if st in (ConnectionStatus.RECONNECTING, ConnectionStatus.CONNECTING)
        _reconnect_buffer_enabled(client) || throw(ConnectionReconnectingError())
        return nothing
    end
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
            _release_pending_bytes!(client, bytes)
            rethrow()
        end
    end
end

prepare_publish(subject::AbstractString, data=nothing; reply::Union{AbstractString,Nothing}=nothing,
                headers=nothing)::PublishFrame =
    PublishFrame(subject, reply, data, headers)

function _validate_publish_frame_for_client(client::Client, frame::_AbstractPublishFrame,
                                            total::Int=_pub_payload_size(frame))
    max_payload = _client_max_payload(client)
    headers_supported = _client_headers_supported(client)
    !isempty(frame.headers) && !headers_supported && throw(UnsupportedFeatureError("headers are not supported by the connected server"))
    total > max_payload && throw(MaxPayloadError(max_payload, total))
    nothing
end

function _validate_pending_entry_for_client(client::Client, entry::_PendingEntry)
    entry.is_publish || return nothing
    max_payload = _client_max_payload(client)
    headers_supported = _client_headers_supported(client)
    entry.header_bytes > 0 && !headers_supported &&
        throw(UnsupportedFeatureError("headers are not supported by the connected server"))
    entry.payload_size > max_payload && throw(MaxPayloadError(max_payload, entry.payload_size))
    nothing
end

function _validate_pending_replay_for_client(client::Client, entries::AbstractVector{<:_PendingEntry})
    for entry in entries
        _validate_pending_entry_for_client(client, entry)
    end
    nothing
end

function _publish_prepared(client::Client, frame::_AbstractPublishFrame; buffer_on_reconnect::Bool=true,
                           force_flush::Bool=false, cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    payload_size = _pub_payload_size(frame)
    frame_size = _serialized_size(frame)
    st = status(client)
    _ensure_usable_status_for_publish(st)
    _validate_publish_frame_for_client(client, frame, payload_size)
    _send_publish(client, frame; buffer_on_reconnect, force_flush, frame_size, st)
    _record_out!(client, payload_size)
    nothing
end

function _publish(client::Client, subject::AbstractString, data=nothing; reply::Union{AbstractString,Nothing}=nothing,
                  headers=nothing, buffer_on_reconnect::Bool=true, force_flush::Bool=false,
                  cancel_token::MaybeCancellationToken=nothing)
    _publish_prepared(client, _publish_frame(subject, reply, data, headers);
                      buffer_on_reconnect, force_flush, cancel_token)
end

function _publish_frame_unchecked(client::Client, frame::_AbstractPublishFrame; buffer_on_reconnect::Bool=true,
                                  force_flush::Bool=false, cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    frame_size = _serialized_size(frame)
    st = status(client)
    _ensure_usable_status_for_publish(st)
    _send_publish(client, frame; buffer_on_reconnect, force_flush, frame_size, st)
    _record_out!(client, _pub_payload_size(frame))
    nothing
end

function _publish_unchecked(client::Client, subject::String, payload::AbstractVector{UInt8};
                            buffer_on_reconnect::Bool=true, force_flush::Bool=false,
                            cancel_token::MaybeCancellationToken=nothing)
    _publish_frame_unchecked(client, _publish_frame(subject, nothing, payload, EMPTY_BYTES);
                             buffer_on_reconnect, force_flush, cancel_token)
end

function publish(client::Client, subject::AbstractString, data=nothing; reply::Union{AbstractString,Nothing}=nothing,
                 headers=nothing, cancel_token::MaybeCancellationToken=nothing)
    _publish(client, subject, data; reply, headers, cancel_token)
end

function publish(client::Client, frame::PublishFrame; cancel_token::MaybeCancellationToken=nothing)
    _publish_prepared(client, frame; cancel_token)
end

function _validate_subscription_limits(max_msgs, pending_msgs_limit, pending_bytes_limit)
    max_msgs = _validate_core_max_msgs(max_msgs)
    pending_msgs_limit = _positive_integer_option("pending_msgs_limit", pending_msgs_limit)
    pending_bytes_limit = _positive_integer_option("pending_bytes_limit", pending_bytes_limit)
    max_msgs, pending_msgs_limit, pending_bytes_limit
end

struct _SubscriptionProcessor{S<:Subscription,F}
    sub::S
    callback::F
end

(processor::_SubscriptionProcessor)() =
    _subscription_processor(processor.sub, processor.callback)

function _start_subscription_processor!(sub::Subscription, callback::Callback) where {Callback}
    processor = _SubscriptionProcessor(sub, callback)
    sub.processor = @async processor()
    nothing
end

function _subscribe(client::Client, subject::AbstractString; queue::Union{AbstractString,Nothing}=nothing, callback=nothing,
                    max_msgs=0, pending_msgs_limit=client.options.sub_pending_msgs_limit,
                    pending_bytes_limit=client.options.sub_pending_bytes_limit,
                    _control_handler::_SubscriptionControlHandler=_NoSubscriptionControlHandler(),
                    require_connected::Bool=false)
    subject = _validate_subject(subject)
    queue = _validate_queue(queue)
    max_msgs, pending_msgs_limit, pending_bytes_limit =
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
        _start_subscription_processor!(sub, callback)
    end
    sub
end

function subscribe(client::Client, subject::AbstractString; cancel_token::MaybeCancellationToken=nothing,
                   kwargs...)
    _throw_if_cancelled(cancel_token)
    _subscribe(client, subject; kwargs...)
end

function subscribe(callback, client::Client, subject::AbstractString;
                   cancel_token::MaybeCancellationToken=nothing, kwargs...)
    subscribe(client, subject; callback, cancel_token, kwargs...)
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
_record_subscription_data_received!(::_SubscriptionControlHandler, ::Msg) = nothing
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
        _record_in!(client, msg_bytes)
        return
    end
    if _handle_ordered_push_data!(control_handler, client, msg)
        _record_drop!(client)
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
    _record_in!(client, msg_bytes)
    _record_subscription_data_received!(control_handler, msg)
    if should_close
        _close_subscription_locally!(sub; throw_errors=false)
    end
end

function _take_subscription_msg_ready!(sub::Subscription)::Tuple{Bool,Msg}
    msg = EMPTY_MSG
    control_handler = _NoSubscriptionControlHandler()
    ready = @lock sub.lock begin
        if !isready(sub.messages)
            false
        else
            msg = take!(sub.messages)
            msg_bytes = _msg_pending_bytes(msg)
            sub.pending_bytes = max(0, sub.pending_bytes - msg_bytes)
            control_handler = sub.control_handler
            true
        end
    end
    ready || return (false, EMPTY_MSG)
    _maybe_reply_to_subscription_flow_control!(sub, control_handler)
    _notify_subscription_waiters!(sub; all=true)
    return (true, msg)
end

function _ensure_sync_subscription(sub::Subscription)
    sub.has_callback && throw(ArgumentError("next requires a subscription without a callback"))
    nothing
end

function next(sub::Subscription; timeout::Real=1.0, cancel_token::MaybeCancellationToken=nothing)
    _ensure_sync_subscription(sub)
    _throw_if_cancelled(cancel_token)
    timeout = _positive_timeout_seconds("timeout", timeout)
    deadline = time() + timeout
    while true
        ready, msg = _take_subscription_msg_ready!(sub)
        ready && return msg

        wait_result = @lock sub.lock begin
            if sub.closed && !isready(sub.messages)
                :closed
            else
                ready = _wait_until_condition_locked(sub.condition, _remaining_timeout(deadline);
                                                     cancel_token) do
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

function unsubscribe(sub::Subscription; max_msgs=0, cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    max_msgs = _validate_core_max_msgs(max_msgs)
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

close(sub::Subscription; cancel_token::MaybeCancellationToken=nothing) =
    unsubscribe(sub; cancel_token)

_drain_deadline(timeout::Real)::Float64 = time() + _positive_timeout_seconds("timeout", timeout)
_drain_timed_out(err)::Bool =
    err isa TimeoutError ||
    (err isa CleanupError && _drain_timed_out(err.cause)) ||
    (err isa Base.CompositeException && any(_drain_timed_out, err.exceptions))
_drain_timed_out(errors::Vector)::Bool = any(_drain_timed_out, errors)

function _drain(sub::Subscription, deadline::Float64; cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    closed, active, sid = @lock sub.lock (sub.closed, sub.server_active, sub.sid)
    closed && return nothing
    status(sub.client) in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING) || throw(ConnectionReconnectingError())
    if active
        try
            _write_raw(sub.client, _unsub_cmd(sid); deadline=deadline, write_mode=_RAW_WRITE_DRAIN,
                       cancel_token)
        catch err
            _drain_timed_out(err) && rethrow()
            _recover_after_write_failure!(sub.client, err) || rethrow()
            throw(ConnectionReconnectingError())
        end
    end
    _flush(sub.client; timeout=_remaining_timeout(deadline), deadline=deadline, cancel_token)
    ready = @lock sub.lock begin
        _wait_until_condition_locked(sub.condition, _remaining_timeout(deadline); cancel_token) do
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

function drain(sub::Subscription; timeout::Real=sub.client.options.drain_timeout,
               cancel_token::MaybeCancellationToken=nothing)
    _drain(sub, _drain_deadline(timeout); cancel_token)
end

function _remove_pong_waiter_locked!(client::Client, waiter::PongWaiter)
    _filter_pong_waiter_queue!(w -> w !== waiter, client.pongs)
    nothing
end

function _wait_pong_waiter!(waiter::PongWaiter, timeout::Real;
                            cancel_token::MaybeCancellationToken=nothing)
    lock(waiter.condition)
    try
        ready = try
            _wait_until_condition_locked(waiter.condition, timeout; cancel_token) do
                waiter.ready
            end
        catch err
            if err isa CancelledError
                waiter.active = false
            end
            rethrow()
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

function _throw_not_flushable_status(st::ConnectionStatus.T)
    st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
    st == ConnectionStatus.DISCONNECTED && throw(ConnectionClosedError("connection is disconnected"))
    throw(ConnectionReconnectingError())
end

function _flush(client::Client; timeout::Real=10.0, deadline=nothing,
                cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    wait_timeout = isnothing(deadline) ? _positive_timeout_seconds("timeout", timeout) :
                   min(Float64(timeout), _remaining_timeout(deadline))
    st = status(client)
    st in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING) || _throw_not_flushable_status(st)
    waiter = PongWaiter(Base.Threads.Condition(client.lock))
    @lock client.lock push!(client.pongs, waiter)
    try
        _send_raw(client, "PING$CRLF"; force_flush=true, deadline, cancel_token)
    catch err
        @lock client.lock _remove_pong_waiter_locked!(client, waiter)
        rethrow()
    end
    result = try
        _wait_pong_waiter!(waiter, wait_timeout; cancel_token)
    catch err
        if err isa CancelledError
            @lock client.lock begin
                _trim_stale_pong_waiters_locked!(client)
            end
        end
        rethrow()
    end
    if result == :timed_out
        # Keep a bounded tombstone so a late PONG is consumed by its original
        # flush and cannot make a later flush appear complete.
        @lock client.lock begin
            _trim_stale_pong_waiters_locked!(client)
        end
        throw(TimeoutError("flush timed out"))
    end
    result || _throw_not_flushable_status(status(client))
    nothing
end

flush(client::Client; timeout::Real=10.0, cancel_token::MaybeCancellationToken=nothing) =
    _flush(client; timeout, cancel_token)

ping(client::Client; timeout::Real=10.0, cancel_token::MaybeCancellationToken=nothing) =
    flush(client; timeout, cancel_token)

function drain(client::Client; timeout::Real=client.options.drain_timeout,
               cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
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
            _drain(sub, deadline; cancel_token)
        catch err
            push!(errors, err)
            _report_error(client, err)
            _drain_timed_out(err) && break
        end
    end
    if !_drain_timed_out(errors)
        try
            _flush(client; timeout=_remaining_timeout(deadline), deadline=deadline, cancel_token)
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
                        deadline=nothing, cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
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
    _clear_js_async_publish_pending!(client, ConnectionClosedError())
    for sub in subs
        @lock sub.lock begin
            sub.closed = true
            sub.server_active = false
            _notify_subscription_waiters_locked(sub; all=true)
        end
    end
    errors = Any[]
    try
        _signal_flusher(client)
    catch err
        push!(errors, CleanupError("signal flusher", err))
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
    event = _connection_event(client, ConnectionEventKind.CLOSED)
    try
        client.options.event_cb(event)
    catch err
        callback_err = CleanupError("closed event callback", err)
        push!(errors, callback_err)
    end
    if throw_errors
        _throw_errors(errors)
    else
        _report_cleanup_errors(client, errors)
    end
    nothing
end

close(client::Client; throw_errors::Bool=false, callback_timeout=nothing,
      cancel_token::MaybeCancellationToken=nothing) =
    _close_client!(client; throw_errors=throw_errors, callback_timeout=callback_timeout,
                   cancel_token=cancel_token)

_nuid_suffix(client::Client)::String =
    @lock client.lock randstring(client.rng, NUID_ALPHABET, 22)

function new_inbox(client::Client; prefix::AbstractString=client.options.inbox_prefix)
    prefix = _validate_inbox_prefix(prefix)
    "$prefix.$(_nuid_suffix(client))"
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
    deadline = time() + Float64(timeout)
    while true
        token = _nuid_suffix(client)
        lock(mux.condition)
        try
            sub_active = @lock mux.sub.lock !mux.sub.closed && mux.sub.server_active
            (@atomic client.request_mux) === mux && sub_active || throw(ConnectionReconnectingError())
            if !haskey(mux.waiters, token)
                waiter = RequestWaiter{C}(deadline)
                mux.waiters[token] = waiter
                _ensure_request_timeout_task_locked!(client, mux)
                notify(mux.condition; all=true)
                return token, waiter
            end
        finally
            unlock(mux.condition)
        end
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

function _wait_request_reply(mux::RequestMux, waiter::RequestWaiter, timeout::Real;
                             cancel_token::MaybeCancellationToken=nothing)::Msg
    deadline = waiter.deadline
    lock(mux.condition)
    value = try
        while !waiter.ready
            _throw_if_cancelled(cancel_token)
            remaining = deadline - time()
            if remaining <= 0
                waiter.active = false
                throw(TimeoutError("request timed out"))
            end
            ready = _wait_until_condition_locked(mux.condition, remaining; cancel_token) do
                waiter.ready
            end
            if !ready
                waiter.active = false
                throw(TimeoutError("request timed out"))
            end
        end
        waiter.value
    finally
        unlock(mux.condition)
    end
    value isa Exception && throw(value)
    value::Msg
end

function _request_raw(client::Client, subject::AbstractString, data=nothing; timeout::Real=1.0,
                      headers=nothing, cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    timeout = _positive_timeout_seconds("timeout", timeout)
    request_frame = _publish_frame(subject, nothing, data, headers)
    _ensure_connected_for_request(client)
    _validate_publish_frame_for_client(client, request_frame)
    mux = _ensure_request_mux(client)
    token, waiter = _register_request_waiter!(client, mux, timeout)
    reply = "$(mux.prefix).$token"
    try
        _validate_publish_subject(reply)
        frame = _PublishFrame(request_frame.subject, reply, request_frame.payload, request_frame.headers)
        _publish_frame_unchecked(client, frame; buffer_on_reconnect=false, force_flush=true,
                                 cancel_token)
        return _wait_request_reply(mux, waiter, timeout; cancel_token)
    finally
        _remove_request_waiter!(client, mux, token, waiter)
    end
end

function request(client::Client, subject::AbstractString, data=nothing; timeout::Real=1.0,
                 headers=nothing, cancel_token::MaybeCancellationToken=nothing)
    msg = _request_raw(client, subject, data; timeout, headers, cancel_token)
    code = _status_header(msg)
    if code == 503
        throw(NoRespondersError(String(subject)))
    elseif !isnothing(code) && code >= 400
        throw(ProtocolError("request failed with status $code $(_status_description(msg))"))
    end
    msg
end
