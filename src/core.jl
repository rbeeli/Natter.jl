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
            err isa CancelledError && rethrow()
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

struct _PublishWriteFailure <: Exception
    cause::Any
    captured::Bool
    attempted::Bool
end

_should_write_publish_direct(frame_size::Int, threshold::Int)::Bool =
    threshold <= 0 || frame_size >= threshold

function _buffer_publish_frame(client::Client, io::BufferedWriteIO, frame::_AbstractPublishFrame,
                               replayable::Bool, frame_size::Int)
    _ensure_open(io)
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
        _write_pub_frame_direct(io, frame, client.write_scratch,
                                client.options.direct_write_threshold)
        force_flush && flush(io)
    end
    nothing
end

function _publish_write_reconnecting_message()
    "publish write failed during reconnect before it could be safely buffered"
end

function _write_publish(client::Client, frame::_AbstractPublishFrame; force_flush::Bool=false,
                        replayable::Bool=false, frame_size::Int=_serialized_size(frame),
                        direct_write::Bool=false,
                        payload_size::Int=_pub_payload_size(frame),
                        validate_frame::Bool=true,
                        cancel_token::MaybeCancellationToken=nothing)::Tuple{Bool,Bool}
    captured, attempted = false, false
    if isnothing(cancel_token)
        lock(client.write_lock)
    else
        _lock_write!(client, "publish", nothing, cancel_token)
    end
    try
        _throw_if_cancelled(cancel_token)
        st = status(client)
        io = @atomic client.write_io
        reconnect_pending = client.write_reconnect_pending[]
        reconnect_pending && st == ConnectionStatus.CONNECTED && throw(ConnectionReconnectingError())
        if !(st == ConnectionStatus.CONNECTED || st == ConnectionStatus.DRAINING)
            st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
            st == ConnectionStatus.DISCONNECTED && throw(ConnectionClosedError("connection is disconnected"))
            throw(ConnectionReconnectingError())
        end
        validate_frame && _validate_publish_frame_for_client(client, frame, payload_size)
        captured, attempted = _write_publish_to_active_io(client, io, frame, force_flush,
                                                          replayable, frame_size, direct_write)
    finally
        _unlock_write!(client)
    end
    captured, attempted
end

@noinline function _write_publish_to_active_io(client::Client, io::Union{Nothing,DefaultWriteTransportIO},
                                               frame::_AbstractPublishFrame, force_flush::Bool,
                                               replayable::Bool, frame_size::Int,
                                               direct_write::Bool)::Tuple{Bool,Bool}
    io === nothing && throw(ConnectionClosedError("connection transport is closed"))

    # connect() clients keep a union-typed field so reconnect can swap plain,
    # TLS, and buffered transports. Split that small default union here so the
    # hot frame writer below is compiled for concrete IO types.
    if io isa Sockets.TCPSocket
        return _write_publish_to_io(client, io, frame, force_flush, replayable,
                                    frame_size, direct_write)
    elseif io isa MbedTLS.SSLContext
        return _write_publish_to_io(client, io, frame, force_flush, replayable,
                                    frame_size, direct_write)
    elseif io isa BufferedWriteIO{Sockets.TCPSocket}
        return _write_publish_to_io(client, io, frame, force_flush, replayable,
                                    frame_size, direct_write)
    else
        return _write_publish_to_io(client, io::BufferedWriteIO{MbedTLS.SSLContext},
                                    frame, force_flush, replayable, frame_size, direct_write)
    end
end

function _write_publish_to_active_io(client::Client, io, frame::_AbstractPublishFrame,
                                     force_flush::Bool, replayable::Bool, frame_size::Int,
                                     direct_write::Bool)::Tuple{Bool,Bool}
    io === nothing && throw(ConnectionClosedError("connection transport is closed"))
    _write_publish_to_io(client, io, frame, force_flush, replayable, frame_size, direct_write)
end

function _write_publish_to_io(client::Client, io::WriteIO, frame::_AbstractPublishFrame, force_flush::Bool,
                              replayable::Bool, frame_size::Int, direct_write::Bool)::Tuple{Bool,Bool} where {WriteIO<:IO}
    attempted = false
    try
        attempted = true
        _write_pub_frame_direct_timed(client, io, frame; force_flush)
        return false, attempted
    catch err
        throw(_PublishWriteFailure(err, false, attempted))
    end
end

function _write_publish_to_io(client::Client, io::BufferedWriteIO, frame::_AbstractPublishFrame,
                              force_flush::Bool, replayable::Bool, frame_size::Int,
                              direct_write::Bool)::Tuple{Bool,Bool}
    threshold = max(0, client.options.write_buffer_size)
    captured, attempted = false, false
    try
        if direct_write || _should_write_publish_direct(frame_size, threshold)
            _flush_write_io(client, io)
            transport = _underlying_transport(io)
            attempted = true
            _write_pub_frame_direct_timed(client, transport, frame; force_flush=true)
            return false, attempted
        end

        captured = _buffer_publish_frame(client, io, frame, replayable, frame_size)
        attempted = _should_flush_write_io(client, io, force_flush)
        _flush_or_signal_locked(client, io, force_flush)
        return captured, attempted
    catch err
        err isa OutboundBufferLimitError && rethrow()
        throw(_PublishWriteFailure(err, captured, attempted))
    end
end

function _throw_not_connected_for_request(st::ConnectionStatus.T)
    st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
    st == ConnectionStatus.DISCONNECTED && throw(ConnectionClosedError("connection is disconnected"))
    st == ConnectionStatus.DRAINING && throw(ConnectionDrainingError())
    throw(ConnectionReconnectingError())
end

function _ensure_usable_status_for_request(st::ConnectionStatus.T)
    st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
    st == ConnectionStatus.DISCONNECTED && throw(ConnectionClosedError("connection is disconnected"))
    st == ConnectionStatus.DRAINING && throw(ConnectionDrainingError())
    nothing
end

function _ensure_connected_for_request(client::Client)
    st = status(client)
    st == ConnectionStatus.CONNECTED && return nothing
    _throw_not_connected_for_request(st)
end

function _ensure_request_publish_ready(client::Client, st::ConnectionStatus.T)
    st == ConnectionStatus.CONNECTED || _throw_not_connected_for_request(st)
    client.write_reconnect_pending[] && throw(ConnectionReconnectingError())
    nothing
end

function _send_publish(client::Client, frame::_AbstractPublishFrame; buffer_on_reconnect::Bool=false,
                       force_flush::Bool=false,
                       frame_size::Int=_serialized_size(frame),
                       payload_size::Int=_pub_payload_size(frame),
                       st::ConnectionStatus.T=status(client),
                       direct_write::Bool=false,
                       validate_frame::Bool=true,
                       cancel_token::MaybeCancellationToken=nothing)
    validate_frame && _validate_publish_frame_for_client(client, frame, payload_size)
    _throw_if_cancelled(cancel_token)
    can_buffer_reconnect = buffer_on_reconnect && _reconnect_buffer_enabled(client)
    if st == ConnectionStatus.CONNECTED && client.write_reconnect_pending[]
        can_buffer_reconnect || throw(ConnectionReconnectingError())
        _enqueue_pending(client, frame)
        return nothing
    end
    if st in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING)
        try
            _write_publish(client, frame; force_flush, replayable=can_buffer_reconnect,
                           frame_size, direct_write, payload_size, validate_frame=false,
                           cancel_token)
        catch err
            failure = err isa _PublishWriteFailure ? err : _PublishWriteFailure(err, false, false)
            cause = failure.cause
            cause isa CancelledError && throw(cause)
            cause isa OutboundBufferLimitError && throw(cause)
            if st == ConnectionStatus.CONNECTED && _recover_after_write_failure!(client, cause)
                if can_buffer_reconnect
                    if failure.attempted
                        throw(ConnectionReconnectingError(_publish_write_reconnecting_message()))
                    elseif !failure.captured
                        _enqueue_pending(client, frame)
                    end
                    return nothing
                end
                throw(ConnectionReconnectingError())
            end
            throw(cause)
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

_subscription_wire_max(max_msgs::Int, delivered_base::Int)::Int =
    max_msgs > 0 ? max_msgs - delivered_base : 0

function _set_subscription_server_active_locked!(sub::Subscription, delivered_base::Int)
    sub.server_delivered_base = delivered_base
    sub.server_active = true
    nothing
end

function _send_subscription_now!(sub::Subscription; cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    sid, subject, queue, max_msgs, delivered = @lock sub.lock begin
        (sub.sid, sub.subject, sub.queue, sub.max_msgs, sub.delivered)
    end
    remaining = max_msgs > 0 ? max_msgs - delivered : 0
    if max_msgs > 0 && remaining <= 0
        _close_subscription_locally!(sub; throw_errors=false)
        return false
    end
    try
        _write_raw(sub.client, _subscription_setup_cmd(subject, queue, sid, remaining);
                   cancel_token)
        @lock sub.lock _set_subscription_server_active_locked!(sub, delivered)
        return true
    catch err
        err isa CancelledError && rethrow()
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
    if st in (ConnectionStatus.RECONNECTING, ConnectionStatus.CONNECTING) ||
       (st == ConnectionStatus.CONNECTED && client.write_reconnect_pending[])
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

function _publish_prepared(client::Client, frame::_AbstractPublishFrame; buffer_on_reconnect::Bool=false,
                           force_flush::Bool=false, direct_write::Bool=false,
                           cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    payload_size = _pub_payload_size(frame)
    frame_size = _serialized_size(frame)
    st = status(client)
    _ensure_usable_status_for_publish(st)
    _validate_publish_frame_for_client(client, frame, payload_size)
    _send_publish(client, frame; buffer_on_reconnect, force_flush, frame_size,
                  payload_size, st, direct_write, validate_frame=false, cancel_token)
    _record_out!(client, payload_size)
    nothing
end

function _publish(client::Client, subject::AbstractString, data=nothing; reply::Union{AbstractString,Nothing}=nothing,
                  headers=nothing, buffer_on_reconnect::Bool=false, force_flush::Bool=false,
                  direct_write::Bool=false,
                  cancel_token::MaybeCancellationToken=nothing)
    _publish_prepared(client, _publish_frame(subject, reply, data, headers);
                      buffer_on_reconnect, force_flush, direct_write, cancel_token)
end

function _publish_frame_unchecked(client::Client, frame::_AbstractPublishFrame; buffer_on_reconnect::Bool=false,
                                  force_flush::Bool=false, direct_write::Bool=false,
                                  cancel_token::MaybeCancellationToken=nothing,
                                  validate_frame::Bool=true)
    _throw_if_cancelled(cancel_token)
    frame_size = _serialized_size(frame)
    payload_size = _pub_payload_size(frame)
    st = status(client)
    _ensure_usable_status_for_publish(st)
    validate_frame && _validate_publish_frame_for_client(client, frame, payload_size)
    _send_publish(client, frame; buffer_on_reconnect, force_flush, frame_size,
                  payload_size, st, direct_write, validate_frame=false, cancel_token)
    _record_out!(client, payload_size)
    nothing
end

function _publish_unchecked(client::Client, subject::String, payload::AbstractVector{UInt8};
                            buffer_on_reconnect::Bool=false, force_flush::Bool=false,
                            direct_write::Bool=false,
                            cancel_token::MaybeCancellationToken=nothing)
    _publish_frame_unchecked(client, _publish_frame(subject, nothing, payload, EMPTY_BYTES);
                             buffer_on_reconnect, force_flush, direct_write, cancel_token)
end

function publish(client::Client, subject::AbstractString, data=nothing; reply::Union{AbstractString,Nothing}=nothing,
                 headers=nothing, buffer_on_reconnect::Bool=false, direct_write::Bool=false,
                 cancel_token::MaybeCancellationToken=nothing)
    _publish(client, subject, data; reply, headers, buffer_on_reconnect, direct_write,
             cancel_token)
end

function publish(client::Client, frame::PublishFrame; buffer_on_reconnect::Bool=false,
                 direct_write::Bool=false, cancel_token::MaybeCancellationToken=nothing)
    _publish_prepared(client, frame; buffer_on_reconnect, direct_write, cancel_token)
end

function respond(client::Client, msg::AbstractMsg, data=nothing; headers=nothing,
                 buffer_on_reconnect::Bool=false, direct_write::Bool=false,
                 cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    reply = msg.reply
    isnothing(reply) && throw(ArgumentError("message has no reply subject"))
    publish(client, reply, data; headers, buffer_on_reconnect, direct_write, cancel_token)
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

function _subscribe_unlocked(client::Client, subject::AbstractString; queue::Union{AbstractString,Nothing}=nothing, callback=nothing,
                             borrowed::Bool=false,
                             max_msgs=0, pending_msgs_limit=client.options.sub_pending_msgs_limit,
                             pending_bytes_limit=client.options.sub_pending_bytes_limit,
                             _control_handler::_SubscriptionControlHandler=_NoSubscriptionControlHandler(),
                             require_connected::Bool=false,
                             cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    subject = _validate_subject(subject)
    queue = _validate_queue(queue)
    borrowed = _connect_option_bool("borrowed", borrowed)
    borrowed && isnothing(callback) &&
        throw(ArgumentError("borrowed subscriptions require a callback"))
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
        sub = Subscription(client, sid, subject, queue, callback, borrowed, sub_lock, ch,
                           condition, _control_handler, pending_msgs_limit,
                           pending_bytes_limit, 0, 0, 0, 0, max_msgs, false, nothing,
                           false, 0, 0)
        client.subscriptions[sid] = sub
        _set_subscription_snapshot_locked!(client, sid, sub)
        send_now = st == ConnectionStatus.CONNECTED
        sub
    end
    if send_now
        try
            _send_subscription_now!(sub; cancel_token)
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
    if !isnothing(callback) && !borrowed
        _start_subscription_processor!(sub, callback)
    end
    sub
end

function _subscribe(client::Client, subject::AbstractString; kwargs...)
    lock(client.subscription_replay_lock)
    try
        _subscribe_unlocked(client, subject; kwargs...)
    finally
        unlock(client.subscription_replay_lock)
    end
end

function subscribe(client::Client, subject::AbstractString; cancel_token::MaybeCancellationToken=nothing,
                   kwargs...)
    _throw_if_cancelled(cancel_token)
    _subscribe(client, subject; cancel_token, kwargs...)
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

mutable struct _ReaderMsgRouteResolver{C<:Client}
    client::C
    sub::Union{Subscription{C},Nothing}
    borrow_payload::Bool
    fast_control::Bool
end

_ReaderMsgRouteResolver(client::C) where {C<:Client} =
    _ReaderMsgRouteResolver{C}(client, nothing, false, false)

@inline function _resolve_message_route(resolver::_ReaderMsgRouteResolver{C},
                                        sid::Int)::_ReaderMsgRouteResolver{C} where {C<:Client}
    entry = _lookup_subscription_snapshot_entry(resolver.client, sid)
    if isnothing(entry)
        resolver.sub = nothing
        resolver.borrow_payload = false
        resolver.fast_control = false
    else
        resolver.sub = entry.sub
        resolver.borrow_payload = entry.borrow_payload
        resolver.fast_control = entry.fast_control
    end
    resolver
end

@inline _route_borrow_payload(route::_ReaderMsgRouteResolver)::Bool = route.borrow_payload

function _owned_msg(msg::BorrowedMsg)::Msg
    Msg(msg.subject, msg.reply, Vector{UInt8}(msg.data);
        headers=msg.headers, sid=msg.sid, header_bytes=msg.header_bytes)
end

_handle_subscription_control(::_NoSubscriptionControlHandler, _sub::Subscription, _msg::AbstractMsg)::Bool = false
_record_subscription_data_received!(::_SubscriptionControlHandler, ::AbstractMsg) = nothing
_maybe_reply_to_subscription_flow_control!(::Subscription, ::_SubscriptionControlHandler) = nothing

function _parse_decimal_token(subject::String, first::Int, last::Int)::Union{Int,Nothing}
    first <= last || return nothing
    if first < last
        leading_zero = @inbounds codeunit(subject, first) == UInt8('0')
        leading_zero && return nothing
    end
    value = 0
    @inbounds for i in first:last
        byte = codeunit(subject, i)
        UInt8('0') <= byte <= UInt8('9') || return nothing
        digit = Int(byte - UInt8('0'))
        value <= (typemax(Int) - digit) ÷ 10 || return nothing
        value = value * 10 + digit
    end
    value
end

function _parse_decimal_token(bytes::AbstractVector{UInt8}, first::Int,
                              last::Int)::Union{Int,Nothing}
    first <= last || return nothing
    if first < last
        leading_zero = @inbounds bytes[first] == UInt8('0')
        leading_zero && return nothing
    end
    value = 0
    @inbounds for i in first:last
        byte = bytes[i]
        UInt8('0') <= byte <= UInt8('9') || return nothing
        digit = Int(byte - UInt8('0'))
        value <= (typemax(Int) - digit) ÷ 10 || return nothing
        value = value * 10 + digit
    end
    value
end

function _request_mux_token(prefix::String, subject::String)::Union{Int,Nothing}
    prefix_len = ncodeunits(prefix)
    subject_len = ncodeunits(subject)
    subject_len > prefix_len + 1 || return nothing
    startswith(subject, prefix) || return nothing
    codeunit(subject, prefix_len + 1) == UInt8('.') || return nothing
    token_start = prefix_len + 2
    _parse_decimal_token(subject, token_start, subject_len)
end

function _request_mux_token(prefix::String, bytes::AbstractVector{UInt8},
                            subject_start::Int, subject_end::Int)::Union{Int,Nothing}
    prefix_len = ncodeunits(prefix)
    subject_len = subject_end - subject_start + 1
    subject_len > prefix_len + 1 || return nothing
    @inbounds for i in 1:prefix_len
        bytes[subject_start + i - 1] == codeunit(prefix, i) || return nothing
    end
    dot = subject_start + prefix_len
    @inbounds bytes[dot] == UInt8('.') || return nothing
    _parse_decimal_token(bytes, dot + 1, subject_end)
end

function _next_request_mux_token!(mux::RequestMux)::Int
    start = mux.next_token
    while true
        token = mux.next_token == typemax(Int) ? 1 : mux.next_token + 1
        mux.next_token = token
        !haskey(mux.waiters, token) && return token
        token == start && throw(OverflowError("request mux token space exhausted"))
    end
end

_try_handle_subscription_msg_control(_handler::_SubscriptionControlHandler, _client::Client,
                                     _sub::Subscription, _reader::ProtocolReader,
                                     _subject_start::Int, _subject_end::Int,
                                     _reply_start::Int, _reply_end::Int,
                                     _size::Int)::Bool = false

_try_handle_subscription_hmsg_control(_handler::_SubscriptionControlHandler, _client::Client,
                                      _sub::Subscription, _reader::ProtocolReader,
                                      _subject_start::Int, _subject_end::Int,
                                      _reply_start::Int, _reply_end::Int,
                                      _hsize::Int, _total::Int)::Bool = false

function _try_handle_msg_control(dispatcher::_ReaderMsgDispatcher{C},
                                 route::_ReaderMsgRouteResolver{C}, reader::ProtocolReader,
                                 sid::Int, subject_start::Int, subject_end::Int,
                                 reply_start::Int, reply_end::Int,
                                 size::Int, borrowed::Bool)::Bool where {C<:Client}
    borrowed && return false
    client = dispatcher.client
    sub = route.sub
    (isnothing(sub) || !route.fast_control) && return false
    active = false
    control_handler = _NoSubscriptionControlHandler()
    @lock sub.lock begin
        if !sub.closed && sub.sid == sid
            active = true
            control_handler = sub.control_handler
        end
    end
    active || return false
    _try_handle_subscription_msg_control(control_handler, client, sub, reader, subject_start,
                                         subject_end, reply_start, reply_end, size)
end

function _try_handle_hmsg_control(dispatcher::_ReaderMsgDispatcher{C},
                                  route::_ReaderMsgRouteResolver{C}, reader::ProtocolReader,
                                  sid::Int, subject_start::Int, subject_end::Int,
                                  reply_start::Int, reply_end::Int,
                                  hsize::Int, total::Int, borrowed::Bool)::Bool where {C<:Client}
    borrowed && return false
    client = dispatcher.client
    sub = route.sub
    (isnothing(sub) || !route.fast_control) && return false
    active = false
    control_handler = _NoSubscriptionControlHandler()
    @lock sub.lock begin
        if !sub.closed && sub.sid == sid
            active = true
            control_handler = sub.control_handler
        end
    end
    active || return false
    _try_handle_subscription_hmsg_control(control_handler, client, sub, reader, subject_start,
                                          subject_end, reply_start, reply_end, hsize, total)
end

function _fast_control_reply(reader::ProtocolReader, reply_start::Int,
                             reply_end::Int)::Union{String,Nothing}
    reply_start == 0 && return nothing
    _bytes_string(reader.buffer, reply_start, reply_end)
end

function _fast_control_msg_parts(reader::ProtocolReader, reply_start::Int,
                                 reply_end::Int, size::Int)
    reply = _fast_control_reply(reader, reply_start, reply_end)
    payload = _read_exact_payload(reader, size)
    reply, payload
end

function _fast_control_hmsg_parts(reader::ProtocolReader, reply_start::Int, reply_end::Int,
                                  hsize::Int, total::Int)
    reply = _fast_control_reply(reader, reply_start, reply_end)
    header_bytes, payload = _read_exact_header_payload(reader, hsize, total)
    status, description_first, description_last = _validate_headers(header_bytes)
    RawHeaders(header_bytes, status, description_first, description_last), reply, payload
end

function _finish_fast_control_msg!(client::Client, msg_bytes::Int, drop::Bool,
                                   notify_err, subject::Union{String,Nothing})
    drop && _record_drop!(client)
    _record_in!(client, msg_bytes)
    if !isnothing(notify_err)
        detail = isnothing(subject) ? "deliver control reply" : "deliver request reply $subject"
        _report_error(client, CleanupError(detail, notify_err))
    end
    true
end

function _resolve_fast_request_mux_reply!(client::Client, sub::Subscription, mux::RequestMux,
                                          token::Union{Int,Nothing},
                                          reply::Union{String,Nothing},
                                          payload::Vector{UInt8}, headers::HeaderStorage,
                                          header_bytes::Int)
    waiter = nothing
    drop = false
    notify_err = nothing
    subject = nothing
    if isnothing(token) || mux.sub !== sub
        drop = true
    else
        lock(mux.condition)
        try
            if (@atomic client.request_mux) !== mux
                drop = true
            else
                waiter = pop!(mux.waiters, token, nothing)
                drop = isnothing(waiter)
                if !drop
                    subject = waiter.reply
                    msg = Msg(waiter.reply, reply, payload, headers, sub.sid, header_bytes)
                    try
                        _resolve_request_waiter_locked!(waiter, msg, mux.condition)
                        _compact_request_deadline_queue_locked!(mux)
                    catch err
                        notify_err = err
                    end
                end
            end
        finally
            unlock(mux.condition)
        end
    end
    drop, notify_err, subject
end

function _try_handle_subscription_msg_control(::_RequestMuxControlHandler, client::Client,
                                              sub::Subscription, reader::ProtocolReader,
                                              subject_start::Int, subject_end::Int,
                                              reply_start::Int, reply_end::Int,
                                              size::Int)::Bool
    mux = @atomic client.request_mux
    token = isnothing(mux) ? nothing :
            _request_mux_token(mux.prefix, reader.buffer, subject_start, subject_end)
    reply, payload = _fast_control_msg_parts(reader, reply_start, reply_end, size)
    drop, notify_err, subject = isnothing(mux) ? (true, nothing, nothing) :
                                _resolve_fast_request_mux_reply!(client, sub, mux, token,
                                                                 reply, payload, nothing, 0)
    _finish_fast_control_msg!(client, size, drop, notify_err, subject)
end

function _try_handle_subscription_hmsg_control(::_RequestMuxControlHandler, client::Client,
                                               sub::Subscription, reader::ProtocolReader,
                                               subject_start::Int, subject_end::Int,
                                               reply_start::Int, reply_end::Int,
                                               hsize::Int, total::Int)::Bool
    mux = @atomic client.request_mux
    token = isnothing(mux) ? nothing :
            _request_mux_token(mux.prefix, reader.buffer, subject_start, subject_end)
    hdrs, reply, payload = _fast_control_hmsg_parts(reader, reply_start, reply_end, hsize, total)
    drop, notify_err, subject = isnothing(mux) ? (true, nothing, nothing) :
                                _resolve_fast_request_mux_reply!(client, sub, mux, token,
                                                                 reply, payload, hdrs, hsize)
    _finish_fast_control_msg!(client, total, drop, notify_err, subject)
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
                            _compact_request_deadline_queue_locked!(mux)
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

@inline function _dispatch_protocol_msg(dispatcher::_ReaderMsgDispatcher{C},
                                        route::_ReaderMsgRouteResolver{C},
                                        msg::Msg) where {C<:Client}
    _dispatch_msg(dispatcher.client, route.sub, msg)
    nothing
end

@inline function _dispatch_protocol_msg(dispatcher::_ReaderMsgDispatcher{C},
                                        route::_ReaderMsgRouteResolver{C},
                                        msg::BorrowedMsg) where {C<:Client}
    _dispatch_msg(dispatcher.client, route.sub, msg)
    nothing
end

@inline function _dispatch_msg(client::C, sub::Union{Subscription{C},Nothing},
                               msg::Msg) where {C<:Client}
    msg_bytes = _msg_pending_bytes(msg)
    if isnothing(sub)
        _record_drop!(client)
        return
    end
    _dispatch_owned_msg_to_sub(client, sub, msg, msg_bytes)
end

@inline function _dispatch_msg(client::C, msg::Msg) where {C<:Client}
    _dispatch_msg(client, _lookup_subscription(client, msg.sid), msg)
end

function _dispatch_owned_msg_to_sub(client::Client, sub::Subscription, msg::Msg,
                                    msg_bytes::Int)
    control_handler = _NoSubscriptionControlHandler()
    should_close = false
    inactive = @lock sub.lock begin
        if sub.closed || sub.sid != msg.sid
            true
        else
            sub.delivered += 1
            should_close = sub.max_msgs > 0 && sub.delivered >= sub.max_msgs
            control_handler = sub.control_handler
            false
        end
    end
    if inactive
        _record_drop!(client)
        return
    end

    if _handle_subscription_control(control_handler, sub, msg)
        _record_in!(client, msg_bytes)
        should_close && _close_subscription_locally!(sub; throw_errors=false)
        return
    end
    if _handle_ordered_push_data!(control_handler, client, msg)
        @lock sub.lock sub.dropped_msgs += 1
        _record_drop!(client)
        should_close && _close_subscription_locally!(sub; throw_errors=false)
        return
    end

    report_slow = false
    accepted = @lock sub.lock begin
        if sub.closed
            report_slow = false
            sub.dropped_msgs += 1
            false
        elseif sub.pending_bytes + msg_bytes > sub.pending_bytes_limit || Base.n_avail(sub.messages) >= sub.pending_msgs_limit
            report_slow = true
            sub.dropped_msgs += 1
            false
        else
            sub.received += 1
            sub.pending_bytes += msg_bytes
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
        should_close && _close_subscription_locally!(sub; throw_errors=false)
        return
    end
    _record_in!(client, msg_bytes)
    _record_subscription_data_received!(control_handler, msg)
    if should_close
        _close_subscription_locally!(sub; throw_errors=false)
    end
    nothing
end

@noinline function _invoke_borrowed_callback(client::Client, callback::Callback,
                                             msg::M) where {Callback,M<:AbstractMsg}
    try
        callback(msg)
    catch err
        _report_error(client, err)
    end
    nothing
end

@noinline function _finish_borrowed_callback!(sub::Subscription)
    @lock sub.lock begin
        sub.processing = max(0, sub.processing - 1)
        _notify_subscription_waiters_locked(sub; all=true)
    end
    nothing
end

function _invoke_borrowed_handler(client::C, handler::_BorrowedCallback{C},
                                  msg::_BorrowedDispatchMsg) where {C<:Client}
    handler.invoke(client, msg)
    nothing
end

function _borrowed_dispatch_msg(msg::BorrowedMsg)::_BorrowedDispatchMsg
    data = Vector{UInt8}(msg.data)
    BorrowedMsg(msg.subject, msg.reply, @view(data[1:length(data)]), msg.headers, msg.sid,
                msg.header_bytes)
end

function _borrowed_dispatch_msg(msg::BorrowedMsg{Vector{UInt8}})::_BorrowedDispatchMsg
    BorrowedMsg(msg.subject, msg.reply, @view(msg.data[1:length(msg.data)]), msg.headers, msg.sid,
                msg.header_bytes)
end

function _invoke_borrowed_handler(client::Client, handler::_BorrowedCallback,
                                  msg::BorrowedMsg)
    handler.invoke(client, _borrowed_dispatch_msg(msg))
    nothing
end

@noinline function _run_borrowed_callback(client::Client, sub::Subscription,
                                          handler::_BorrowedCallback, msg::M) where {M<:AbstractMsg}
    try
        _invoke_borrowed_handler(client, handler, msg)
    finally
        _finish_borrowed_callback!(sub)
    end
    nothing
end

@inline function _dispatch_msg(client::C, sub::Union{Subscription{C},Nothing},
                               msg::BorrowedMsg) where {C<:Client}
    msg_bytes = _msg_pending_bytes(msg)
    if isnothing(sub)
        _record_drop!(client)
        return
    end
    _dispatch_borrowed_msg_to_sub(client, sub, msg, msg_bytes)
end

@inline function _dispatch_msg(client::C, msg::BorrowedMsg) where {C<:Client}
    _dispatch_msg(client, _lookup_subscription(client, msg.sid), msg)
end

function _dispatch_borrowed_msg_to_sub(client::C, sub::Subscription{C}, msg::BorrowedMsg,
                                       msg_bytes::Int) where {C<:Client}
    control_handler = _NoSubscriptionControlHandler()
    dispatch_owned = false
    should_close = false
    inactive = @lock sub.lock begin
        if sub.closed || sub.sid != msg.sid
            true
        elseif !sub.borrowed_callback
            dispatch_owned = true
            false
        else
            sub.delivered += 1
            should_close = sub.max_msgs > 0 && sub.delivered >= sub.max_msgs
            control_handler = sub.control_handler
            false
        end
    end

    if inactive
        _record_drop!(client)
        return
    end
    if dispatch_owned
        _dispatch_msg(client, sub, _owned_msg(msg))
        return
    end

    if _handle_subscription_control(control_handler, sub, msg)
        _record_in!(client, msg_bytes)
        should_close && _close_subscription_locally!(sub; throw_errors=false)
        return
    end
    if _handle_ordered_push_data!(control_handler, client, msg)
        @lock sub.lock sub.dropped_msgs += 1
        _record_drop!(client)
        should_close && _close_subscription_locally!(sub; throw_errors=false)
        return
    end

    handler = sub.borrowed_callback_handler
    accepted = @lock sub.lock begin
        if sub.closed || !sub.borrowed_callback || !sub.has_callback
            sub.dropped_msgs += 1
            false
        else
            sub.received += 1
            sub.processing += 1
            true
        end
    end
    if !accepted
        _record_drop!(client)
        should_close && _close_subscription_locally!(sub; throw_errors=false)
        return
    end

    _record_in!(client, msg_bytes)
    _record_subscription_data_received!(control_handler, msg)
    _maybe_reply_to_subscription_flow_control!(sub, control_handler)
    _run_borrowed_callback(client, sub, handler, msg)
    if should_close
        _close_subscription_locally!(sub; throw_errors=false)
    end
    nothing
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

function _unsubscribe_target(delivered::Int, additional::Int)
    delivered > typemax(Int) - additional &&
        throw(ArgumentError("max_msgs is too large for subscription message count"))
    delivered + additional
end

function _restore_unsubscribe_target!(sub::Subscription, max_msgs::Int, target::Int,
                                      previous_max::Int)
    max_msgs > 0 || return nothing
    @lock sub.lock begin
        if !sub.closed && sub.max_msgs == target
            sub.max_msgs = previous_max
        end
    end
    nothing
end

function _unsubscribe_write_mode(st::ConnectionStatus.T, active::Bool)
    st == ConnectionStatus.CONNECTED && return _RAW_WRITE_CONNECTED
    st == ConnectionStatus.RECONNECTING && active && return _RAW_WRITE_RECONNECT_REPLAY
    nothing
end

function _unsubscribe(sub::Subscription; max_msgs=0,
                      cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    max_msgs = _validate_core_max_msgs(max_msgs)
    st = status(sub.client)
    closed, active, sid, target, wire_target, previous_max = @lock sub.lock begin
        if sub.closed
            (true, false, sub.sid, 0, 0, sub.max_msgs)
        else
            target = 0
            wire_target = 0
            previous_max = sub.max_msgs
            if max_msgs > 0
                st in (ConnectionStatus.CLOSED, ConnectionStatus.DISCONNECTED) &&
                    throw(ConnectionClosedError("connection is disconnected"))
                st == ConnectionStatus.DRAINING && throw(ConnectionDrainingError())
                target = _unsubscribe_target(sub.delivered, max_msgs)
                wire_target = sub.server_active ?
                              _subscription_wire_max(target, sub.server_delivered_base) :
                              0
                sub.max_msgs = target
            end
            (false, sub.server_active, sub.sid, target, wire_target, previous_max)
        end
    end
    closed && return nothing
    write_mode = _unsubscribe_write_mode(st, active)
    if !isnothing(write_mode)
        try
            _write_raw(sub.client, _unsub_cmd(sid, wire_target);
                       write_mode, cancel_token)
        catch err
            if err isa CancelledError
                _restore_unsubscribe_target!(sub, max_msgs, target, previous_max)
                rethrow()
            end
            if !_recover_after_write_failure!(sub.client, err)
                _restore_unsubscribe_target!(sub, max_msgs, target, previous_max)
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

function unsubscribe(sub::Subscription; max_msgs=0, cancel_token::MaybeCancellationToken=nothing)
    lock(sub.client.subscription_replay_lock)
    try
        _unsubscribe(sub; max_msgs, cancel_token)
    finally
        unlock(sub.client.subscription_replay_lock)
    end
end

close(sub::Subscription; cancel_token::MaybeCancellationToken=nothing) =
    unsubscribe(sub; cancel_token)

_drain_deadline(timeout::Real)::Float64 = time() + _positive_timeout_seconds("timeout", timeout)
_drain_timed_out(err)::Bool =
    err isa TimeoutError ||
    (err isa CleanupError && _drain_timed_out(err.cause)) ||
    (err isa Base.CompositeException && any(_drain_timed_out, err.exceptions))
_drain_timed_out(errors::Vector)::Bool = any(_drain_timed_out, errors)

function _drain_send_unsub!(sub::Subscription, deadline::Float64;
                            cancel_token::MaybeCancellationToken=nothing)::Bool
    _throw_if_cancelled(cancel_token)
    closed, active, sid = @lock sub.lock (sub.closed, sub.server_active, sub.sid)
    closed && return false
    status(sub.client) in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING) || throw(ConnectionReconnectingError())
    if active
        try
            _write_raw(sub.client, _unsub_cmd(sid); deadline=deadline, write_mode=_RAW_WRITE_DRAIN,
                       cancel_token)
            @lock sub.lock begin
                sub.closed || (sub.server_active = false)
            end
        catch err
            err isa CancelledError && rethrow()
            _drain_timed_out(err) && rethrow()
            _recover_after_write_failure!(sub.client, err) || rethrow()
            throw(ConnectionReconnectingError())
        end
    end
    true
end

function _drain_wait_subscription!(sub::Subscription, deadline::Float64;
                                   cancel_token::MaybeCancellationToken=nothing)
    ready = @lock sub.lock begin
        _wait_until_condition_locked(sub.condition, _remaining_timeout(deadline); cancel_token) do
            !isready(sub.messages) && sub.processing == 0
        end
    end
    ready || throw(TimeoutError("subscription drain timed out"))
    nothing
end

function _finish_drained_subscription!(sub::Subscription)
    sid, already_closed = @lock sub.lock begin
        already_closed = sub.closed
        if !already_closed
            sub.closed = true
            sub.server_active = false
            _notify_subscription_waiters_locked(sub; all=true)
        end
        (sub.sid, already_closed)
    end
    already_closed && return nothing
    @lock sub.client.lock begin
        _delete_subscription_locked!(sub.client, sid, sub)
    end
    errors = Any[]
    _close_subscription_channel!(errors, sub)
    _throw_errors(errors)
    nothing
end

function _drain(sub::Subscription, deadline::Float64; cancel_token::MaybeCancellationToken=nothing)
    active = _drain_send_unsub!(sub, deadline; cancel_token)
    active || return nothing
    _flush(sub.client; timeout=_remaining_timeout(deadline), deadline=deadline, cancel_token)
    _drain_wait_subscription!(sub, deadline; cancel_token)
    _finish_drained_subscription!(sub)
    nothing
end

function _drain_client_subscription_wait!(sub::Subscription, deadline::Float64;
                                          cancel_token::MaybeCancellationToken=nothing)
    (@lock sub.lock sub.closed) && return nothing
    _drain_wait_subscription!(sub, deadline; cancel_token)
    (@lock sub.lock sub.closed) && return nothing
    _finish_drained_subscription!(sub)
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
            _drain_send_unsub!(sub, deadline; cancel_token)
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
    if !_drain_timed_out(errors)
        for sub in subs
            try
                _drain_client_subscription_wait!(sub, deadline; cancel_token)
            catch err
                push!(errors, err)
                _report_error(client, err)
                _drain_timed_out(err) && break
            end
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
            @atomic client.subscription_snapshot =
                Dict{Int,_SubscriptionSnapshotEntry{typeof(client)}}()
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

function _request_mux_usable_locked(client::Client)
    mux = @atomic client.request_mux
    isnothing(mux) && return nothing
    open, active = @lock mux.sub.lock (!mux.sub.closed, mux.sub.server_active)
    open || return nothing
    st = client.status
    if st == ConnectionStatus.CONNECTED && !client.write_reconnect_pending[]
        return active ? mux : nothing
    elseif st in (ConnectionStatus.CONNECTING, ConnectionStatus.RECONNECTING) ||
           (st == ConnectionStatus.CONNECTED && client.write_reconnect_pending[])
        return mux
    end
    nothing
end

function _request_mux_usable_for_new_sub_locked(client::Client, sub::Subscription)::Bool
    open, active = @lock sub.lock (!sub.closed, sub.server_active)
    open || return false
    st = client.status
    if st == ConnectionStatus.CONNECTED && !client.write_reconnect_pending[]
        return active
    elseif st in (ConnectionStatus.CONNECTING, ConnectionStatus.RECONNECTING) ||
           (st == ConnectionStatus.CONNECTED && client.write_reconnect_pending[])
        return true
    end
    false
end

function _request_mux_usable_for_new_sub(client::Client, sub::Subscription)::Bool
    @lock client.lock _request_mux_usable_for_new_sub_locked(client, sub)
end

function _request_mux_usable(client::Client, mux::RequestMux)::Bool
    @lock client.lock begin
        (@atomic client.request_mux) === mux || return false
        !isnothing(_request_mux_usable_locked(client))
    end
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

function _ensure_request_mux(client::Client; cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    _ensure_usable_status_for_request(status(client))
    mux = @lock client.lock _request_mux_usable_locked(client)
    isnothing(mux) || return mux

    @lock client.request_mux_lock begin
        _throw_if_cancelled(cancel_token)
        _ensure_usable_status_for_request(status(client))
        mux = @lock client.lock _request_mux_usable_locked(client)
        isnothing(mux) || return mux

        prefix = new_inbox(client)
        sub = _subscribe(client, "$prefix.*"; _control_handler=_RequestMuxControlHandler(),
                         cancel_token)
        if !_request_mux_usable_for_new_sub(client, sub)
            _close_inactive_request_mux_subscription!(client, sub)
            throw(ConnectionReconnectingError())
        end

        mux_lock = ReentrantLock()
        mux = RequestMux(prefix, sub, Dict{Int,RequestWaiter{typeof(client)}}(),
                         Base.Threads.Condition(mux_lock),
                         Base.Threads.Condition(mux_lock), 0,
                         _DeadlineQueue{RequestWaiter{typeof(client)}}(), nothing)
        assigned = @lock client.lock begin
            if _request_mux_usable_for_new_sub_locked(client, sub)
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

function _request_deadline_entry_valid_locked(mux::RequestMux, entry::_DeadlineEntry)::Bool
    waiter = get(mux.waiters, entry.token, nothing)
    waiter === entry.value && waiter.active && waiter.deadline == entry.deadline
end

function _next_request_deadline_entry_locked!(mux::RequestMux)
    while true
        entry = _deadline_queue_peek(mux.deadline_queue)
        isnothing(entry) && return nothing
        _request_deadline_entry_valid_locked(mux, entry) && return entry
        _deadline_queue_pop!(mux.deadline_queue)
    end
end

function _rebuild_request_deadline_queue_locked!(mux::RequestMux{C}) where {C<:Client}
    queue = _DeadlineQueue{RequestWaiter{C}}()
    for (token, waiter) in mux.waiters
        waiter.active && _deadline_queue_push!(queue, token, waiter.deadline, waiter)
    end
    mux.deadline_queue = queue
    nothing
end

function _compact_request_deadline_queue_locked!(mux::RequestMux)
    _deadline_queue_compaction_due(mux.deadline_queue, length(mux.waiters)) ||
        return nothing
    _rebuild_request_deadline_queue_locked!(mux)
end

function _notify_request_timeout_task_locked(mux::RequestMux)
    notify(mux.timeout_condition; all=true)
    nothing
end

function _wait_request_timeout_locked!(mux::RequestMux, delay::Float64)
    timer = Timer(min(delay, _MAX_TIMER_DELAY_SECONDS)) do _
        lock(mux.timeout_condition)
        try
            _notify_request_timeout_task_locked(mux)
        finally
            unlock(mux.timeout_condition)
        end
    end
    try
        wait(mux.timeout_condition)
    finally
        close(timer)
    end
    nothing
end

function _request_timeout_loop(client::C, mux::RequestMux{C}) where {C<:Client}
    lock(mux.condition)
    try
        while true
            if (@atomic client.request_mux) !== mux || isempty(mux.waiters)
                empty!(mux.deadline_queue)
                mux.timeout_task = nothing
                return nothing
            end

            entry = _next_request_deadline_entry_locked!(mux)
            if isnothing(entry)
                _rebuild_request_deadline_queue_locked!(mux)
                entry = _next_request_deadline_entry_locked!(mux)
                if isnothing(entry)
                    empty!(mux.waiters)
                    empty!(mux.deadline_queue)
                    mux.timeout_task = nothing
                    return nothing
                end
            end

            now = time()
            delay = entry.deadline - now
            if !isfinite(delay)
                wait(mux.timeout_condition)
            elseif delay <= 0
                _deadline_queue_pop!(mux.deadline_queue)
                waiter = pop!(mux.waiters, entry.token, nothing)
                if waiter === entry.value && waiter.active && waiter.deadline == entry.deadline
                    _resolve_request_waiter_locked!(waiter, TimeoutError("request timed out"), mux.condition)
                end
            else
                _wait_request_timeout_locked!(mux, delay)
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
    _ensure_usable_status_for_request(st)
    _request_mux_usable(client, mux) || throw(ConnectionReconnectingError())
    deadline = time() + Float64(timeout)
    lock(mux.condition)
    try
        _request_mux_usable(client, mux) || throw(ConnectionReconnectingError())
        token = _next_request_mux_token!(mux)
        reply = string(mux.prefix, '.', token)
        waiter = RequestWaiter{C}(deadline, reply)
        mux.waiters[token] = waiter
        wake_timeout = _deadline_queue_push!(mux.deadline_queue, token, deadline, waiter)
        _ensure_request_timeout_task_locked!(client, mux)
        wake_timeout && _notify_request_timeout_task_locked(mux)
        return token, waiter
    finally
        unlock(mux.condition)
    end
end

function _remove_request_waiter!(client::C, mux::RequestMux{C}, token::Int,
                                 waiter::RequestWaiter{C}) where {C<:Client}
    lock(mux.condition)
    try
        waiter.active = false
        if (@atomic client.request_mux) === mux && get(mux.waiters, token, nothing) === waiter
            delete!(mux.waiters, token)
        end
        if isempty(mux.waiters)
            empty!(mux.deadline_queue)
            _notify_request_timeout_task_locked(mux)
        else
            _compact_request_deadline_queue_locked!(mux)
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
            _wait_until_notified_locked(mux.condition; cancel_token) do
                waiter.ready
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
    st = status(client)
    _ensure_request_publish_ready(client, st)
    _validate_publish_frame_for_client(client, request_frame)
    mux = _ensure_request_mux(client; cancel_token)
    token, waiter = _register_request_waiter!(client, mux, timeout)
    reply = waiter.reply
    try
        _validate_publish_subject(reply)
        frame = _PublishFrame(request_frame.subject, reply, request_frame.payload, request_frame.headers)
        _publish_frame_unchecked(client, frame; buffer_on_reconnect=false, force_flush=true,
                                 cancel_token, validate_frame=false)
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
