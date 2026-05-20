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

function _write_pub_frame_buffered(io::BufferedWriteIO, frame::_AbstractPublishFrame,
                                   scratch::Vector{UInt8}, contiguous_threshold::Int,
                                   frame_size::Int)
    if contiguous_threshold > 0 && frame_size <= contiguous_threshold
        wire = _cached_pub_wire(frame)
        isnothing(wire) ? write(io, _pub_cmd!(scratch, frame)) : write(io, wire)
    else
        write(io, _pub_prefix!(scratch, frame))
        isempty(frame.headers) || write(io, frame.headers)
        write(io, frame.payload)
        write(io, CRLF)
    end
    nothing
end

function _buffer_publish_frame(client::Client, io::BufferedWriteIO, frame::_AbstractPublishFrame,
                               replayable::Bool, frame_size::Int,
                               scratch::Vector{UInt8}, contiguous_threshold::Int)
    _ensure_open(io)
    start = position(io.buffer) + 1
    entry_count = length(io.replayable_entries)
    replayable_bytes = io.replayable_bytes
    if replayable
        _reserve_pending_bytes!(client, frame_size)
        try
            _write_pub_frame_buffered(io, frame, scratch, contiguous_threshold, frame_size)
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
            _write_pub_frame_buffered(io, frame, scratch, contiguous_threshold, frame_size)
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
            if _buffered_bytes(io) > 0 || _replayable_bytes(io) > 0
                _flush_write_io(client, io)
            end
            transport = _underlying_transport(io)
            attempted = true
            _write_pub_frame_direct_timed(client, transport, frame; force_flush=true)
            return false, attempted
        end

        had_buffered = _buffered_bytes(io) > 0
        captured = _buffer_publish_frame(client, io, frame, replayable, frame_size,
                                         client.write_scratch,
                                         client.options.direct_write_threshold)
        attempted = _should_flush_write_io(client, io, force_flush)
        _flush_or_signal_locked(client, io, force_flush, had_buffered)
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
            _writer_barrier!(client; cancel_token)
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

function _send_publish_no_replay(client::Client, frame::_AbstractPublishFrame;
                                 force_flush::Bool=false,
                                 frame_size::Int=_serialized_size(frame),
                                 payload_size::Int=_pub_payload_size(frame),
                                 direct_write::Bool=false,
                                 cancel_token::MaybeCancellationToken=nothing)
    try
        if !force_flush && !direct_write &&
           _enqueue_writer_publish(client, frame, frame_size; cancel_token)
            return nothing
        end
        _writer_queue_pending(client) && _writer_barrier!(client; cancel_token)
        _write_publish(client, frame; force_flush, replayable=false, frame_size,
                       direct_write, payload_size, validate_frame=false,
                       cancel_token)
    catch err
        failure = err isa _PublishWriteFailure ? err : _PublishWriteFailure(err, false, false)
        cause = failure.cause
        cause isa CancelledError && throw(cause)
        cause isa OutboundBufferLimitError && throw(cause)
        if failure.attempted && _recover_after_write_failure!(client, cause)
            throw(ConnectionReconnectingError())
        end
        throw(cause)
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

function _resolve_publish_mode(client::Client, mode::Nothing)::PublishMode.T
    client.options.publish_mode
end

_resolve_publish_mode(_client::Client, mode::PublishMode.T)::PublishMode.T = mode

function _resolve_publish_mode(_client::Client, _mode)::PublishMode.T
    throw(ArgumentError("publish mode must be a PublishMode value"))
end

function _publish_mode_behavior(mode::PublishMode.T)::Tuple{Bool,Bool}
    mode == PublishMode.REPLAYABLE && return true, false
    mode == PublishMode.DIRECT && return false, true
    mode == PublishMode.QUEUED && return false, false
    throw(ArgumentError("unknown publish mode"))
end

function _publish_prepared(client::Client, frame::_AbstractPublishFrame; force_flush::Bool=false,
                           mode=nothing,
                           cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    buffer_on_reconnect, direct_write = _publish_mode_behavior(_resolve_publish_mode(client, mode))
    payload_size = _pub_payload_size(frame)
    frame_size = _serialized_size(frame)
    st = status(client)
    _ensure_usable_status_for_publish(st)
    _validate_publish_frame_for_client(client, frame, payload_size)
    if buffer_on_reconnect
        _send_publish(client, frame; buffer_on_reconnect, force_flush, frame_size,
                      payload_size, st, direct_write, validate_frame=false, cancel_token)
    else
        _send_publish_no_replay(client, frame; force_flush, frame_size, payload_size,
                                direct_write, cancel_token)
    end
    _record_out!(client, payload_size)
    nothing
end

function _publish(client::Client, subject::AbstractString, data=nothing; reply::Union{AbstractString,Nothing}=nothing,
                  headers=nothing, force_flush::Bool=false,
                  mode=nothing,
                  cancel_token::MaybeCancellationToken=nothing)
    _publish_prepared(client, _publish_frame(subject, reply, data, headers);
                      force_flush, mode, cancel_token)
end

function _publish_frame_unchecked(client::Client, frame::_AbstractPublishFrame; force_flush::Bool=false,
                                  mode=nothing,
                                  cancel_token::MaybeCancellationToken=nothing,
                                  validate_frame::Bool=true)
    _throw_if_cancelled(cancel_token)
    buffer_on_reconnect, direct_write = _publish_mode_behavior(_resolve_publish_mode(client, mode))
    frame_size = _serialized_size(frame)
    payload_size = _pub_payload_size(frame)
    st = status(client)
    _ensure_usable_status_for_publish(st)
    validate_frame && _validate_publish_frame_for_client(client, frame, payload_size)
    if buffer_on_reconnect
        _send_publish(client, frame; buffer_on_reconnect, force_flush, frame_size,
                      payload_size, st, direct_write, validate_frame=false, cancel_token)
    else
        _send_publish_no_replay(client, frame; force_flush, frame_size, payload_size,
                                direct_write, cancel_token)
    end
    _record_out!(client, payload_size)
    nothing
end

function _publish_unchecked(client::Client, subject::String, payload::AbstractVector{UInt8};
                            force_flush::Bool=false,
                            mode=nothing,
                            cancel_token::MaybeCancellationToken=nothing)
    _publish_frame_unchecked(client, _publish_frame(subject, nothing, payload, EMPTY_BYTES);
                             force_flush, mode, cancel_token)
end

function publish(client::Client, subject::AbstractString, data=nothing; reply::Union{AbstractString,Nothing}=nothing,
                 headers=nothing, mode=nothing,
                 cancel_token::MaybeCancellationToken=nothing)
    _publish(client, subject, data; reply, headers, mode, cancel_token)
end

function publish(client::Client, frame::PublishFrame; mode=nothing,
                 cancel_token::MaybeCancellationToken=nothing)
    _publish_prepared(client, frame; mode, cancel_token)
end

function respond(client::Client, msg::AbstractMsg, data=nothing; headers=nothing,
                 mode=nothing,
                 cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    reply = msg.reply
    isnothing(reply) && throw(ArgumentError("message has no reply subject"))
    publish(client, reply, data; headers, mode, cancel_token)
end
