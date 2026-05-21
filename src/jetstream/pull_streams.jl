struct _PullStreamClosed <: Exception end

function _pull_stream_closed(state::_PullMessageStreamState)::Bool
    @lock state.lock state.closed
end

function _pull_stream_error(state::_PullMessageStreamState)
    @lock state.lock state.error
end

function _pull_stream_set_error!(state::_PullMessageStreamState, err)
    exception = err isa Exception ? err : ErrorException(string(err))
    @lock state.lock begin
        isnothing(state.error) && (state.error = exception)
        state.closed = true
        _pull_stream_clear_requests!(state)
    end
    nothing
end

function _pull_stream_close_state!(state::_PullMessageStreamState)
    @lock state.lock begin
        was_closed = state.closed
        state.closed = true
        _pull_stream_clear_requests!(state)
        was_closed
    end
end

function _pull_stream_threshold(name::AbstractString, value, limit::Int)::Int
    threshold = _positive_integer_option(name, value)
    threshold <= limit || throw(ArgumentError("$name must not exceed its request limit"))
    threshold
end

function _validate_pull_messages(psub::PullSubscription, batch, max_bytes, expires::Real,
                                 heartbeat::Union{Nothing,Real}, threshold_messages,
                                 threshold_bytes, channel_size, stop_after,
                                 min_pending, min_ack_pending, priority_group,
                                 priority)::Tuple{_PullStreamConfig,Int}
    batch = _positive_integer_option("messages batch", batch)
    max_bytes = _optional_positive_int_option("messages max_bytes", max_bytes)
    expires = _positive_timeout_seconds("messages expires", expires)
    heartbeat = _pull_fetch_heartbeat(expires, heartbeat)
    channel_size = _positive_integer_option("messages channel_size", channel_size)
    threshold_messages =
        isnothing(threshold_messages) ? max(1, min(batch, channel_size) ÷ 2) :
        _pull_stream_threshold("messages threshold_messages", threshold_messages, channel_size)
    if isnothing(max_bytes)
        isnothing(threshold_bytes) ||
            throw(ArgumentError("messages threshold_bytes requires max_bytes"))
        threshold_bytes = nothing
    else
        threshold_bytes =
            isnothing(threshold_bytes) ? max(1, max_bytes ÷ 2) :
            _pull_stream_threshold("messages threshold_bytes", threshold_bytes, max_bytes)
    end
    stop_after = _optional_positive_int_option("messages stop_after", stop_after)
    min_pending, min_ack_pending, priority_group, priority =
        _validate_pull_request_scheduling("messages", min_pending, min_ack_pending,
                                          priority_group, priority)
    _check_pull_subscription_open(psub)
    _validate_pull_request_priority!(psub, "messages", min_pending, min_ack_pending,
                                     priority_group, priority)
    _PullStreamConfig(batch, max_bytes, expires, heartbeat, threshold_messages,
                      threshold_bytes, min_pending, min_ack_pending, priority_group,
                      priority, stop_after, channel_size), channel_size
end

function _next_pull_stream_msg(stream::PullMessageStream, timeout::Real)
    sub = stream.delivery
    client = sub.client
    deadline = time() + Float64(timeout)
    while true
        _pull_stream_closed(stream.state) && throw(_PullStreamClosed())
        ready, msg = _take_subscription_msg_ready!(sub)
        ready && return msg

        closed, empty = @lock sub.lock (sub.closed, !isready(sub.messages))
        st = status(client)
        closed && empty && _throw_pull_fetch_wait_interrupted(closed, st)
        ready = @lock sub.lock begin
            _wait_subscription_condition_locked(sub, _remaining_timeout(deadline)) do
                isready(sub.messages) || sub.closed || status(client) != ConnectionStatus.CONNECTED ||
                    _pull_stream_closed(stream.state)
            end
        end
        ready || throw(TimeoutError("next message timed out"))
        _pull_stream_closed(stream.state) && throw(_PullStreamClosed())
        ready, msg = _take_subscription_msg_ready!(sub)
        ready && return msg
        closed = @lock sub.lock sub.closed
        st = status(client)
        (closed || st != ConnectionStatus.CONNECTED) && _throw_pull_fetch_wait_interrupted(closed, st)
    end
end

function _pull_stream_pending_messages(state::_PullMessageStreamState)::Int
    state.buffered_messages + state.requested_messages
end

function _pull_stream_pending_bytes(state::_PullMessageStreamState)::Int
    state.buffered_bytes + state.requested_bytes
end

function _pull_stream_request_batch(config::_PullStreamConfig,
                                    state::_PullMessageStreamState)::Int
    requested = state.requested_messages
    available = config.channel_size - state.buffered_messages - requested
    available > 0 || return 0
    batch = min(config.batch, available)
    if !isnothing(config.stop_after)
        remaining = (config.stop_after::Int) - state.delivered - requested
        remaining > 0 || return 0
        batch = min(batch, remaining)
    end
    batch
end

_pull_stream_reserved_bytes(max_bytes::Union{Int,Nothing})::Int =
    isnothing(max_bytes) ? 0 : max_bytes::Int

function _pull_stream_reserve_counters!(state::_PullMessageStreamState,
                                        request::_PullStreamRequest)
    state.requested_messages += request.remaining_messages
    state.requested_bytes += request.remaining_bytes
    nothing
end

function _pull_stream_release_counters!(state::_PullMessageStreamState,
                                        messages::Int, bytes::Int)
    state.requested_messages = max(0, state.requested_messages - messages)
    state.requested_bytes = max(0, state.requested_bytes - bytes)
    nothing
end

function _pull_stream_request_max_bytes(config::_PullStreamConfig,
                                        state::_PullMessageStreamState)::Union{Int,Nothing}
    isnothing(config.max_bytes) && return nothing
    remaining = (config.max_bytes::Int) - _pull_stream_pending_bytes(state)
    remaining > 0 ? remaining : 0
end

function _pull_stream_should_refill(config::_PullStreamConfig,
                                    state::_PullMessageStreamState)::Bool
    _pull_stream_pending_messages(state) <= config.threshold_messages && return true
    !isnothing(config.threshold_bytes) &&
        _pull_stream_pending_bytes(state) <= (config.threshold_bytes::Int)
end

function _reserve_pull_stream_request!(config::_PullStreamConfig,
                                       state::_PullMessageStreamState)::Union{_PullStreamReservation,Nothing}
    @lock state.lock begin
        state.closed && return nothing
        _pull_stream_should_refill(config, state) || return nothing
        batch = _pull_stream_request_batch(config, state)
        batch > 0 || return nothing
        max_bytes = _pull_stream_request_max_bytes(config, state)
        max_bytes === 0 && return nothing
        request = _PullStreamRequest(batch, _pull_stream_reserved_bytes(max_bytes))
        push!(state.requests, request)
        _pull_stream_reserve_counters!(state, request)
        _PullStreamReservation(request, batch, max_bytes)
    end
end

function _pull_stream_request_index(requests::Vector{_PullStreamRequest},
                                    request::_PullStreamRequest)::Int
    @inbounds for i in eachindex(requests)
        requests[i] === request && return i
    end
    0
end

function _unreserve_pull_stream_request!(state::_PullMessageStreamState, request::_PullStreamRequest)
    @lock state.lock begin
        index = _pull_stream_request_index(state.requests, request)
        if index != 0
            deleteat!(state.requests, index)
            _pull_stream_release_counters!(state, request.remaining_messages, request.remaining_bytes)
        end
    end
    nothing
end

function _publish_pull_stream_request!(stream::PullMessageStream, config::_PullStreamConfig,
                                       state::_PullMessageStreamState)::Bool
    psub = stream.subscription
    @lock psub.fetch_lock begin
        reservation = _reserve_pull_stream_request!(config, state)
        isnothing(reservation) && return false
        heartbeat_ns = config.heartbeat > 0 ? _seconds_to_nanoseconds(config.heartbeat) : 0
        payload = _pull_fetch_request_payload!(stream.payload_buffer, reservation.batch,
                                               _seconds_to_nanoseconds(config.expires),
                                               heartbeat_ns, reservation.max_bytes, false, psub.pin_id,
                                               config.min_pending, config.min_ack_pending,
                                               config.priority_group, config.priority)
        try
            _publish_pull_fetch_request(psub, psub.next_subject, payload, stream.delivery.subject)
        catch
            _unreserve_pull_stream_request!(state, reservation.request)
            rethrow()
        end
    end
    true
end

function _pull_stream_msg_bytes(msg::AbstractMsg)::Int
    max(1, msg.header_bytes + length(msg.data))
end

function _pull_stream_decrement_requested!(state::_PullMessageStreamState, msg::Msg)
    bytes = _pull_stream_msg_bytes(msg)
    _pull_stream_release_counters!(state, 1, bytes)
    if !isempty(state.requests)
        request = first(state.requests)
        request.remaining_messages = max(0, request.remaining_messages - 1)
        request.remaining_bytes = max(0, request.remaining_bytes - bytes)
        request.remaining_messages == 0 && deleteat!(state.requests, 1)
    end
    nothing
end

function _pull_stream_clear_requests!(state::_PullMessageStreamState)
    empty!(state.requests)
    state.requested_messages = 0
    state.requested_bytes = 0
    nothing
end

function _pull_stream_pending_header_value(msg::Msg, name::AbstractString)::Union{Int,Nothing}
    value = header(msg, name)
    (isnothing(value) || isempty(value)) && return nothing
    parsed = tryparse(Int, value)
    isnothing(parsed) && throw(ProtocolError("invalid $name header: $value"))
    parsed >= 0 || throw(ProtocolError("invalid $name header: $value"))
    parsed
end

_pull_stream_pending_header(msg::Msg, name::AbstractString)::Int =
    something(_pull_stream_pending_header_value(msg, name), 0)

function _pull_stream_status_pending(msg::Msg)::Tuple{Int,Int}
    _pull_stream_pending_header(msg, "Nats-Pending-Messages"),
        _pull_stream_pending_header(msg, "Nats-Pending-Bytes")
end

function _pull_stream_release_terminal_request!(state::_PullMessageStreamState,
                                                msg::Msg)::Bool
    isempty(state.requests) && return false
    request = popfirst!(state.requests)
    messages = something(_pull_stream_pending_header_value(msg, "Nats-Pending-Messages"),
                         request.remaining_messages)
    bytes = something(_pull_stream_pending_header_value(msg, "Nats-Pending-Bytes"),
                      request.remaining_bytes)
    _pull_stream_release_counters!(state, messages, bytes)
    true
end

function _pull_stream_maybe_refill!(stream::PullMessageStream, config::_PullStreamConfig=stream.config)
    _publish_pull_stream_request!(stream, config, stream.state)
    nothing
end

function _pull_stream_put!(stream::PullMessageStream, msg::Msg)::Int
    bytes = _pull_stream_msg_bytes(msg)
    @lock stream.message_lock begin
        while isopen(stream.messages) && length(stream.messages) >= _queue_capacity(stream.messages)
            _pull_stream_closed(stream.state) && throw(_PullStreamClosed())
            wait(stream.message_condition)
        end
        isopen(stream.messages) || throw(_PullStreamClosed())
        @lock stream.state.lock begin
            stream.state.closed && throw(_PullStreamClosed())
            put!(stream.messages, msg)
            _pull_stream_decrement_requested!(stream.state, msg)
            stream.state.buffered_messages += 1
            stream.state.buffered_bytes += bytes
            stream.state.delivered += 1
        end
        notify(stream.message_condition)
    end
    1
end

function _pull_stream_loop(stream::PullMessageStream, config::_PullStreamConfig)
    psub = stream.subscription
    local_timeout = config.expires + min(config.expires * 0.1, 5.0)
    heartbeat_deadline = config.heartbeat > 0 ? time() + 2 * config.heartbeat : Inf
    try
        while !_pull_stream_closed(stream.state)
            if !isnothing(config.stop_after) &&
               (@lock stream.state.lock stream.state.delivered >= (config.stop_after::Int))
                break
            end
            _pull_stream_maybe_refill!(stream, config)
            wait_deadline = min(time() + local_timeout, heartbeat_deadline)
            msg = try
                _next_pull_stream_msg(stream, max(0.001, wait_deadline - time()))
            catch err
                if err isa TimeoutError
                    if config.heartbeat > 0 && time() >= heartbeat_deadline
                        throw(_jetstream_heartbeat_error())
                    end
                    @lock stream.state.lock _pull_stream_clear_requests!(stream.state)
                    continue
                elseif err isa _PullStreamClosed
                    break
                end
                rethrow()
            end

            action, err = _jetstream_status_action(msg, psub.next_subject)
            if action != :message
                has_request = @lock stream.state.lock !isempty(stream.state.requests)
                has_request || continue
                config.heartbeat > 0 && (heartbeat_deadline = time() + 2 * config.heartbeat)
                pin_id = header(msg, "Nats-Pin-Id")
                !isnothing(pin_id) && !isempty(pin_id) && (@lock psub.fetch_lock psub.pin_id = pin_id)
                if action in (:idle_heartbeat, :flow_control, :control)
                    continue
                elseif action in (:no_messages, :timeout, :batch_completed, :max_bytes_exceeded)
                    released = @lock stream.state.lock _pull_stream_release_terminal_request!(stream.state, msg)
                    released || continue
                    _pull_stream_maybe_refill!(stream, config)
                    continue
                else
                    action == :consumer_deleted && (@lock psub.close_lock psub.server_deleted = true)
                    action == :pin_id_mismatch && (@lock psub.fetch_lock psub.pin_id = nothing)
                    throw(err)
                end
            end

            config.heartbeat > 0 && (heartbeat_deadline = time() + 2 * config.heartbeat)
            pin_id = header(msg, "Nats-Pin-Id")
            !isnothing(pin_id) && !isempty(pin_id) && (@lock psub.fetch_lock psub.pin_id = pin_id)
            _pull_stream_put!(stream, msg)
            _pull_stream_maybe_refill!(stream, config)
        end
    catch err
        err isa _PullStreamClosed && return nothing
        if _pull_stream_closed(stream.state) && err isa InvalidStateException
            return nothing
        end
        _pull_stream_set_error!(stream.state, err)
        rethrow()
    finally
        try
            _close_pull_delivery!(psub, stream.delivery, "close pull stream delivery subscription")
        catch cleanup_err
            _pull_stream_set_error!(stream.state, cleanup_err)
        end
        _pull_stream_close_state!(stream.state)
        @lock stream.message_lock begin
            isopen(stream.messages) && close(stream.messages)
            notify(stream.message_condition; all=true)
        end
        _end_pull_stream!(psub)
    end
    nothing
end

function messages(psub::PullSubscription{C}; batch=100, max_bytes=nothing,
                  expires::Real=30.0, heartbeat::Union{Nothing,Real}=nothing,
                  threshold_messages=nothing, threshold_bytes=nothing,
                  channel_size=batch, stop_after=nothing,
                  min_pending=nothing, min_ack_pending=nothing,
                  priority_group=nothing, priority=nothing,
                  cancel_token::MaybeCancellationToken=nothing) where {C}
    _throw_if_cancelled(cancel_token)
    config, channel_size = _validate_pull_messages(psub, batch, max_bytes, expires, heartbeat,
                                                   threshold_messages, threshold_bytes,
                                                   channel_size, stop_after,
                                                   min_pending, min_ack_pending,
                                                   priority_group, priority)
    _begin_pull_stream!(psub)
    delivery = nothing
    try
        delivery_batch = _saturating_add_int(channel_size, config.batch)
        delivery = _subscribe_pull_delivery!(psub, delivery_batch, config.max_bytes; cancel_token)
        state = _PullMessageStreamState()
        queue_lock = ReentrantLock()
        queue_condition = Base.Threads.Condition(queue_lock)
        queue = MsgQueue{Msg}(channel_size)
        stream = PullMessageStream{C,typeof(psub)}(psub, delivery, queue, queue_lock,
                                                   queue_condition, config, UInt8[],
                                                   Task(() -> nothing), nothing, state)
        task = _spawn_control(:pull_stream) do
            _pull_stream_loop(stream, config)
        end
        stream.task = task
        stream
    catch err
        cleanup_error = nothing
        if !isnothing(delivery)
            try
                _close_pull_delivery!(psub, delivery, "close pull stream delivery subscription")
            catch cleanup_err
                cleanup_error = cleanup_err
            end
        end
        _end_pull_stream!(psub)
        isnothing(cleanup_error) || throw(Base.CompositeException([err, cleanup_error]))
        rethrow()
    end
end

function _pull_consume_callback_loop(stream::PullMessageStream, callback)
    try
        for msg in stream
            callback(msg)
        end
    catch err
        _pull_stream_set_error!(stream.state, err)
        close(stream)
        rethrow()
    end
    nothing
end

function consume(callback, psub::PullSubscription; kwargs...)
    stream = messages(psub; kwargs...)
    stream.callback_task = _spawn_work(:pull_consume_callback) do
        _pull_consume_callback_loop(stream, callback)
    end
    stream
end

function Base.close(stream::PullMessageStream; timeout::Real=stream.subscription.js.timeout,
                    cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    timeout = _positive_timeout_seconds("timeout", timeout)
    deadline = time() + timeout
    already_closed = _pull_stream_close_state!(stream.state)
    errors = Any[]
    if !already_closed
        try
            _close_pull_delivery!(stream.subscription, stream.delivery,
                                  "close pull stream delivery subscription")
        catch err
            push!(errors, err)
        end
    end
    @lock stream.message_lock begin
        isopen(stream.messages) && close(stream.messages)
        notify(stream.message_condition; all=true)
    end
    _wait_task!(errors, "stop pull message stream $(stream.subscription.consumer)",
                stream.task; timeout, interrupt=true, deadline)
    callback_task = stream.callback_task
    if !isnothing(callback_task) && callback_task !== current_task()
        _wait_task!(errors, "stop pull consume callback $(stream.subscription.consumer)",
                    callback_task; timeout, interrupt=true, deadline)
    end
    err = _pull_stream_error(stream.state)
    isnothing(err) || push!(errors, err)
    _throw_errors(errors)
    nothing
end

function _pull_stream_take_ready_locked(stream::PullMessageStream)::Union{Msg,Nothing}
    isready(stream.messages) || return nothing
    msg = take!(stream.messages)
    @lock stream.state.lock begin
        stream.state.buffered_messages = max(0, stream.state.buffered_messages - 1)
        stream.state.buffered_bytes = max(0, stream.state.buffered_bytes - _pull_stream_msg_bytes(msg))
    end
    notify(stream.message_condition)
    msg
end

function _wait_pull_stream_message_locked(stream::PullMessageStream,
                                          timeout::Float64,
                                          cancel_token::MaybeCancellationToken)::Bool
    if isinf(timeout) && isnothing(cancel_token)
        while !isready(stream.messages) && isopen(stream.messages)
            wait(stream.message_condition)
        end
        return isready(stream.messages)
    end

    if isinf(timeout)
        _wait_until_notified_locked(stream.message_condition; cancel_token) do
            isready(stream.messages) || !isopen(stream.messages)
        end
        return isready(stream.messages)
    end

    deadline = time() + timeout
    while !isready(stream.messages) && isopen(stream.messages)
        remaining = _remaining_timeout(deadline)
        remaining > 0 || return false
        ready = _wait_until_condition_locked(stream.message_condition, remaining;
                                             cancel_token) do
            isready(stream.messages) || !isopen(stream.messages)
        end
        ready || return false
    end
    isready(stream.messages)
end

function _take_pull_stream(stream::PullMessageStream, wait_timeout::Float64,
                           cancel_token::MaybeCancellationToken)
    while true
        msg = nothing
        wait_result = @lock stream.message_lock begin
            ready = _wait_pull_stream_message_locked(stream, wait_timeout, cancel_token)
            if ready
                msg = _pull_stream_take_ready_locked(stream)
                isnothing(msg) ? :retry : :ready
            elseif isopen(stream.messages)
                :timeout
            else
                :closed
            end
        end
        wait_result === :retry && continue
        if wait_result === :ready
            jsmsg = JetStreamMsg(msg::Msg, stream.subscription.js.client)
            try
                _pull_stream_maybe_refill!(stream)
            catch err
                _pull_stream_set_error!(stream.state, err)
                _notify_subscription_waiters!(stream.delivery; all=true)
                @lock stream.message_lock begin
                    isopen(stream.messages) && close(stream.messages)
                    notify(stream.message_condition; all=true)
                end
            end
            return jsmsg
        end
        wait_result === :timeout && throw(TimeoutError("take! timed out"))
        err = _pull_stream_error(stream.state)
        isnothing(err) || throw(err)
        throw(InvalidStateException("pull message stream is closed", :closed))
    end
end

function Base.take!(stream::PullMessageStream; timeout::Real=Inf,
                    cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    wait_timeout = _connect_option_positive_or_infinite_float("timeout", timeout)
    _take_pull_stream(stream, wait_timeout, cancel_token)
end

function Base.iterate(stream::PullMessageStream, state=nothing)
    try
        take!(stream), nothing
    catch err
        queue_closed = @lock stream.message_lock !isopen(stream.messages)
        err isa InvalidStateException && queue_closed && return nothing
        rethrow()
    end
end

function Base.wait(stream::PullMessageStream)
    try
        wait(stream.task)
    catch err
        err isa TaskFailedException && err.task === stream.task || rethrow()
    end
    task = stream.callback_task
    if !isnothing(task)
        try
            wait(task)
        catch err
            err isa TaskFailedException && err.task === task || rethrow()
        end
    end
    err = _pull_stream_error(stream.state)
    isnothing(err) || throw(err)
    stream
end

Base.fetch(stream::PullMessageStream) = wait(stream)
Base.isopen(stream::PullMessageStream) =
    (@lock stream.message_lock isopen(stream.messages)) && !_pull_stream_closed(stream.state)
