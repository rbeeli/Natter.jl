
function _js_async_publish_subscription_active(sub::Union{Subscription,Nothing})::Bool
    isnothing(sub) && return false
    @lock sub.lock !sub.closed && sub.server_active
end

function _js_async_publish_connection_error(client::Client, generation::Int)
    st = status(client)
    st == ConnectionStatus.CLOSED && return ConnectionClosedError()
    st == ConnectionStatus.DISCONNECTED && return ConnectionClosedError("connection is disconnected")
    st in (ConnectionStatus.CONNECTING, ConnectionStatus.RECONNECTING) && return ConnectionReconnectingError()
    _load_generation(client) == generation || return ConnectionReconnectingError()
    nothing
end

function _js_async_publish_result(future::JetStreamPublishFuture, msg::Msg)::Union{PubAck,Exception}
    try
        code = _status_header(msg)
        if code == _JS_STATUS_NO_RESPONDERS
            return NoRespondersError(future.subject)
        elseif !isnothing(code) && code >= 400
            return ProtocolError("request failed with status $code $(_status_description(msg))")
        end
        return _js_read_puback(msg)
    catch err
        return err
    end
end

function _resolve_js_publish_future_locked!(future::JetStreamPublishFuture{C},
                                            value::Union{PubAck,Exception})::Bool where {C<:Client}
    future.active || return false
    state = future.state::JetStreamAsyncPublishState{C}
    future.value = value
    future.ready = true
    future.active = false
    if get(state.futures, future.token, nothing) === future
        delete!(state.futures, future.token)
        state.pending = max(0, state.pending - 1)
        if isempty(state.futures)
            empty!(state.deadline_queue)
            empty!(state.retry_queue)
            _notify_js_async_publish_timeout_task_locked(state)
        else
            _compact_js_async_publish_deadline_queue_locked!(state)
            _compact_js_async_publish_retry_queue_locked!(state)
        end
    end
    notify(state.condition; all=true)
    true
end

function _resolve_js_publish_future!(future::JetStreamPublishFuture{C},
                                     value::Union{PubAck,Exception}) where {C<:Client}
    state = future.state::JetStreamAsyncPublishState{C}
    lock(state.condition)
    try
        _resolve_js_publish_future_locked!(future, value)
    finally
        unlock(state.condition)
    end
    nothing
end

function _clear_js_async_publish_pending!(state::JetStreamAsyncPublishState{C},
                                          err::Exception) where {C<:Client}
    lock(state.condition)
    try
        if isempty(state.futures)
            empty!(state.deadline_queue)
            empty!(state.retry_queue)
            _notify_js_async_publish_timeout_task_locked(state)
            return nothing
        end
        state.pending = 0
        for future in values(state.futures)
            if future.active
                future.value = err
                future.ready = true
                future.active = false
            end
        end
        empty!(state.futures)
        empty!(state.deadline_queue)
        empty!(state.retry_queue)
        notify(state.condition; all=true)
        _notify_js_async_publish_timeout_task_locked(state)
    finally
        unlock(state.condition)
    end
    nothing
end

_client_lifecycle_error!(state::JetStreamAsyncPublishState, err::Exception) =
    _clear_js_async_publish_pending!(state, err)

function _handle_js_async_publish_ack(state::JetStreamAsyncPublishState,
                                      token::Union{Int,Nothing}, msg::Msg)
    if isnothing(token)
        _record_drop!(state.client)
        return nothing
    end
    future = lock(state.condition) do
        get(state.futures, token, nothing)
    end
    if isnothing(future)
        _record_drop!(state.client)
        return nothing
    end
    result = _js_async_publish_result(future, msg)
    result isa NoRespondersError && _retry_js_async_publish!(future) && return nothing
    _resolve_js_publish_future!(future, result)
    nothing
end

function _handle_js_async_publish_ack(state::JetStreamAsyncPublishState, msg::Msg)
    token = _request_mux_token(state.prefix, msg.subject)
    _handle_js_async_publish_ack(state, token, msg)
end

function _handle_js_async_publish_ack_parts(state::JetStreamAsyncPublishState,
                                            token::Union{Int,Nothing}, reply,
                                            payload::Vector{UInt8}, headers::HeaderStorage,
                                            sid::Int, header_bytes::Int)
    if isnothing(token)
        _record_drop!(state.client)
        return nothing
    end
    future = lock(state.condition) do
        get(state.futures, token, nothing)
    end
    if isnothing(future)
        _record_drop!(state.client)
        return nothing
    end
    msg = Msg(future.reply, reply, payload, headers, sid, header_bytes)
    result = _js_async_publish_result(future, msg)
    result isa NoRespondersError && _retry_js_async_publish!(future) && return nothing
    _resolve_js_publish_future!(future, result)
    nothing
end

function _try_handle_subscription_msg_control(handler::_JetStreamAsyncPublishControlHandler,
                                              client::Client, sub::Subscription,
                                              reader::ProtocolReader, subject_start::Int,
                                              subject_end::Int, reply_start::Int,
                                              reply_end::Int, size::Int)::Bool
    state = handler.state
    token = _request_mux_token(state.prefix, reader.buffer, subject_start, subject_end)
    reply, payload = _fast_control_msg_parts(reader, reply_start, reply_end, size)
    _handle_js_async_publish_ack_parts(state, token, reply, payload, nothing, sub.sid, 0)
    _record_in!(client, size)
    true
end

function _try_handle_subscription_hmsg_control(handler::_JetStreamAsyncPublishControlHandler,
                                               client::Client, sub::Subscription,
                                               reader::ProtocolReader, subject_start::Int,
                                               subject_end::Int, reply_start::Int,
                                               reply_end::Int, hsize::Int, total::Int)::Bool
    state = handler.state
    token = _request_mux_token(state.prefix, reader.buffer, subject_start, subject_end)
    hdrs, reply, payload = _fast_control_hmsg_parts(reader, reply_start, reply_end, hsize, total)
    _handle_js_async_publish_ack_parts(state, token, reply, payload, hdrs, sub.sid, hsize)
    _record_in!(client, total)
    true
end

function _handle_subscription_control(handler::_JetStreamAsyncPublishControlHandler,
                                      _sub::Subscription, msg::Msg)::Bool
    _handle_js_async_publish_ack(handler.state, msg)
    true
end

function _retry_js_async_publish!(future::JetStreamPublishFuture{C})::Bool where {C<:Client}
    state = future.state::JetStreamAsyncPublishState{C}
    lock(state.condition)
    try
        future.active || return false
        future.retries < future.retry_attempts || return false
        now = time()
        remaining = future.deadline - now
        remaining > 0 || return false
        retry_frame = future.retry_frame
        isnothing(retry_frame) && return false
        future.retries += 1
        future.retry_deadline = now + min(future.retry_wait, remaining)
        wake_timeout = _deadline_queue_push!(state.retry_queue, future.token,
                                             future.retry_deadline, future)
        _ensure_js_async_publish_timeout_task_locked!(state)
        wake_timeout && _notify_js_async_publish_timeout_task_locked(state)
        return true
    finally
        unlock(state.condition)
    end
end

function _ensure_js_async_publish_timeout_task_locked!(state::JetStreamAsyncPublishState)
    task = state.timeout_task
    if isnothing(task) || istaskdone(task)
        state.timeout_task = _spawn_control(:jetstream_publish_timeout) do
            _js_async_publish_timeout_loop(state)
        end
    end
    nothing
end

function _js_async_publish_deadline_entry_valid_locked(state::JetStreamAsyncPublishState,
                                                       entry::_DeadlineEntry)::Bool
    future = get(state.futures, entry.token, nothing)
    future === entry.value && future.active && future.deadline == entry.deadline
end

function _next_js_async_publish_deadline_entry_locked!(state::JetStreamAsyncPublishState)
    while true
        entry = _deadline_queue_peek(state.deadline_queue)
        isnothing(entry) && return nothing
        _js_async_publish_deadline_entry_valid_locked(state, entry) && return entry
        _deadline_queue_pop!(state.deadline_queue)
    end
end

function _rebuild_js_async_publish_deadline_queue_locked!(state::JetStreamAsyncPublishState{C}) where {C<:Client}
    queue = _DeadlineQueue{JetStreamPublishFuture{C,JetStreamAsyncPublishState{C}}}()
    for (token, future) in state.futures
        future.active && _deadline_queue_push!(queue, token, future.deadline, future)
    end
    state.deadline_queue = queue
    nothing
end

function _compact_js_async_publish_deadline_queue_locked!(state::JetStreamAsyncPublishState)
    _deadline_queue_compaction_due(state.deadline_queue, length(state.futures)) ||
        return nothing
    _rebuild_js_async_publish_deadline_queue_locked!(state)
end

function _js_async_publish_retry_entry_valid_locked(state::JetStreamAsyncPublishState,
                                                   entry::_DeadlineEntry)::Bool
    future = get(state.futures, entry.token, nothing)
    future === entry.value && future.active && future.retry_deadline == entry.deadline
end

function _next_js_async_publish_retry_entry_locked!(state::JetStreamAsyncPublishState)
    while true
        entry = _deadline_queue_peek(state.retry_queue)
        isnothing(entry) && return nothing
        _js_async_publish_retry_entry_valid_locked(state, entry) && return entry
        _deadline_queue_pop!(state.retry_queue)
    end
end

function _rebuild_js_async_publish_retry_queue_locked!(state::JetStreamAsyncPublishState{C}) where {C<:Client}
    queue = _DeadlineQueue{JetStreamPublishFuture{C,JetStreamAsyncPublishState{C}}}()
    for (token, future) in state.futures
        future.active && isfinite(future.retry_deadline) &&
            _deadline_queue_push!(queue, token, future.retry_deadline, future)
    end
    state.retry_queue = queue
    nothing
end

function _compact_js_async_publish_retry_queue_locked!(state::JetStreamAsyncPublishState)
    _deadline_queue_compaction_due(state.retry_queue, length(state.futures)) ||
        return nothing
    _rebuild_js_async_publish_retry_queue_locked!(state)
end

function _notify_js_async_publish_timeout_task_locked(state::JetStreamAsyncPublishState)
    notify(state.timeout_condition; all=true)
    nothing
end

function _wait_js_async_publish_timeout_locked!(state::JetStreamAsyncPublishState, delay::Float64)
    if !isfinite(delay)
        wait(state.timeout_condition)
        return nothing
    end
    timer = Timer(min(max(delay, 0.0), _MAX_TIMER_DELAY_SECONDS)) do _
        lock(state.timeout_condition)
        try
            notify(state.timeout_condition; all=true)
        finally
            unlock(state.timeout_condition)
        end
    end
    try
        wait(state.timeout_condition)
    finally
        close(timer)
    end
    nothing
end

function _js_async_publish_timeout_loop(state::JetStreamAsyncPublishState{C}) where {C<:Client}
    lock(state.condition)
    try
        while true
            if isempty(state.futures)
                empty!(state.deadline_queue)
                empty!(state.retry_queue)
                state.timeout_task = nothing
                return nothing
            end

            deadline_entry = _next_js_async_publish_deadline_entry_locked!(state)
            if isnothing(deadline_entry)
                _rebuild_js_async_publish_deadline_queue_locked!(state)
                deadline_entry = _next_js_async_publish_deadline_entry_locked!(state)
                if isnothing(deadline_entry)
                    empty!(state.futures)
                    empty!(state.deadline_queue)
                    empty!(state.retry_queue)
                    state.pending = 0
                    state.timeout_task = nothing
                    return nothing
                end
            end

            retry_entry = _next_js_async_publish_retry_entry_locked!(state)
            use_retry = !isnothing(retry_entry) && retry_entry.deadline <= deadline_entry.deadline
            entry = use_retry ? retry_entry : deadline_entry
            future = entry.value

            err = _js_async_publish_connection_error(state.client, future.generation)
            if !isnothing(err)
                use_retry ? _deadline_queue_pop!(state.retry_queue) :
                            _deadline_queue_pop!(state.deadline_queue)
                future = get(state.futures, entry.token, nothing)
                if future === entry.value && future.active &&
                   (use_retry ? future.retry_deadline == entry.deadline :
                    future.deadline == entry.deadline)
                    _resolve_js_publish_future_locked!(future, err)
                end
                continue
            end

            delay = entry.deadline - time()
            if delay > 0
                _wait_js_async_publish_timeout_locked!(state, delay)
            elseif use_retry
                _deadline_queue_pop!(state.retry_queue)
                future = get(state.futures, entry.token, nothing)
                if future === entry.value && future.active &&
                   future.retry_deadline == entry.deadline
                    future.retry_deadline = Inf
                    retry_frame = future.retry_frame
                    isnothing(retry_frame) && continue
                    publish_err::Union{Exception,Nothing} = nothing
                    unlock(state.condition)
                    try
                        try
                            _publish_frame_unchecked(state.client, retry_frame;
                                                     mode=PublishMode.QUEUED)
                        catch err
                            publish_err = err
                        end
                    finally
                        lock(state.condition)
                    end
                    isnothing(publish_err) ||
                        _resolve_js_publish_future_locked!(future, publish_err)
                end
            else
                _deadline_queue_pop!(state.deadline_queue)
                future = get(state.futures, entry.token, nothing)
                if future === entry.value && future.active && future.deadline == entry.deadline
                    _resolve_js_publish_future_locked!(future, TimeoutError("JetStream async publish ack timed out"))
                end
            end
        end
    finally
        unlock(state.condition)
    end
end

function _ensure_js_async_publish_subscription!(state::JetStreamAsyncPublishState{C};
                                                cancel_token::MaybeCancellationToken=nothing) where {C<:Client}
    _throw_if_cancelled(cancel_token)
    sub = lock(state.condition) do
        state.sub
    end
    _js_async_publish_subscription_active(sub) && return sub::Subscription{C}

    @lock state.setup_lock begin
        _throw_if_cancelled(cancel_token)
        sub = lock(state.condition) do
            state.sub
        end
        _js_async_publish_subscription_active(sub) && return sub::Subscription{C}
        _ensure_connected_for_request(state.client)

        if !isnothing(sub)
            closed = @lock sub.lock sub.closed
            if !closed
                _send_subscription_now!(sub; cancel_token) || throw(ConnectionReconnectingError())
                _js_async_publish_subscription_active(sub) && return sub::Subscription{C}
                throw(ConnectionReconnectingError())
            end
        end

        sub = _subscribe(state.client, "$(state.prefix).*";
                         _control_handler=_JetStreamAsyncPublishControlHandler(state),
                         pending_msgs_limit=state.max_pending,
                         pending_bytes_limit=state.client.options.sub_pending_bytes_limit,
                         require_connected=true,
                         cancel_token)
        lock(state.condition)
        try
            state.sub = sub
            notify(state.condition; all=true)
        finally
            unlock(state.condition)
        end
        sub
    end
end

function _next_js_async_publish_token!(state::JetStreamAsyncPublishState)::Int
    start = state.next_token
    while true
        token = state.next_token == typemax(Int) ? 1 : state.next_token + 1
        state.next_token = token
        !haskey(state.futures, token) && return token
        token == start && throw(OverflowError("JetStream async publish token space exhausted"))
    end
end

function _reserve_js_async_publish_future!(state::JetStreamAsyncPublishState{C},
                                           subject::AbstractString, deadline::Float64,
                                           generation::Int,
                                           retry_attempts::Int, retry_wait::Float64,
                                           retry_frame::Union{_AbstractPublishFrame,Nothing},
                                           cancel_token::MaybeCancellationToken) where {C<:Client}
    lock(state.condition)
    try
        while state.pending >= state.max_pending
            err = _js_async_publish_connection_error(state.client, generation)
            isnothing(err) || throw(err)
            remaining = deadline - time()
            remaining > 0 || throw(TimeoutError("JetStream async publish backpressure timed out"))
            ready = _wait_condition_timeout_queue_locked(state.condition, state.wait_queue,
                                                         remaining; cancel_token) do
                state.pending < state.max_pending ||
                    !isnothing(_js_async_publish_connection_error(state.client, generation))
            end
            ready || throw(TimeoutError("JetStream async publish backpressure timed out"))
        end

        token = _next_js_async_publish_token!(state)
        reply = string(state.prefix, '.', token)
        future = JetStreamPublishFuture{C,typeof(state)}(
            state, token, reply, String(subject), deadline, generation, retry_attempts,
            retry_wait, 0, Inf, retry_frame, false, true, nothing)
        state.futures[token] = future
        state.pending += 1
        wake_timeout = _deadline_queue_push!(state.deadline_queue, token, deadline, future)
        _ensure_js_async_publish_timeout_task_locked!(state)
        wake_timeout && _notify_js_async_publish_timeout_task_locked(state)
        future
    finally
        unlock(state.condition)
    end
end

function _wait_js_publish_future_value(future::JetStreamPublishFuture,
                                       cancel_token::MaybeCancellationToken)
    state = future.state
    lock(state.condition)
    value = try
        while !future.ready
            remaining = future.deadline - time()
            if remaining <= 0
                _resolve_js_publish_future_locked!(future, TimeoutError("JetStream async publish ack timed out"))
                break
            end
            _wait_until_notified_locked(state.condition; cancel_token) do
                future.ready
            end
        end
        future.value
    finally
        unlock(state.condition)
    end
    value isa Exception && throw(value)
    future
end

function _wait_js_publish_future_value(future::JetStreamPublishFuture,
                                       timeout::Float64,
                                       cancel_token::MaybeCancellationToken)
    wait_deadline = time() + timeout
    state = future.state
    lock(state.condition)
    value = try
        while !future.ready
            now = time()
            remaining = future.deadline - now
            if remaining <= 0
                _resolve_js_publish_future_locked!(future, TimeoutError("JetStream async publish ack timed out"))
                break
            end
            wait_remaining = wait_deadline - now
            wait_remaining > 0 ||
                throw(TimeoutError("JetStream async publish future wait timed out"))
            ready = _wait_condition_timeout_queue_locked(state.condition, state.wait_queue,
                                                         min(remaining, wait_remaining);
                                                         cancel_token) do
                future.ready
            end
            ready || continue
        end
        future.value
    finally
        unlock(state.condition)
    end
    value isa Exception && throw(value)
    future
end

function Base.wait(future::JetStreamPublishFuture; timeout::Real=Inf,
                   cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    wait_timeout = _connect_option_positive_or_infinite_float("timeout", timeout)
    if isinf(wait_timeout)
        return _wait_js_publish_future_value(future, cancel_token)
    end
    _wait_js_publish_future_value(future, wait_timeout, cancel_token)
end

function Base.fetch(future::JetStreamPublishFuture; timeout::Real=Inf,
                    cancel_token::MaybeCancellationToken=nothing)
    wait(future; timeout, cancel_token)
    future.value::PubAck
end

function Base.isready(future::JetStreamPublishFuture)
    state = future.state
    lock(state.condition)
    try
        future.ready
    finally
        unlock(state.condition)
    end
end

Base.show(io::IO, future::JetStreamPublishFuture) =
    print(io, "JetStreamPublishFuture(", future.subject, ", ",
          isready(future) ? "ready" : "pending", ")")

function js_publish_future(js::JetStreamContext, subject::AbstractString, data=nothing; timeout::Real=js.timeout,
                          stream::Union{AbstractString,Nothing}=nothing, headers=nothing,
                          expected_stream::Union{AbstractString,Nothing}=nothing, msg_id=nothing,
                          expected_last_sequence=nothing, expected_last_subject_sequence=nothing,
                          expected_last_subject=nothing, expected_last_msg_id=nothing, ttl=nothing,
                          schedule=nothing, schedule_at=nothing, schedule_every=nothing,
                          schedule_target=nothing, schedule_source=nothing, schedule_ttl=nothing,
                          schedule_timezone=nothing,
                          retry_attempts::Integer=DEFAULT_JS_PUBLISH_RETRY_ATTEMPTS,
                          retry_wait::Real=DEFAULT_JS_PUBLISH_RETRY_WAIT,
                          cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    timeout = _positive_timeout_seconds("timeout", timeout)
    attempts = _nonnegative_integer_option("retry_attempts", retry_attempts)
    wait_seconds = _js_publish_retry_wait(retry_wait)
    hdrs = _js_publish_headers(headers; stream, expected_stream, msg_id, expected_last_sequence,
                               expected_last_subject_sequence, expected_last_subject,
                               expected_last_msg_id, ttl, schedule, schedule_at, schedule_every,
                               schedule_target, schedule_source, schedule_ttl, schedule_timezone)
    frame = _publish_frame(subject, nothing, data, hdrs)
    _validate_publish_frame_for_client(js.client, frame)
    _ensure_js_async_publish_subscription!(js.publish_futures; cancel_token)

    deadline = time() + timeout
    generation = _load_generation(js.client)
    future = _reserve_js_async_publish_future!(js.publish_futures, frame.subject, deadline, generation,
                                               attempts, wait_seconds, nothing, cancel_token)
    publish_frame = _PublishFrame(frame.subject, future.reply, frame.payload, frame.headers)
    if attempts > 0
        future.retry_frame = publish_frame
    end
    try
        _publish_frame_unchecked(js.client, publish_frame; mode=PublishMode.QUEUED,
                                 cancel_token)
    catch err
        _resolve_js_publish_future!(future, err)
        rethrow()
    end
    future
end

function js_publish_future_pending(js::JetStreamContext)::Int
    lock(js.publish_futures.condition)
    try
        js.publish_futures.pending
    finally
        unlock(js.publish_futures.condition)
    end
end

function js_publish_future_complete(js::JetStreamContext; timeout::Real=js.timeout,
                                     cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    timeout = _positive_timeout_seconds("timeout", timeout)
    deadline = time() + timeout
    state = js.publish_futures
    lock(state.condition)
    try
        while state.pending > 0
            remaining = deadline - time()
            remaining > 0 || throw(TimeoutError("JetStream async publish completion timed out"))
            ready = _wait_condition_timeout_queue_locked(state.condition, state.wait_queue,
                                                         remaining; cancel_token) do
                state.pending == 0
            end
            ready || throw(TimeoutError("JetStream async publish completion timed out"))
        end
    finally
        unlock(state.condition)
    end
    nothing
end
