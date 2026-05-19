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
        mux.timeout_task = _spawn_control(:request_timeout) do
            _request_timeout_loop(client, mux)
        end
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
