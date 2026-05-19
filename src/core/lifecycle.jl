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
        _wait_subscription_condition_locked(sub, _remaining_timeout(deadline); cancel_token) do
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
    _notify_client_lifecycle_watchers!(client, ConnectionClosedError())
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
    try
        _deactivate_writer_queue!(client; close=true, clear=true)
        _signal_writer(client)
    catch err
        push!(errors, CleanupError("signal writer", err))
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
