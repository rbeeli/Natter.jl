function _validate_subscription_limits(max_msgs, pending_msgs_limit, pending_bytes_limit)
    max_msgs = _validate_core_max_msgs(max_msgs)
    pending_msgs_limit = _positive_integer_option("pending_msgs_limit", pending_msgs_limit)
    pending_bytes_limit = _positive_integer_option("pending_bytes_limit", pending_bytes_limit)
    max_msgs, pending_msgs_limit, pending_bytes_limit
end

function _subscription_inline_callback(callback, borrowed::Bool, callback_mode)::Bool
    callback_mode isa Symbol ||
        throw(ArgumentError("callback_mode must be :task or :inline"))
    if callback_mode === :task
        borrowed && throw(ArgumentError("borrowed=true requires callback_mode=:inline"))
        return false
    elseif callback_mode === :inline
        isnothing(callback) &&
            throw(ArgumentError("callback_mode=:inline requires a callback"))
        return true
    end
    throw(ArgumentError("callback_mode must be :task or :inline"))
end

struct _SubscriptionProcessor{S<:Subscription,F}
    sub::S
    callback::F
end

(processor::_SubscriptionProcessor)() =
    _subscription_processor(processor.sub, processor.callback)

function _start_subscription_processor!(sub::Subscription, callback::Callback) where {Callback}
    processor = _SubscriptionProcessor(sub, callback)
    sub.processor = _spawn_work(:subscription_processor) do
        processor()
    end
    nothing
end

function _subscribe_unlocked(client::Client, subject::AbstractString; queue::Union{AbstractString,Nothing}=nothing, callback=nothing,
                             borrowed::Bool=false,
                             callback_mode=(borrowed ? :inline : :task),
                             max_msgs=0, pending_msgs_limit=client.options.sub_pending_msgs_limit,
                             pending_bytes_limit=client.options.sub_pending_bytes_limit,
                             _control_handler::_SubscriptionControlHandler=_NoSubscriptionControlHandler(),
                             require_connected::Bool=false,
                             cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    subject = _validate_subject(subject)
    queue = _validate_queue(queue)
    borrowed = _connect_option_bool("borrowed", borrowed)
    inline_callback = _subscription_inline_callback(callback, borrowed, callback_mode)
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
        sub = Subscription(client, sid, subject, queue, callback, inline_callback, sub_lock, ch,
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
    if !isnothing(callback) && !inline_callback
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
                (!isready(sub.messages) || sub.processing == 0 || sub.closed) &&
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
            was_empty = !isready(sub.messages)
            sub.received += 1
            sub.pending_bytes += msg_bytes
            put!(sub.messages, msg)
            was_empty && _notify_subscription_waiters_locked(sub)
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
        if sub.closed || sub.timeout_queue.active > 0
            _notify_subscription_waiters_locked(sub; all=true)
        end
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

_borrowed_dispatch_msg(msg::_BorrowedDispatchMsg) = msg

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
    handler = sub.borrowed_callback_handler
    dispatch_owned = false
    should_close = false
    accepted_fast = false
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
            if control_handler isa _NoSubscriptionControlHandler && sub.borrowed_callback &&
               sub.has_callback
                handler = sub.borrowed_callback_handler
                sub.received += 1
                sub.processing += 1
                accepted_fast = true
            end
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

    if accepted_fast
        _record_in!(client, msg_bytes)
        _run_borrowed_callback(client, sub, handler, msg)
        should_close && _close_subscription_locally!(sub; throw_errors=false)
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
    sub.has_callback && throw(ArgumentError("take! requires a subscription without a callback"))
    nothing
end

function Base.take!(sub::Subscription; timeout::Real=1.0,
                    cancel_token::MaybeCancellationToken=nothing)
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
                ready = _wait_subscription_condition_locked(sub, _remaining_timeout(deadline);
                                                           cancel_token) do
                    isready(sub.messages) || sub.closed
                end
                ready ? :retry : :timeout
            end
        end
        wait_result === :closed && throw(ConnectionClosedError("subscription is closed"))
        wait_result === :timeout && throw(TimeoutError("take! timed out"))
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
