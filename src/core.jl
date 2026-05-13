function _ensure_usable_for_publish(client::Client)
    st = status(client)
    st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
    st == ConnectionStatus.DRAINING && throw(ConnectionDrainingError())
    st == ConnectionStatus.DISCONNECTED && throw(ConnectionClosedError("connection is disconnected"))
    nothing
end

function _send_raw(client::Client, data::Union{AbstractString,Vector{UInt8}}; buffer_on_reconnect::Bool=false)
    st = status(client)
    if st in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING)
        try
            _write_raw(client, data)
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
    elseif st == ConnectionStatus.DRAINING
        throw(ConnectionDrainingError())
    else
        throw(ConnectionClosedError())
    end
    nothing
end

function _write_publish(client::Client, frame::PublishFrame)
    @lock client.write_lock begin
        io = @lock client.lock client.write_io
        isnothing(io) && throw(ConnectionClosedError("connection transport is closed"))
        _write_pub_frame(io, frame)
        flush(io)
    end
    nothing
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

function _send_publish(client::Client, frame::PublishFrame; buffer_on_reconnect::Bool=true)
    st = status(client)
    if st in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING)
        try
            _write_publish(client, frame)
        catch err
            if st == ConnectionStatus.CONNECTED && _recover_after_write_failure!(client, err)
                if buffer_on_reconnect
                    _enqueue_pending(client, frame)
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
    sid, subject, queue, max_msgs = @lock sub.client.lock (sub.sid, sub.subject, sub.queue, sub.max_msgs)
    try
        _write_raw(sub.client, _sub_cmd(subject, queue, sid))
        if max_msgs > 0
            _write_raw(sub.client, _unsub_cmd(sid, max_msgs))
        end
        @lock sub.client.lock sub.server_active = true
        return true
    catch err
        if _recover_after_write_failure!(sub.client, err)
            @lock sub.client.lock sub.server_active = false
            return false
        end
        rethrow()
    end
end

function _close_subscription_channel!(errors::Vector, sub::Subscription)
    if isopen(sub.messages)
        _close_resource!(errors, "close subscription channel $(sub.sid)", sub.messages)
    end
    errors
end

_pending_size(data::Vector{UInt8}) = length(data)
_pending_size(data::AbstractString) = ncodeunits(data)
_pending_size(frame::PublishFrame) = _serialized_size(frame)

_write_pending(io::IO, data::Union{AbstractString,Vector{UInt8}}) = write(io, data)
_write_pending(io::IO, frame::PublishFrame) = _write_pub_frame(io, frame)

function _enqueue_pending(client::Client, data)
    @lock client.lock begin
        bytes = _pending_size(data)
        projected = client.pending_bytes + bytes
        if projected > client.options.pending_size
            throw(OutboundBufferLimitError(client.options.pending_size, projected))
        end
        _write_pending(client.pending, data)
        client.pending_bytes = projected
    end
end

function _publish(client::Client, subject::AbstractString, data=nothing; reply::Union{String,Nothing}=nothing,
                  headers::Union{Headers,Nothing}=nothing, buffer_on_reconnect::Bool=true)
    _ensure_usable_for_publish(client)
    subject = _validate_publish_subject(subject)
    !isnothing(reply) && _validate_publish_subject(reply)
    payload = _payload_bytes(data)
    hdr = isnothing(headers) ? EMPTY_BYTES : _headers_bytes(headers)
    frame = PublishFrame(subject, reply, payload, hdr)
    total = _pub_payload_size(frame)
    max_payload = @lock client.lock something(client.info.max_payload, typemax(Int))
    total > max_payload && throw(MaxPayloadError(max_payload, total))
    _send_publish(client, frame; buffer_on_reconnect)
    _record_out!(client, length(payload))
    nothing
end

function publish(client::Client, subject::AbstractString, data=nothing; reply::Union{String,Nothing}=nothing, headers::Union{Headers,Nothing}=nothing)
    _publish(client, subject, data; reply, headers)
end

function _subscribe(client::Client, subject::AbstractString; queue::Union{String,Nothing}=nothing, callback::Union{Function,Nothing}=nothing,
                    max_msgs::Int=0, pending_msgs_limit::Int=client.options.sub_pending_msgs_limit,
                    pending_bytes_limit::Int=client.options.sub_pending_bytes_limit, auto_ack::Bool=false,
                    _control_handler::_SubscriptionControlHandler=_NoSubscriptionControlHandler(),
                    require_connected::Bool=false)
    subject = _validate_subject(subject)
    queue = _validate_queue(queue)
    send_now = false
    sub = @lock client.lock begin
        st = client.status
        require_connected && st != ConnectionStatus.CONNECTED && _throw_not_connected_for_request(st)
        st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
        st == ConnectionStatus.DRAINING && throw(ConnectionDrainingError())
        st == ConnectionStatus.DISCONNECTED && throw(ConnectionClosedError("connection is disconnected"))
        client.sid += 1
        sid = client.sid
        ch = Channel{Msg}(pending_msgs_limit)
        sub = Subscription(client, sid, subject, queue, callback, ch, _control_handler, pending_msgs_limit, pending_bytes_limit, 0, 0, max_msgs, false, nothing, auto_ack, false, 0)
        client.subscriptions[sid] = sub
        send_now = st == ConnectionStatus.CONNECTED
        sub
    end
    if send_now
        try
            _send_subscription_now!(sub)
        catch err
            cleanup_errors = Any[]
            @lock client.lock begin
                delete!(client.subscriptions, sub.sid)
                sub.closed = true
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

function subscribe(callback::Function, client::Client, subject::AbstractString; kwargs...)
    subscribe(client, subject; callback, kwargs...)
end

function _subscription_processor(sub::Subscription, callback::Callback) where {Callback}
    while true
        keep_running = @lock sub.client.lock !sub.closed
        (keep_running || isready(sub.messages)) || return
        msg = nothing
        try
            msg = take!(sub.messages)
        catch err
            err isa InvalidStateException && return
            _report_error(sub.client, err)
            continue
        end
        @lock sub.client.lock begin
            sub.pending_bytes = max(0, sub.pending_bytes - length(msg.data))
            sub.processing += 1
        end
        try
            callback(msg)
            sub.auto_ack && ack(msg)
        catch err
            _report_error(sub.client, err)
        finally
            @lock sub.client.lock sub.processing = max(0, sub.processing - 1)
        end
    end
end

_handle_subscription_control(::_NoSubscriptionControlHandler, _sub::Subscription, _msg::Msg)::Bool = false

function _request_mux_token(prefix::String, subject::String)::Union{String,Nothing}
    prefix_len = ncodeunits(prefix)
    subject_len = ncodeunits(subject)
    subject_len > prefix_len + 1 || return nothing
    startswith(subject, prefix) || return nothing
    codeunit(subject, prefix_len + 1) == UInt8('.') || return nothing
    token = SubString(subject, prefix_len + 2)
    isempty(token) && return nothing
    occursin('.', token) && return nothing
    String(token)
end

function _handle_subscription_control(::_RequestMuxControlHandler, sub::Subscription, msg::Msg)::Bool
    client = sub.client
    waiter = nothing
    drop = false
    @lock client.lock begin
        mux = client.request_mux
        if isnothing(mux) || mux.sub !== sub
            drop = true
        else
            token = _request_mux_token(mux.prefix, msg.subject)
            if isnothing(token)
                drop = true
            else
                waiter = pop!(mux.waiters, token, nothing)
                drop = isnothing(waiter)
            end
        end
        drop && (client.stats.dropped_msgs += 1)
    end
    if !isnothing(waiter)
        try
            put!(waiter, msg)
        catch err
            _report_error(client, CleanupError("deliver request reply $(msg.subject)", err))
        end
    end
    true
end

function _dispatch_msg(client::Client, msg::Msg)
    sub = @lock client.lock begin
        sub = get(client.subscriptions, msg.sid, nothing)
        if isnothing(sub) || sub.closed
            client.stats.dropped_msgs += 1
            nothing
        else
            sub
        end
    end
    isnothing(sub) && return

    control_handler = @lock client.lock sub.control_handler
    if _handle_subscription_control(control_handler, sub, msg)
        @lock client.lock begin
            client.stats.in_msgs += 1
            client.stats.in_bytes += length(msg.data)
        end
        return
    end

    should_close = false
    accepted = @lock client.lock begin
        if sub.closed
            client.stats.dropped_msgs += 1
            false
        elseif sub.pending_bytes + length(msg.data) > sub.pending_bytes_limit || Base.n_avail(sub.messages) >= sub.pending_msgs_limit
            client.stats.dropped_msgs += 1
            false
        else
            sub.received += 1
            client.stats.in_msgs += 1
            client.stats.in_bytes += length(msg.data)
            sub.pending_bytes += length(msg.data)
            should_close = sub.max_msgs > 0 && sub.received >= sub.max_msgs
            true
        end
    end
    if !accepted
        if !isnothing(sub) && (@lock client.lock !sub.closed)
            _report_error(client, SlowConsumerError(msg.subject, msg.sid, "subscription pending limits exceeded"))
        end
        return
    end
    try
        put!(sub.messages, msg)
    catch err
        @lock client.lock sub.pending_bytes = max(0, sub.pending_bytes - length(msg.data))
        if err isa InvalidStateException || (@lock client.lock sub.closed)
            _record_drop!(client)
            return
        end
        rethrow()
    end
    if should_close
        @lock client.lock begin
            delete!(client.subscriptions, sub.sid)
            sub.closed = true
        end
        errors = Any[]
        _close_subscription_channel!(errors, sub)
        _report_cleanup_errors(client, errors)
    end
end

function next(sub::Subscription; timeout::Real=1.0)
    (@lock sub.client.lock sub.closed) && !isready(sub.messages) && throw(ConnectionClosedError("subscription is closed"))
    result = timedwait(timeout; pollint=0.001) do
        isready(sub.messages) || (@lock sub.client.lock sub.closed)
    end
    result == :timed_out && throw(TimeoutError("next message timed out"))
    isready(sub.messages) || throw(ConnectionClosedError("subscription is closed"))
    msg = take!(sub.messages)
    @lock sub.client.lock sub.pending_bytes = max(0, sub.pending_bytes - length(msg.data))
    msg
end

function unsubscribe(sub::Subscription; max_msgs::Int=0)
    closed, active = @lock sub.client.lock (sub.closed, sub.server_active)
    closed && return nothing
    st = status(sub.client)
    if st == ConnectionStatus.CONNECTED && active
        try
            _write_raw(sub.client, _unsub_cmd(sub.sid, max_msgs))
        catch err
            _recover_after_write_failure!(sub.client, err) || rethrow()
            @lock sub.client.lock sub.server_active = false
        end
    elseif max_msgs > 0 && st in (ConnectionStatus.CLOSED, ConnectionStatus.DISCONNECTED)
        throw(ConnectionClosedError("connection is disconnected"))
    elseif max_msgs > 0 && st == ConnectionStatus.DRAINING
        throw(ConnectionDrainingError())
    end
    if max_msgs == 0
        @lock sub.client.lock begin
            delete!(sub.client.subscriptions, sub.sid)
            sub.closed = true
        end
        errors = Any[]
        _close_subscription_channel!(errors, sub)
        _throw_errors(errors)
    else
        @lock sub.client.lock sub.max_msgs = max_msgs
    end
    nothing
end

close(sub::Subscription) = unsubscribe(sub)

function drain(sub::Subscription; timeout::Real=sub.client.options.drain_timeout)
    closed, active = @lock sub.client.lock (sub.closed, sub.server_active)
    closed && return nothing
    status(sub.client) in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING) || throw(ConnectionReconnectingError())
    if active
        try
            _write_raw(sub.client, _unsub_cmd(sub.sid))
        catch err
            _recover_after_write_failure!(sub.client, err) || rethrow()
            throw(ConnectionReconnectingError())
        end
    end
    flush(sub.client; timeout)
    result = timedwait(timeout; pollint=0.01) do
        !isready(sub.messages) && (@lock sub.client.lock sub.processing == 0)
    end
    result == :timed_out && throw(TimeoutError("subscription drain timed out"))
    @lock sub.client.lock begin
        delete!(sub.client.subscriptions, sub.sid)
        sub.closed = true
    end
    errors = Any[]
    _close_subscription_channel!(errors, sub)
    _throw_errors(errors)
    nothing
end

function flush(client::Client; timeout::Real=10.0)
    st = status(client)
    st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
    st in (ConnectionStatus.RECONNECTING, ConnectionStatus.CONNECTING, ConnectionStatus.DISCONNECTED) && throw(ConnectionReconnectingError())
    ch = Channel{Bool}(1)
    waiter = PongWaiter(ch)
    @lock client.lock push!(client.pongs, waiter)
    try
        _send_raw(client, "PING$CRLF")
    catch err
        @lock client.lock filter!(x -> x !== waiter, client.pongs)
        rethrow()
    end
    result = timedwait(timeout; pollint=0.001) do
        isready(ch)
    end
    if result == :timed_out
        # Keep a bounded tombstone so a late PONG is consumed by its original
        # flush and cannot make a later flush appear complete.
        @lock client.lock begin
            waiter.channel = nothing
            _trim_stale_pong_waiters_locked!(client)
        end
        throw(TimeoutError("flush timed out"))
    end
    take!(ch) || throw(status(client) == ConnectionStatus.CLOSED ? ConnectionClosedError() : ConnectionReconnectingError())
    nothing
end

ping(client::Client; timeout::Real=10.0) = flush(client; timeout)

function drain(client::Client; timeout::Real=client.options.drain_timeout)
    @lock client.lock begin
        client.status == ConnectionStatus.CLOSED && return nothing
        client.status == ConnectionStatus.CONNECTED || throw(ConnectionReconnectingError())
        client.status = ConnectionStatus.DRAINING
    end
    errors = Any[]
    subs = @lock client.lock collect(values(client.subscriptions))
    for sub in subs
        try
            drain(sub; timeout)
        catch err
            push!(errors, err)
            _report_error(client, err)
        end
    end
    try
        flush(client; timeout)
    catch err
        push!(errors, err)
        _report_error(client, err)
    end
    try
        close(client; throw_errors=true)
    catch err
        push!(errors, err)
    end
    _throw_errors(errors)
    nothing
end

function close(client::Client; throw_errors::Bool=false)
    already = false
    subs = Subscription[]
    @lock client.lock begin
        already = client.status == ConnectionStatus.CLOSED
        if !already
            client.status = ConnectionStatus.CLOSED
            client.generation += 1
            subs = collect(values(client.subscriptions))
            empty!(client.subscriptions)
            for sub in subs
                sub.closed = true
            end
        end
    end
    already && return nothing
    errors = Any[]
    append!(errors, _notify_pong_waiters!(client, false))
    append!(errors, _notify_request_waiters!(client, ConnectionClosedError(); clear_mux=true))
    for sub in subs
        _close_subscription_channel!(errors, sub)
    end
    append!(errors, _close_transport(_take_transport!(client)...))
    append!(errors, _stop_client_tasks!(client; timeout=min(5.0, max(0.5, client.options.connect_timeout + 0.2))))
    for sub in subs
        _wait_task!(errors, "stop subscription processor $(sub.sid)", sub.processor)
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

function new_inbox(client::Client; prefix::String=client.options.inbox_prefix)
    suffix = @lock client.lock randstring(client.rng, NUID_ALPHABET, 22)
    "$prefix.$suffix"
end

function _request_mux_active_locked(client::Client)
    mux = client.request_mux
    if isnothing(mux) || mux.sub.closed || !mux.sub.server_active
        return nothing
    end
    mux
end

function _close_inactive_request_mux_subscription!(client::Client, sub::Subscription)
    errors = Any[]
    @lock client.lock begin
        if get(client.subscriptions, sub.sid, nothing) === sub
            delete!(client.subscriptions, sub.sid)
        end
        sub.closed = true
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
        active = @lock client.lock client.status == ConnectionStatus.CONNECTED && !sub.closed && sub.server_active
        if !active
            _close_inactive_request_mux_subscription!(client, sub)
            throw(ConnectionReconnectingError())
        end

        mux = RequestMux(prefix, sub, Dict{String,Channel{Any}}())
        assigned = @lock client.lock begin
            if client.status == ConnectionStatus.CONNECTED && !sub.closed && sub.server_active
                client.request_mux = mux
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

function _register_request_waiter!(client::Client, mux::RequestMux)
    @lock client.lock begin
        client.status == ConnectionStatus.CONNECTED || _throw_not_connected_for_request(client.status)
        client.request_mux === mux && !mux.sub.closed && mux.sub.server_active || throw(ConnectionReconnectingError())
        while true
            token = randstring(client.rng, NUID_ALPHABET, 22)
            if !haskey(mux.waiters, token)
                ch = Channel{Any}(1)
                mux.waiters[token] = ch
                return token, ch
            end
        end
    end
end

function _remove_request_waiter!(client::Client, mux::RequestMux, token::String, ch::Channel{Any})
    @lock client.lock begin
        if client.request_mux === mux && get(mux.waiters, token, nothing) === ch
            delete!(mux.waiters, token)
        end
    end
    nothing
end

function _wait_request_reply(ch::Channel{Any}, timeout::Real)
    result = timedwait(timeout; pollint=0.001) do
        isready(ch)
    end
    result == :timed_out && throw(TimeoutError("request timed out"))
    value = take!(ch)
    value isa Exception && throw(value)
    value::Msg
end

function _request_raw(client::Client, subject::AbstractString, data=nothing; timeout::Real=1.0, headers::Union{Headers,Nothing}=nothing)
    mux = _ensure_request_mux(client)
    token, ch = _register_request_waiter!(client, mux)
    reply = "$(mux.prefix).$token"
    try
        _publish(client, subject, data; reply, headers, buffer_on_reconnect=false)
        return _wait_request_reply(ch, timeout)
    finally
        _remove_request_waiter!(client, mux, token, ch)
    end
end

function request(client::Client, subject::AbstractString, data=nothing; timeout::Real=1.0, headers::Union{Headers,Nothing}=nothing)
    msg = _request_raw(client, subject, data; timeout, headers)
    code = _status_header(msg)
    if code == 503
        throw(NoRespondersError(String(subject)))
    elseif !isnothing(code) && code >= 400
        throw(ProtocolError("request failed with status $code $(_status_description(msg))"))
    end
    msg
end
