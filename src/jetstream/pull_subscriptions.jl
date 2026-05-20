function _pull_fetch_request_payload!(buffer::Vector{UInt8}, batch::Int, expires_ns::Int,
                                      heartbeat_ns::Int, max_bytes::Union{Int,Nothing},
                                      no_wait::Bool, pin_id::Union{String,Nothing},
                                      min_pending::Union{Int,Nothing},
                                      min_ack_pending::Union{Int,Nothing},
                                      priority_group::Union{String,Nothing},
                                      priority::Union{Int,Nothing})::Vector{UInt8}
    empty!(buffer)
    _append_ascii!(buffer, "{\"batch\":")
    _append_json_int!(buffer, batch)
    if !isnothing(max_bytes)
        _append_ascii!(buffer, ",\"max_bytes\":")
        _append_json_int!(buffer, max_bytes)
    end
    if expires_ns > 0
        _append_ascii!(buffer, ",\"expires\":")
        _append_json_int!(buffer, expires_ns)
    end
    if heartbeat_ns > 0
        _append_ascii!(buffer, ",\"idle_heartbeat\":")
        _append_json_int!(buffer, heartbeat_ns)
    end
    no_wait && _append_ascii!(buffer, ",\"no_wait\":true")
    if !isnothing(min_pending)
        _append_ascii!(buffer, ",\"min_pending\":")
        _append_json_int!(buffer, min_pending)
    end
    if !isnothing(min_ack_pending)
        _append_ascii!(buffer, ",\"min_ack_pending\":")
        _append_json_int!(buffer, min_ack_pending)
    end
    if !isnothing(priority_group)
        _append_ascii!(buffer, ",\"group\":")
        _append_json_string!(buffer, priority_group)
    end
    if !isnothing(priority)
        _append_ascii!(buffer, ",\"priority\":")
        _append_json_int!(buffer, priority)
    end
    if !isnothing(pin_id)
        _append_ascii!(buffer, ",\"id\":")
        _append_json_string!(buffer, pin_id)
    end
    push!(buffer, UInt8('}'))
    buffer
end

mutable struct _PullMessageStreamState
    lock::ReentrantLock
    closed::Bool
    error::Union{Exception,Nothing}
    requests::Vector{_PullStreamRequest}
    requested_messages::Int
    requested_bytes::Int
    delivered::Int
    buffered_messages::Int
    buffered_bytes::Int
end

_PullMessageStreamState() = _PullMessageStreamState(ReentrantLock(), false, nothing,
                                                    _PullStreamRequest[], 0, 0, 0, 0, 0)

mutable struct PullSubscription{C<:Client,J<:JetStreamContext{C}}
    js::J
    stream::String
    consumer::String
    next_subject::String
    fetch_lock::ReentrantLock
    fetch_payload_buffer::Vector{UInt8}
    close_lock::ReentrantLock
    delete_on_close::Bool
    closed::Bool
    server_deleted::Bool
    pin_id::Union{String,Nothing}
    active_fetches::Int
    active_stream::Bool
    active_deliveries::Vector{Subscription{C}}
    fetch_delivery::Union{Subscription{C},Nothing}
    fetch_delivery_in_use::Bool
    priority_policy::Union{PriorityPolicy.T,Nothing}
    priority_groups::Vector{String}
end

mutable struct PullMessageStream{C<:Client,P<:PullSubscription{C}}
    subscription::P
    delivery::Subscription{C}
    messages::MsgQueue{Msg}
    message_lock::ReentrantLock
    message_condition::Base.GenericCondition{ReentrantLock}
    config::_PullStreamConfig
    payload_buffer::Vector{UInt8}
    task::Task
    callback_task::Union{Task,Nothing}
    state::_PullMessageStreamState
end

function PullSubscription(js::J, stream::AbstractString, consumer::AbstractString,
                          fetch_lock::ReentrantLock, close_lock::ReentrantLock,
                          delete_on_close::Bool, closed::Bool,
                          priority_policy::Union{PriorityPolicy.T,Nothing}=nothing,
                          priority_groups::Vector{String}=String[]) where {C<:Client,J<:JetStreamContext{C}}
    stream = String(stream)
    consumer = String(consumer)
    PullSubscription{C,J}(js, stream, consumer, _pull_fetch_next_subject(js, stream, consumer),
                          fetch_lock, UInt8[], close_lock, delete_on_close, closed, false, nothing,
                          0, false, Subscription{C}[], nothing, false,
                          priority_policy, copy(priority_groups))
end

function _stream_by_subject(js::JetStreamContext, subject::AbstractString; timeout::Real=js.timeout,
                            cancel_token::MaybeCancellationToken=nothing)
    subject = _validate_subject(subject)
    names = stream_names(js; subject, timeout, cancel_token)
    isempty(names) && throw(JetStreamError(404, nothing, "no stream found for subject $subject"))
    first(names)
end

function pull_subscribe(js::JetStreamContext, subject::AbstractString; stream::Union{AbstractString,Nothing}=nothing, durable::Union{AbstractString,Nothing}=nothing,
                        config::Union{ConsumerConfig,AbstractDict{String,<:Any}}=ConsumerConfig(),
                        timeout::Real=js.timeout, cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    subject = _validate_subject(subject)
    timeout = _positive_timeout_seconds("timeout", timeout)
    verify_config = config isa ConsumerConfig
    cfg = _js_config_payload(config)
    _validate_pull_consumer_config!(cfg)
    _validate_consumer_config_payload!(cfg)

    stream = isnothing(stream) ? _stream_by_subject(js, subject; timeout, cancel_token) : _validate_api_name("stream", stream)
    durable = isnothing(durable) ? nothing : _validate_api_name("consumer", durable)
    bind_fields = Set{String}(keys(cfg))
    if !_consumer_has_filter(cfg)
        cfg["filter_subject"] = String(subject)
        push!(bind_fields, "filter_subject")
    end
    if !isnothing(durable)
        _set_config_default!(cfg, "name", durable)
        _set_config_default!(cfg, "durable_name", durable)
        push!(bind_fields, "durable_name")
    end
    bind_name = _consumer_bind_name(cfg)
    delete_on_close = isnothing(bind_name)
    info =
        if isnothing(bind_name)
            _set_config_default!(cfg, "name", @lock js.client.lock randstring(js.client.rng, 16))
            _consumer_create_payload_request(js, stream, cfg; timeout, action="create",
                                             verify_config, cancel_token)
        else
            consumer, _created = _bind_or_create_consumer(js, stream, bind_name, cfg, bind_fields;
                                                          timeout, verify_config, cancel_token)
            _validate_existing_pull_consumer(consumer)
        end
    if !haskey(cfg, "name") || isnothing(cfg["name"])
        cfg["name"] = info.name
    end
    try
        priority_policy, priority_groups = _validate_pull_consumer_priority_config(info)
        PullSubscription(js, stream, info.name, ReentrantLock(), ReentrantLock(),
                         delete_on_close, false, priority_policy, priority_groups)
    catch err
        if delete_on_close
            try
                consumer_delete(js, stream, info.name; timeout, cancel_token)
            catch cleanup_err
                throw(Base.CompositeException([err, CleanupError("delete pull consumer $(info.name)", cleanup_err)]))
            end
        end
        rethrow()
    end
end

function _pull_fetch_heartbeat(expires::Real, heartbeat::Union{Nothing,Real})::Float64
    hb =
        if isnothing(heartbeat)
            expires >= 10 ? 5.0 : 0.0
        else
            heartbeat isa Bool && throw(ArgumentError("fetch heartbeat must be a non-negative number of seconds"))
            Float64(heartbeat)
        end
    hb >= 0 || throw(ArgumentError("fetch heartbeat must be a non-negative number of seconds"))
    hb == 0 && return 0.0
    expires >= 2 * hb || throw(ArgumentError("fetch expires must be at least twice the heartbeat"))
    hb
end

function _pull_fetch_default_expires(timeout::Real)::Float64
    ttl = Float64(timeout)
    ttl - min(ttl * 0.1, 5.0)
end

function _optional_positive_int_option(name::AbstractString, value)::Union{Int,Nothing}
    isnothing(value) && return nothing
    _positive_integer_option(name, value)
end

function _optional_pull_priority(value)::Union{Int,Nothing}
    isnothing(value) && return nothing
    _integer_range_option("pull priority", value, 0, 9, "an integer from 0 to 9")
end

function _validate_pull_priority_group(value)::Union{String,Nothing}
    isnothing(value) && return nothing
    group = _validate_queue(value)
    group
end

function _validate_pull_request_scheduling(prefix::AbstractString, min_pending, min_ack_pending,
                                           priority_group, priority)
    min_pending = _optional_positive_int_option("$prefix min_pending", min_pending)
    min_ack_pending = _optional_positive_int_option("$prefix min_ack_pending", min_ack_pending)
    priority_group = _validate_pull_priority_group(priority_group)
    priority = _optional_pull_priority(priority)
    if isnothing(priority_group) &&
       (!isnothing(min_pending) || !isnothing(min_ack_pending) || !isnothing(priority))
        throw(ArgumentError("$prefix priority_group is required with min_pending, min_ack_pending, or priority"))
    end
    min_pending, min_ack_pending, priority_group, priority
end

function _validate_pull_request_priority!(psub::PullSubscription, prefix::AbstractString,
                                          min_pending::Union{Int,Nothing},
                                          min_ack_pending::Union{Int,Nothing},
                                          priority_group::Union{String,Nothing},
                                          priority::Union{Int,Nothing})
    has_groups = !isempty(psub.priority_groups)
    if has_groups
        isnothing(priority_group) &&
            throw(ArgumentError("$prefix priority_group is required for priority consumer $(psub.consumer)"))
        priority_group in psub.priority_groups ||
            throw(ArgumentError("$prefix priority_group is not configured for consumer $(psub.consumer)"))
    elseif !isnothing(priority_group)
        throw(ArgumentError("$prefix priority_group is not supported for consumer $(psub.consumer)"))
    end

    if !isnothing(min_pending) || !isnothing(min_ack_pending)
        psub.priority_policy == PriorityPolicy.OVERFLOW ||
            throw(ArgumentError("$prefix min_pending and min_ack_pending require priority_policy=overflow"))
    end
    if !isnothing(priority)
        psub.priority_policy == PriorityPolicy.PRIORITIZED ||
            throw(ArgumentError("$prefix priority requires priority_policy=prioritized"))
    end
    nothing
end

function _bool_option(name::AbstractString, value)::Bool
    value isa Bool || throw(ArgumentError("$name must be a Bool"))
    value
end

function _check_pull_subscription_open(psub::PullSubscription)
    (@lock psub.close_lock psub.closed) && throw(ConnectionClosedError("pull subscription is closed"))
    nothing
end

function _begin_pull_fetch!(psub::PullSubscription)
    @lock psub.close_lock begin
        psub.closed && throw(ConnectionClosedError("pull subscription is closed"))
        psub.active_stream && throw(ArgumentError("pull subscription already has an active message stream"))
        psub.active_fetches += 1
    end
    nothing
end

function _end_pull_fetch!(psub::PullSubscription)
    @lock psub.close_lock begin
        psub.active_fetches = max(0, psub.active_fetches - 1)
    end
    nothing
end

function _begin_pull_stream!(psub::PullSubscription)
    @lock psub.close_lock begin
        psub.closed && throw(ConnectionClosedError("pull subscription is closed"))
        (psub.active_stream || psub.active_fetches > 0) &&
            throw(ArgumentError("pull subscription already has an active fetch or message stream"))
        psub.active_stream = true
    end
    nothing
end

function _end_pull_stream!(psub::PullSubscription)
    @lock psub.close_lock psub.active_stream = false
    nothing
end

function _validate_pull_fetch(psub::PullSubscription, batch, timeout::Real, expires::Real,
                              heartbeat::Union{Nothing,Real}, max_bytes, no_wait,
                              min_pending, min_ack_pending, priority_group, priority)
    batch = _positive_integer_option("fetch batch", batch)
    max_bytes = _optional_positive_int_option("fetch max_bytes", max_bytes)
    no_wait = _bool_option("fetch no_wait", no_wait)
    timeout = _positive_timeout_seconds("fetch timeout", timeout)
    expires = _positive_timeout_seconds("fetch expires", expires)
    timeout > expires || throw(ArgumentError("fetch timeout must be greater than expires"))
    min_pending, min_ack_pending, priority_group, priority =
        _validate_pull_request_scheduling("fetch", min_pending, min_ack_pending,
                                          priority_group, priority)
    _check_pull_subscription_open(psub)
    _validate_pull_request_priority!(psub, "fetch", min_pending, min_ack_pending,
                                     priority_group, priority)
    batch, timeout, expires, _pull_fetch_heartbeat(expires, heartbeat), max_bytes, no_wait,
        min_pending, min_ack_pending, priority_group, priority
end

function _throw_pull_fetch_wait_interrupted(closed::Bool, st::ConnectionStatus.T)
    st in (ConnectionStatus.RECONNECTING, ConnectionStatus.DISCONNECTED, ConnectionStatus.CONNECTING) &&
        throw(FetchDisconnectedError())
    st == ConnectionStatus.DRAINING && throw(ConnectionDrainingError())
    st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
    closed && throw(ConnectionClosedError("subscription is closed"))
    throw(TimeoutError("next message timed out"))
end

_saturating_add_int(a::Int, b::Int)::Int =
    a > typemax(Int) - b ? typemax(Int) : a + b

function _saturating_mul_int(a::Int, b::Int)::Int
    (a == 0 || b == 0) && return 0
    a > typemax(Int) ÷ b ? typemax(Int) : a * b
end

function _pull_delivery_pending_msgs_limit(client::Client, batch::Int)::Int
    requested = _saturating_add_int(batch, 8)
    min(max(requested, 8), client.options.sub_pending_msgs_limit)
end

function _pull_delivery_pending_bytes_limit(client::Client, pending_msgs_limit::Int,
                                            max_bytes::Union{Int,Nothing})::Int
    isnothing(max_bytes) && return client.options.sub_pending_bytes_limit
    header_budget = _saturating_mul_int(client.options.max_header_bytes, pending_msgs_limit)
    requested = _saturating_add_int(max_bytes::Int, header_budget)
    max(requested, client.options.sub_pending_bytes_limit)
end

function _pull_delivery_limits(client::Client, batch::Int,
                               max_bytes::Union{Int,Nothing})::Tuple{Int,Int}
    pending_msgs_limit = _pull_delivery_pending_msgs_limit(client, batch)
    pending_bytes_limit = _pull_delivery_pending_bytes_limit(client, pending_msgs_limit, max_bytes)
    pending_msgs_limit, pending_bytes_limit
end

function _pull_delivery_capacity_ok(sub::Subscription, pending_msgs_limit::Int,
                                    pending_bytes_limit::Int)::Bool
    sub.pending_msgs_limit >= pending_msgs_limit && sub.pending_bytes_limit >= pending_bytes_limit
end

function _pull_fetch_delivery_idle(sub::Subscription)::Bool
    @lock sub.lock !sub.closed && !isready(sub.messages) && sub.pending_bytes == 0
end

function _remove_active_pull_delivery_locked!(psub::PullSubscription, sub::Subscription)
    index = findfirst(active -> active === sub, psub.active_deliveries)
    isnothing(index) || deleteat!(psub.active_deliveries, index)
    nothing
end

function _register_pull_delivery!(psub::PullSubscription{C},
                                  sub::Subscription{C}) where {C<:Client}
    close_now = @lock psub.close_lock begin
        if psub.closed
            true
        else
            push!(psub.active_deliveries, sub)
            false
        end
    end
    if close_now
        closed_err = ConnectionClosedError("pull subscription is closed")
        try
            close(sub)
        catch cleanup_err
            throw(Base.CompositeException([closed_err,
                                           CleanupError("close pull delivery subscription", cleanup_err)]))
        end
        throw(closed_err)
    end
    nothing
end

function _unregister_pull_delivery!(psub::PullSubscription, sub::Subscription)
    @lock psub.close_lock begin
        _remove_active_pull_delivery_locked!(psub, sub)
    end
    nothing
end

function _subscribe_pull_delivery!(psub::PullSubscription{C}, batch::Int,
                                   max_bytes::Union{Int,Nothing};
                                   cancel_token::MaybeCancellationToken=nothing)::Subscription{C} where {C<:Client}
    client = psub.js.client
    pending_msgs_limit, pending_bytes_limit = _pull_delivery_limits(client, batch, max_bytes)
    sub = subscribe(client, new_inbox(client); pending_msgs_limit, pending_bytes_limit,
                    cancel_token)
    _register_pull_delivery!(psub, sub)
    sub
end

function _close_pull_delivery!(psub::PullSubscription, sub::Subscription,
                               label::AbstractString="close pull delivery subscription")
    _unregister_pull_delivery!(psub, sub)
    try
        close(sub)
    catch err
        throw(CleanupError(label, err))
    end
    nothing
end

function _acquire_pull_fetch_delivery!(psub::PullSubscription{C}, batch::Int,
                                       max_bytes::Union{Int,Nothing};
                                       cancel_token::MaybeCancellationToken=nothing)::Tuple{Subscription{C},Bool} where {C<:Client}
    client = psub.js.client
    pending_msgs_limit, pending_bytes_limit = _pull_delivery_limits(client, batch, max_bytes)
    retire::Union{Subscription{C},Nothing} = nothing
    cached::Union{Subscription{C},Nothing} = @lock psub.close_lock begin
        psub.closed && throw(ConnectionClosedError("pull subscription is closed"))
        sub = psub.fetch_delivery
        if !psub.fetch_delivery_in_use && !isnothing(sub)
            if _pull_delivery_capacity_ok(sub, pending_msgs_limit, pending_bytes_limit)
                psub.fetch_delivery_in_use = true
                sub
            else
                psub.fetch_delivery = nothing
                _remove_active_pull_delivery_locked!(psub, sub)
                retire = sub
                nothing
            end
        else
            nothing
        end
    end
    if !isnothing(cached)
        return cached, true
    end
    if !isnothing(retire)
        try
            close(retire)
        catch err
            throw(CleanupError("close undersized pull fetch delivery subscription", err))
        end
    end

    sub = subscribe(client, new_inbox(client); pending_msgs_limit, pending_bytes_limit,
                    cancel_token)
    close_now = false
    use_cached = false
    @lock psub.close_lock begin
        if psub.closed
            close_now = true
        elseif isnothing(psub.fetch_delivery) && !psub.fetch_delivery_in_use
            psub.fetch_delivery = sub
            psub.fetch_delivery_in_use = true
            push!(psub.active_deliveries, sub)
            use_cached = true
        else
            push!(psub.active_deliveries, sub)
        end
    end
    if close_now
        closed_err = ConnectionClosedError("pull subscription is closed")
        try
            close(sub)
        catch cleanup_err
            throw(Base.CompositeException([closed_err,
                                           CleanupError("close pull delivery subscription", cleanup_err)]))
        end
        throw(closed_err)
    end
    sub, use_cached
end

function _release_pull_fetch_delivery!(psub::PullSubscription, sub::Subscription,
                                       cached::Bool, reusable::Bool,
                                       label::AbstractString="close pull fetch delivery subscription")
    if cached
        keep_cached = reusable && _pull_fetch_delivery_idle(sub)
        close_now = @lock psub.close_lock begin
            if psub.fetch_delivery === sub && keep_cached && !psub.closed
                psub.fetch_delivery_in_use = false
                false
            else
                psub.fetch_delivery === sub && (psub.fetch_delivery = nothing)
                psub.fetch_delivery_in_use = false
                _remove_active_pull_delivery_locked!(psub, sub)
                true
            end
        end
        if close_now
            try
                close(sub)
            catch err
                throw(CleanupError(label, err))
            end
        end
        return nothing
    end
    _close_pull_delivery!(psub, sub, label)
end

function _next_pull_fetch_msg(sub::Subscription, timeout::Real;
                              cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    client = sub.client
    deadline = time() + Float64(timeout)
    while true
        ready, msg = _take_subscription_msg_ready!(sub)
        ready && return msg

        closed, empty = @lock sub.lock (sub.closed, !isready(sub.messages))
        st = status(client)
        closed && empty && _throw_pull_fetch_wait_interrupted(closed, st)
        ready = @lock sub.lock begin
            _wait_subscription_condition_locked(sub, _remaining_timeout(deadline); cancel_token) do
                isready(sub.messages) || sub.closed || status(client) != ConnectionStatus.CONNECTED
            end
        end
        ready || throw(TimeoutError("next message timed out"))
        ready, msg = _take_subscription_msg_ready!(sub)
        ready && return msg
        closed = @lock sub.lock sub.closed
        st = status(client)
        (closed || st != ConnectionStatus.CONNECTED) && _throw_pull_fetch_wait_interrupted(closed, st)
    end
end

function _publish_pull_fetch_request(psub::PullSubscription, request_subject::String,
                                     payload::AbstractVector{UInt8}, reply::String;
                                     cancel_token::MaybeCancellationToken=nothing)
    try
        _publish(psub.js.client, request_subject, payload; reply, force_flush=true,
                 mode=PublishMode.DIRECT, cancel_token)
    catch err
        if err isa ConnectionReconnectingError ||
           (err isa ConnectionClosedError && status(psub.js.client) == ConnectionStatus.DISCONNECTED)
            throw(FetchDisconnectedError())
        end
        rethrow()
    end
    nothing
end

function _prepare_pull_fetch_request!(psub::PullSubscription, request_subject::String,
                                      batch::Int, expires_ns::Int, heartbeat_ns::Int,
                                      max_bytes::Union{Int,Nothing}, no_wait::Bool,
                                      min_pending::Union{Int,Nothing},
                                      min_ack_pending::Union{Int,Nothing},
                                      priority_group::Union{String,Nothing},
                                      priority::Union{Int,Nothing}, reply::String;
                                      cancel_token::MaybeCancellationToken=nothing)
    _check_pull_subscription_open(psub)
    @lock psub.fetch_lock begin
        payload = _pull_fetch_request_payload!(psub.fetch_payload_buffer, batch, expires_ns,
                                               heartbeat_ns, max_bytes, no_wait, psub.pin_id,
                                               min_pending, min_ack_pending, priority_group,
                                               priority)
        _publish_pull_fetch_request(psub, request_subject, payload, reply; cancel_token)
    end
    nothing
end

function fetch(psub::PullSubscription{C}, batch=1; timeout::Real=psub.js.timeout,
               expires::Real=_pull_fetch_default_expires(timeout),
               heartbeat::Union{Nothing,Real}=nothing, max_bytes=nothing,
               no_wait=false, min_pending=nothing, min_ack_pending=nothing,
               priority_group=nothing, priority=nothing,
               cancel_token::MaybeCancellationToken=nothing) where {C}
    _throw_if_cancelled(cancel_token)
    batch, timeout_seconds, expires_seconds, heartbeat_seconds, max_bytes_int, no_wait_bool,
        min_pending, min_ack_pending, priority_group, priority =
        _validate_pull_fetch(psub, batch, timeout, expires, heartbeat, max_bytes, no_wait,
                             min_pending, min_ack_pending, priority_group, priority)
    _begin_pull_fetch!(psub)
    delivery::Union{Subscription{C},Nothing} = nothing
    cached_delivery::Bool = false
    reusable_delivery::Bool = false
    result = JetStreamMsg{C}[]
    primary_error = nothing
    try
        delivery, cached_delivery = _acquire_pull_fetch_delivery!(psub, batch, max_bytes_int;
                                                                  cancel_token)
        request_subject = psub.next_subject
        reply = delivery.subject
        heartbeat_ns = heartbeat_seconds > 0 ? _seconds_to_nanoseconds(heartbeat_seconds) : 0
        _prepare_pull_fetch_request!(psub, request_subject, batch,
                                     _seconds_to_nanoseconds(expires_seconds),
                                     heartbeat_ns, max_bytes_int, no_wait_bool,
                                     min_pending, min_ack_pending,
                                     priority_group, priority, reply;
                                     cancel_token)
        sizehint!(result, _pull_delivery_pending_msgs_limit(psub.js.client, batch))
        deadline = time() + timeout_seconds
        heartbeat_deadline = heartbeat_seconds > 0 ? time() + 2 * heartbeat_seconds : Inf
        while length(result) < batch && time() < deadline
            wait_deadline = min(deadline, heartbeat_deadline)
            remaining = max(0.001, wait_deadline - time())
            try
                msg = _next_pull_fetch_msg(delivery, remaining; cancel_token)
                action, err = _jetstream_status_action(msg, request_subject)
                heartbeat_seconds > 0 && (heartbeat_deadline = time() + 2 * heartbeat_seconds)
                pin_id = header(msg, "Nats-Pin-Id")
                !isnothing(pin_id) && !isempty(pin_id) && (@lock psub.fetch_lock psub.pin_id = pin_id)
                if action in (:idle_heartbeat, :flow_control, :control)
                    continue
                elseif action in (:no_messages, :timeout, :batch_completed, :max_bytes_exceeded)
                    reusable_delivery = true
                    break
                elseif action == :message
                    push!(result, JetStreamMsg(msg, psub.js.client))
                else
                    action == :consumer_deleted && (@lock psub.close_lock psub.server_deleted = true)
                    action == :pin_id_mismatch && (@lock psub.fetch_lock psub.pin_id = nothing)
                    throw(err)
                end
            catch err
                if err isa TimeoutError
                    if heartbeat_seconds > 0 && time() < deadline && time() >= heartbeat_deadline
                        throw(_jetstream_heartbeat_error())
                    end
                    break
                elseif err isa FetchDisconnectedError
                    isempty(result) || break
                end
                rethrow()
            end
        end
        length(result) == batch && (reusable_delivery = true)
    catch err
        primary_error = err
    finally
        if !isnothing(delivery)
            try
                _release_pull_fetch_delivery!(psub, delivery, cached_delivery,
                                              isnothing(primary_error) && reusable_delivery,
                                              "close pull fetch delivery subscription")
            catch cleanup_err
                primary_error = isnothing(primary_error) ? cleanup_err :
                                Base.CompositeException([primary_error, cleanup_err])
            end
        end
        _end_pull_fetch!(psub)
    end
    isnothing(primary_error) || throw(primary_error)
    result
end
