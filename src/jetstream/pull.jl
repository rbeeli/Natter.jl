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

mutable struct PushSubscription{C<:Client,J<:JetStreamContext{C},S<:Subscription{C}}
    js::J
    sub::S
    stream::String
    consumer::String
    close_lock::ReentrantLock
    delete_on_close::Bool
    closed::Bool
    server_deleted::Bool
    heartbeat_task::Union{Task,Nothing}
    ordered_reset_task::Union{Task,Nothing}
    control_handler::Union{_JetStreamPushControlHandler,Nothing}
    info::Union{ConsumerInfo,Nothing}
end

function PushSubscription(js::J, sub::S, stream::AbstractString, consumer::AbstractString,
                          close_lock::ReentrantLock, delete_on_close::Bool, closed::Bool,
                          heartbeat_task::Union{Task,Nothing},
                          control_handler::Union{_JetStreamPushControlHandler,Nothing}) where {C<:Client,J<:JetStreamContext{C},S<:Subscription{C}}
    PushSubscription{C,J,S}(js, sub, String(stream), String(consumer), close_lock, delete_on_close, closed,
                            false, heartbeat_task, nothing, control_handler, nothing)
end

PushSubscription(js::JetStreamContext, sub::Subscription, stream::AbstractString, consumer::AbstractString,
                 close_lock::ReentrantLock, delete_on_close::Bool, closed::Bool) =
    PushSubscription(js, sub, String(stream), String(consumer), close_lock, delete_on_close, closed, nothing, nothing)

function _touch_push_control_handler!(handler::_JetStreamPushControlHandler)
    handler.idle_heartbeat[] > 0 || return nothing
    handler.last_seen[] = time()
    nothing
end

function _close_subscription_from_control!(sub::Subscription)
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
    _report_cleanup_errors(sub.client, errors)
    nothing
end

function _push_msg_metadata(msg::M) where {M<:AbstractMsg}
    reply = msg.reply
    isnothing(reply) && return nothing
    startswith(reply, _JS_ACK_PREFIX) || return nothing
    try
        _parse_msg_metadata(reply)
    catch err
        err isa JetStreamError || err isa ProtocolError || err isa ArgumentError ||
            err isa OverflowError || rethrow()
        nothing
    end
end

function _record_subscription_data_received!(handler::_JetStreamPushControlHandler,
                                             msg::M) where {M<:AbstractMsg}
    handler.flow_control[] && Threads.atomic_add!(handler.flow_incoming, one(UInt64))
    handler.ordered && return nothing
    handler.idle_heartbeat[] > 0 || return nothing
    @lock handler.lock begin
        if handler.sequence_state_anchored
            if handler.last_stream_seq > 0
                handler.next_consumer_seq += 1
                handler.last_stream_seq += 1
            end
        else
            handler.sequence_state_anchored = true
            parsed = _push_msg_metadata(msg)
            if !isnothing(parsed)
                handler.next_consumer_seq = parsed.consumer_sequence + 1
                handler.last_stream_seq = parsed.stream_sequence
            end
        end
    end
    nothing
end

function _update_flow_delivered_locked!(handler::_JetStreamPushControlHandler, queued::UInt64)
    incoming = handler.flow_incoming[]
    delivered = incoming > queued ? incoming - queued : UInt64(0)
    delivered > handler.flow_delivered && (handler.flow_delivered = delivered)
    handler.flow_delivered
end

function _maybe_reply_to_subscription_flow_control!(sub::Subscription, handler::_JetStreamPushControlHandler)
    handler.flow_control[] || return nothing
    queued = @lock sub.lock UInt64(Base.n_avail(sub.messages))
    reply = @lock handler.lock begin
        _update_flow_delivered_locked!(handler, queued)
        if !isnothing(handler.flow_reply) && handler.flow_delivered >= handler.flow_target
            reply = handler.flow_reply
            handler.flow_reply = nothing
            handler.flow_target = UInt64(0)
            reply
        else
            nothing
        end
    end
    isnothing(reply) || _publish_flow_control_reply(sub, reply)
    nothing
end

function _handle_subscription_control(handler::_JetStreamPushControlHandler, sub::Subscription,
                                      msg::M)::Bool where {M<:AbstractMsg}
    _touch_push_control_handler!(handler)
    isempty(msg.data) || return false
    action, err = _jetstream_status_action(msg)
    action == :message && return false
    if action == :flow_control
        _schedule_or_reply_to_flow_control(sub, handler, msg)
    elseif action == :idle_heartbeat
        _reply_to_consumer_stalled!(sub, msg)
        _handle_push_sequence_heartbeat!(handler, sub, msg)
    elseif !isnothing(err)
        _report_error(sub.client, err)
    end
    if action == :consumer_deleted
        handler.consumer_deleted[] = true
        _close_subscription_from_control!(sub)
    end
    true
end

function _push_heartbeat_monitor(psub::PushSubscription, handler::_JetStreamPushControlHandler)
    interval = handler.idle_heartbeat[]
    interval > 0 || return nothing
    poll = min(0.25, max(0.01, interval / 2))
    while true
        sleep(poll)
        closed = (@lock psub.close_lock psub.closed) ||
                 (@lock psub.sub.lock psub.sub.closed) ||
                 status(psub.sub.client) in (ConnectionStatus.CLOSED, ConnectionStatus.DISCONNECTED)
        closed && return nothing
        st = status(psub.js.client)
        if st in (ConnectionStatus.CONNECTING, ConnectionStatus.RECONNECTING)
            _touch_push_control_handler!(handler)
            continue
        end
        _maybe_reply_to_subscription_flow_control!(psub.sub, handler)
        missed = time() - handler.last_seen[] > 2 * interval
        if missed
            if handler.ordered
                reset_seq = @lock handler.lock handler.last_stream_seq + 1
                try
                    _request_ordered_push_reset!(handler, reset_seq)
                catch err
                    _report_error(psub.js.client, CleanupError("reset ordered push consumer after missed heartbeat", err))
                end
            else
                _report_error(psub.js.client, _jetstream_heartbeat_error())
            end
            _touch_push_control_handler!(handler)
        end
    end
end

function _start_push_heartbeat_monitor(psub::PushSubscription, handler::_JetStreamPushControlHandler)
    handler.idle_heartbeat[] > 0 || return nothing
    _spawn_control(:push_heartbeat) do
        _push_heartbeat_monitor(psub, handler)
    end
end

function next(psub::PushSubscription{C}; timeout::Real=1.0,
              cancel_token::MaybeCancellationToken=nothing)::JetStreamMsg{C} where {C<:Client}
    JetStreamMsg(next(psub.sub; timeout, cancel_token), psub.js.client)
end

function _push_idle_heartbeat_seconds(info::ConsumerInfo)::Float64
    heartbeat = info.config.idle_heartbeat
    isnothing(heartbeat) ? 0.0 : Float64(heartbeat)
end

function _push_idle_heartbeat_seconds(config::Dict{String,Any})::Float64
    heartbeat = get(config, "idle_heartbeat", nothing)
    heartbeat isa Real && !(heartbeat isa Bool) ? Float64(heartbeat) / 1_000_000_000 : 0.0
end

_push_flow_control_enabled(info::ConsumerInfo)::Bool = info.config.flow_control == true
_push_flow_control_enabled(config::Dict{String,Any})::Bool = get(config, "flow_control", false) == true

function _push_callback_auto_ack(manual_ack::Bool, callback, info::ConsumerInfo)::Bool
    !manual_ack && !isnothing(callback) && info.config.ack_policy != AckPolicy.NONE
end

function _push_callback_auto_ack(manual_ack::Bool, callback, config::Dict{String,Any})::Bool
    !manual_ack && !isnothing(callback) && get(config, "ack_policy", nothing) != "none"
end

function _jetstream_push_callback(js::JetStreamContext{C}, callback,
                                  auto_ack::Bool; borrowed::Bool=false) where {C<:Client}
    isnothing(callback) && return nothing
    if borrowed
        return msg -> begin
            jsmsg = BorrowedJetStreamMsg(msg, js.client)
            callback(jsmsg)
            auto_ack && ack(jsmsg)
            nothing
        end
    end
    msg -> begin
        jsmsg = JetStreamMsg(msg, js.client)
        callback(jsmsg)
        auto_ack && ack(jsmsg)
        nothing
    end
end

function _publish_flow_control_reply(sub::Subscription, reply::String)
    try
        _publish(sub.client, reply, EMPTY_BYTES; force_flush=true)
    catch err
        _report_error(sub.client, err)
    end
    nothing
end

function _reply_to_consumer_stalled!(sub::Subscription, msg::AbstractMsg)
    reply = header(msg, _JS_HEADER_CONSUMER_STALLED)
    isnothing(reply) && return nothing
    !isempty(reply) && _publish_flow_control_reply(sub, reply)
    nothing
end

function _schedule_or_reply_to_flow_control(sub::Subscription, handler::_JetStreamPushControlHandler, msg::AbstractMsg)
    if isnothing(msg.reply)
        _report_error(sub.client, JetStreamError(_JS_STATUS_CONTROL, nothing, "flow control request missing reply subject"))
        return nothing
    end

    reply = msg.reply
    if !handler.flow_control[]
        _publish_flow_control_reply(sub, reply)
        return nothing
    end
    queued = @lock sub.lock UInt64(Base.n_avail(sub.messages))
    send_now = @lock handler.lock begin
        _update_flow_delivered_locked!(handler, queued)
        if handler.flow_delivered >= handler.flow_incoming[]
            true
        else
            handler.flow_reply = reply
            handler.flow_target = handler.flow_incoming[]
            false
        end
    end
    send_now && _publish_flow_control_reply(sub, reply)
    nothing
end

_header_int(msg::AbstractMsg, key::AbstractString)::Union{Int,Nothing} = begin
    value = header(msg, key)
    isnothing(value) && return nothing
    tryparse(Int, value)
end

function _request_ordered_push_reset!(handler::_JetStreamPushControlHandler, start_seq::Int)
    callback = @lock handler.lock begin
        handler.ordered || return nothing
        handler.ordered_resetting && return nothing
        handler.ordered_resetting = true
        handler.next_consumer_seq = 1
        handler.last_stream_seq = max(0, start_seq - 1)
        handler.sequence_state_anchored = false
        handler.flow_incoming[] = UInt64(0)
        handler.flow_delivered = UInt64(0)
        handler.flow_reply = nothing
        handler.flow_target = UInt64(0)
        handler.ordered_reset_callback
    end
    if isnothing(callback)
        @lock handler.lock handler.ordered_resetting = false
    else
        try
            callback(max(1, start_seq))
            return nothing
        catch err
            @lock handler.lock handler.ordered_resetting = false
            rethrow()
        end
    end
    nothing
end

function _handle_ordered_push_data!(handler::_JetStreamPushControlHandler, client::Client,
                                    msg::M)::Bool where {M<:AbstractMsg}
    handler.ordered || return false
    parsed = _push_msg_metadata(msg)
    isnothing(parsed) && return false
    reset_seq = @lock handler.lock begin
        handler.ordered || return 0
        if parsed.consumer_sequence != handler.next_consumer_seq
            handler.last_stream_seq + 1
        else
            handler.next_consumer_seq = parsed.consumer_sequence + 1
            handler.last_stream_seq = parsed.stream_sequence
            0
        end
    end
    reset_seq == 0 && return false
    try
        _request_ordered_push_reset!(handler, reset_seq)
    catch err
        _report_error(client, CleanupError("reset ordered push consumer", err))
    end
    true
end

_handle_ordered_push_data!(::_SubscriptionControlHandler, ::Client, ::AbstractMsg)::Bool = false

function _handle_push_sequence_heartbeat!(handler::_JetStreamPushControlHandler, sub::Subscription, msg::AbstractMsg)
    last_consumer = _header_int(msg, _JS_HEADER_LAST_CONSUMER)
    isnothing(last_consumer) && return nothing
    reset_seq = 0
    report_err::Union{ConsumerSequenceMismatchError,Nothing} = nothing
    have_sequence = false
    @lock handler.lock begin
        have_sequence = handler.last_stream_seq > 0
        if have_sequence
            delivered = handler.next_consumer_seq - 1
            if last_consumer != delivered
                if handler.ordered
                    last_consumer > delivered && (reset_seq = handler.last_stream_seq + 1)
                else
                    report_err = ConsumerSequenceMismatchError(max(1, handler.last_stream_seq),
                                                               delivered, last_consumer)
                end
            end
        end
    end
    have_sequence || return nothing
    isnothing(report_err) || return _report_error(sub.client, report_err)
    reset_seq == 0 && return nothing
    try
        _request_ordered_push_reset!(handler, reset_seq)
    catch err
        _report_error(sub.client, CleanupError("reset ordered push consumer after heartbeat gap", err))
    end
    nothing
end

struct _OrderedSubscriptionRemap
    old_sid::Int
    new_sid::Int
    old_subject::String
    new_subject::String
    old_server_active::Bool
end

function _remap_ordered_subscription!(sub::Subscription, deliver::String)::_OrderedSubscriptionRemap
    client = sub.client
    @lock sub.lock begin
        sub.closed && throw(ConnectionClosedError("subscription is closed"))
        @lock client.lock begin
            old_sid = sub.sid
            old_subject = sub.subject
            old_server_active = sub.server_active
            _delete_subscription_locked!(client, old_sid, sub)
            client.sid += 1
            new_sid = client.sid
            sub.sid = new_sid
            sub.subject = deliver
            sub.server_active = false
            client.subscriptions[new_sid] = sub
            _set_subscription_snapshot_locked!(client, new_sid, sub)
            _OrderedSubscriptionRemap(old_sid, new_sid, old_subject, deliver, old_server_active)
        end
    end
end

function _restore_ordered_subscription_mapping!(sub::Subscription, remap::_OrderedSubscriptionRemap;
                                                server_active::Bool=false)::Bool
    client = sub.client
    @lock sub.lock begin
        sub.closed && return false
        @lock client.lock begin
            sub.sid == remap.new_sid || return false
            get(client.subscriptions, remap.new_sid, nothing) === sub || return false
            _delete_subscription_locked!(client, remap.new_sid, sub)
            sub.sid = remap.old_sid
            sub.subject = remap.old_subject
            sub.server_active = server_active
            client.subscriptions[remap.old_sid] = sub
            _set_subscription_snapshot_locked!(client, remap.old_sid, sub)
            true
        end
    end
end

function _send_ordered_subscription_reset!(sub::Subscription, remap::_OrderedSubscriptionRemap)
    _send_raw(sub.client, string(_unsub_cmd(remap.old_sid),
                                 _sub_cmd(remap.new_subject, sub.queue, remap.new_sid));
              force_flush=true)
    @lock sub.lock begin
        if !sub.closed && sub.sid == remap.new_sid
            sub.server_delivered_base = sub.delivered
            sub.server_active = true
        end
    end
    nothing
end

function _rollback_ordered_subscription_reset!(sub::Subscription, remap::_OrderedSubscriptionRemap,
                                               reset_sent::Bool)
    restored = _restore_ordered_subscription_mapping!(
        sub, remap;
        server_active=!reset_sent && status(sub.client) == ConnectionStatus.CONNECTED &&
                      remap.old_server_active,
    )
    restored || return nothing
    reset_sent || return nothing
    _send_raw(sub.client, string(_unsub_cmd(remap.new_sid),
                                 _sub_cmd(remap.old_subject, sub.queue, remap.old_sid));
              force_flush=true)
    @lock sub.lock begin
        if !sub.closed && sub.sid == remap.old_sid && sub.subject == remap.old_subject
            sub.server_delivered_base = sub.delivered
            sub.server_active = true
        end
    end
    nothing
end

function _finish_ordered_reset!(handler::Union{_JetStreamPushControlHandler,Nothing})
    isnothing(handler) && return nothing
    @lock handler.lock handler.ordered_resetting = false
    nothing
end

function _ordered_delete_consumer_task(psub::PushSubscription, consumer::String)
    isempty(consumer) && return nothing
    try
        consumer_delete(psub.js, psub.stream, consumer; timeout=psub.js.timeout)
    catch err
        _consumer_missing(err) || _report_error(psub.js.client, CleanupError("delete ordered push consumer $consumer", err))
    end
    nothing
end

function _ordered_push_reset_task(psub::PushSubscription, base_config::Dict{String,Any}, start_seq::Int)
    handler = psub.control_handler
    remap = nothing
    reset_sent = false
    try
        (@lock psub.close_lock psub.closed) && return nothing
        old_consumer = psub.consumer
        deliver = new_inbox(psub.js.client)
        remap = _remap_ordered_subscription!(psub.sub, deliver)
        _send_ordered_subscription_reset!(psub.sub, remap)
        reset_sent = true

        name = @lock psub.js.client.lock randstring(psub.js.client.rng, 16)
        cfg = _ordered_push_reset_config(base_config, name, deliver, start_seq)
        info = _consumer_create_payload_request(psub.js, psub.stream, cfg; timeout=psub.js.timeout, action="create")
        closed = @lock psub.close_lock begin
            if !psub.closed
                psub.consumer = info.name
                psub.info = info
                false
            else
                true
            end
        end
        if closed
            try
                consumer_delete(psub.js, psub.stream, info.name; timeout=psub.js.timeout)
            catch err
                _consumer_missing(err) || _report_error(psub.js.client, CleanupError("delete closed ordered push consumer $(info.name)", err))
            end
            return nothing
        end
        _spawn_work(:ordered_delete_consumer) do
            _ordered_delete_consumer_task(psub, old_consumer)
        end
    catch err
        if !isnothing(remap)
            try
                _rollback_ordered_subscription_reset!(psub.sub, remap, reset_sent)
            catch cleanup_err
                _report_error(psub.js.client,
                              CleanupError("rollback ordered push subscription reset", cleanup_err))
            end
        end
        _report_error(psub.js.client, CleanupError("reset ordered push consumer", err))
    finally
        _finish_ordered_reset!(handler)
    end
    nothing
end

function _schedule_ordered_push_reset!(psub::PushSubscription, base_config::Dict{String,Any}, start_seq::Int)
    psub.ordered_reset_task = _spawn_work(:ordered_push_reset) do
        _ordered_push_reset_task(psub, base_config, start_seq)
    end
    nothing
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
        _publish(psub.js.client, request_subject, payload; reply,
                 buffer_on_reconnect=false, force_flush=true, cancel_token)
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
            requested_messages_before = stream.state.requested_messages
            requested_bytes_before = stream.state.requested_bytes
            _pull_stream_release_counters!(stream.state, 1, bytes)
            stream.state.buffered_messages += 1
            stream.state.buffered_bytes += bytes
            stream.state.delivered += 1
            try
                put!(stream.messages, msg)
            catch
                stream.state.requested_messages = requested_messages_before
                stream.state.requested_bytes = requested_bytes_before
                stream.state.buffered_messages = max(0, stream.state.buffered_messages - 1)
                stream.state.buffered_bytes = max(0, stream.state.buffered_bytes - bytes)
                stream.state.delivered = max(0, stream.state.delivered - 1)
                rethrow()
            end
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

function Base.take!(stream::PullMessageStream)
    while true
        msg = EMPTY_MSG
        ready = @lock stream.message_lock begin
            while !isready(stream.messages) && isopen(stream.messages)
                wait(stream.message_condition)
            end
            if isready(stream.messages)
                msg = take!(stream.messages)
                @lock stream.state.lock begin
                    stream.state.buffered_messages = max(0, stream.state.buffered_messages - 1)
                    stream.state.buffered_bytes = max(0, stream.state.buffered_bytes - _pull_stream_msg_bytes(msg))
                end
                notify(stream.message_condition)
                true
            else
                false
            end
        end
        if ready
            jsmsg = JetStreamMsg(msg, stream.subscription.js.client)
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
        err = _pull_stream_error(stream.state)
        isnothing(err) || throw(err)
        throw(InvalidStateException("pull message stream is closed", :closed))
    end
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
