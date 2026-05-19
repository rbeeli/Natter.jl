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
