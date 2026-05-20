function _push_subscribe(js::JetStreamContext, subject::AbstractString; stream::Union{AbstractString,Nothing}=nothing, durable::Union{AbstractString,Nothing}=nothing,
                         queue::Union{AbstractString,Nothing}=nothing, callback=nothing, manual_ack::Bool=false,
                         config::Union{ConsumerConfig,AbstractDict{String,<:Any}}=ConsumerConfig(),
                         timeout::Real=js.timeout, ordered::Bool=false, borrowed::Bool=false,
                         cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    subject = _validate_subject(subject)
    timeout = _positive_timeout_seconds("timeout", timeout)
    borrowed = _connect_option_bool("borrowed", borrowed)
    borrowed && isnothing(callback) &&
        throw(ArgumentError("borrowed push subscriptions require a callback"))
    verify_config = config isa ConsumerConfig
    cfg = _js_config_payload(config)
    queue_explicit = !isnothing(queue)
    local_queue = _resolve_push_queue!(cfg, queue)
    ordered && _prepare_ordered_push_consumer_config!(cfg, local_queue)
    _validate_push_queue_control_config!(cfg, local_queue)
    _validate_push_consumer_control_config!(cfg)
    _validate_consumer_config_payload!(cfg)
    bind_fields = Set{String}(keys(cfg))
    !isnothing(queue) && push!(bind_fields, "deliver_group")

    stream = isnothing(stream) ? _stream_by_subject(js, subject; timeout, cancel_token) : _validate_api_name("stream", stream)
    durable = isnothing(durable) ? nothing : _validate_api_name("consumer", durable)
    ordered && !isnothing(durable) && throw(ArgumentError("ordered push consumers do not support durable consumers"))
    deliver = new_inbox(js.client)
    if !_consumer_has_filter(cfg)
        cfg["filter_subject"] = String(subject)
        push!(bind_fields, "filter_subject")
    end
    if !isnothing(durable)
        _set_config_default!(cfg, "name", durable)
        _set_config_default!(cfg, "durable_name", durable)
        push!(bind_fields, "durable_name")
    end
    _default_push_queue_consumer!(cfg, bind_fields, local_queue)
    _validate_consumer_config_payload!(cfg)
    bind_name = _consumer_bind_name(cfg)
    ordered && !isnothing(bind_name) && throw(ArgumentError("ordered push consumers cannot bind existing consumers"))
    delete_on_close = isnothing(bind_name)
    ordered_base_config = ordered ? _copy_config_payload(cfg) : nothing
    info::Union{ConsumerInfo,Nothing} = nothing
    create_consumer = false
    if isnothing(bind_name)
        _set_config_default!(cfg, "name", @lock js.client.lock randstring(js.client.rng, 16))
        _set_config_default!(cfg, "deliver_subject", deliver)
        create_consumer = true
    else
        existing = _consumer_info_or_nothing(js, stream, bind_name; timeout, cancel_token)
        if isnothing(existing)
            _set_config_default!(cfg, "deliver_subject", deliver)
            create_consumer = true
        else
            info = _bind_existing_push_consumer!(existing, cfg, bind_fields, local_queue, queue_explicit)
        end
    end
    control_handler = _JetStreamPushControlHandler(
        isnothing(info) ? _push_idle_heartbeat_seconds(cfg) : _push_idle_heartbeat_seconds(info);
        flow_control=isnothing(info) ? _push_flow_control_enabled(cfg) : _push_flow_control_enabled(info),
    )
    if ordered
        @lock control_handler.lock begin
            control_handler.ordered = true
            control_handler.next_consumer_seq = 1
            control_handler.last_stream_seq = 0
            control_handler.sequence_state_anchored = false
            control_handler.ordered_resetting = false
        end
    end
    deliver_subject = String(cfg["deliver_subject"])
    auto_ack = isnothing(info) ? _push_callback_auto_ack(manual_ack, callback, cfg) :
               _push_callback_auto_ack(manual_ack, callback, info)
    wrapped_callback = _jetstream_push_callback(js, callback, auto_ack; borrowed)
    sub = subscribe(js.client, deliver_subject; queue=local_queue, callback=wrapped_callback,
                    borrowed=!isnothing(wrapped_callback) && borrowed,
                    _control_handler=control_handler, cancel_token)
    consumer_created = false
    try
        if create_consumer
            try
                info = _consumer_create_payload_request(js, stream, cfg; timeout, action="create",
                                                        verify_config, cancel_token)
                consumer_created = true
            catch err
                (!isnothing(bind_name) && _consumer_create_conflict(err)) || rethrow()
                try
                    close(sub)
                catch cleanup_err
                    throw(Base.CompositeException([err, CleanupError("close provisional push subscription $deliver_subject", cleanup_err)]))
                end
                existing = consumer_info(js, stream, bind_name; timeout, cancel_token)
                info = _bind_existing_push_consumer!(existing, cfg, bind_fields, local_queue, queue_explicit)
                deliver_subject = String(cfg["deliver_subject"])
                retry_auto_ack = _push_callback_auto_ack(manual_ack, callback, info)
                wrapped_callback = _jetstream_push_callback(js, callback, retry_auto_ack; borrowed)
                sub = subscribe(js.client, deliver_subject; queue=local_queue, callback=wrapped_callback,
                                borrowed=!isnothing(wrapped_callback) && borrowed,
                                _control_handler=control_handler, cancel_token)
            end
        end
        info = info::ConsumerInfo
        if !haskey(cfg, "name") || isnothing(cfg["name"])
            cfg["name"] = info.name
        end
        control_handler.idle_heartbeat[] = _push_idle_heartbeat_seconds(info)
        control_handler.flow_control[] = _push_flow_control_enabled(info)
        control_handler.last_seen[] = time()
        psub = PushSubscription(js, sub, String(stream), String(info.name), ReentrantLock(), delete_on_close, false,
                                false, nothing, nothing, control_handler, info)
        if ordered
            base_config = ordered_base_config::Dict{String,Any}
            @lock control_handler.lock begin
                control_handler.ordered_reset_callback =
                    start_seq -> _schedule_ordered_push_reset!(psub, base_config, start_seq)
            end
        end
        psub.heartbeat_task = _start_push_heartbeat_monitor(psub, control_handler)
        psub
    catch err
        cleanup_errors = Any[]
        try
            close(sub)
        catch cleanup_err
            push!(cleanup_errors, CleanupError("close push subscription $deliver_subject", cleanup_err))
        end
        if delete_on_close && consumer_created
            try
                consumer_delete(js, stream, (info::ConsumerInfo).name; timeout, cancel_token)
            catch cleanup_err
                push!(cleanup_errors, CleanupError("delete push consumer $((info::ConsumerInfo).name)", cleanup_err))
            end
        end
        isempty(cleanup_errors) ? rethrow() : throw(Base.CompositeException(vcat(Any[err], cleanup_errors)))
    end
end

function push_subscribe(js::JetStreamContext, subject::AbstractString; stream::Union{AbstractString,Nothing}=nothing, durable::Union{AbstractString,Nothing}=nothing,
                        queue::Union{AbstractString,Nothing}=nothing, callback=nothing, manual_ack::Bool=false,
                        config::Union{ConsumerConfig,AbstractDict{String,<:Any}}=ConsumerConfig(),
                        timeout::Real=js.timeout, ordered::Bool=false, borrowed::Bool=false,
                        cancel_token::MaybeCancellationToken=nothing)
    _push_subscribe(js, subject; stream, durable, queue, callback, manual_ack, config, timeout,
                    ordered, borrowed, cancel_token)
end

function _mark_push_server_deleted!(psub::PushSubscription)
    handler = psub.control_handler
    @lock psub.close_lock psub.server_deleted = true
    isnothing(handler) || (handler.consumer_deleted[] = true)
    nothing
end

function _push_server_deleted(psub::PushSubscription)::Bool
    handler = psub.control_handler
    @lock psub.close_lock begin
        if psub.server_deleted
            true
        else
            deleted = !isnothing(handler) && handler.consumer_deleted[]
            deleted && (psub.server_deleted = true)
            deleted
        end
    end
end

function _close_pull_subscription(psub::PullSubscription; timeout::Real=psub.js.timeout,
                                  cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    timeout = _positive_timeout_seconds("timeout", timeout)
    deadline = time() + timeout
    already_closed, deliveries = @lock psub.close_lock begin
        was_closed = psub.closed
        psub.closed = true
        deliveries = copy(psub.active_deliveries)
        empty!(psub.active_deliveries)
        psub.fetch_delivery = nothing
        psub.fetch_delivery_in_use = false
        was_closed, deliveries
    end
    errors = Any[]
    if !already_closed
        for sub in deliveries
            try
                close(sub)
            catch err
                push!(errors, CleanupError("close pull delivery subscription", err))
            end
        end
    end
    server_deleted = @lock psub.close_lock psub.server_deleted
    if psub.delete_on_close && !server_deleted
        try
            _delete_consumer_for_close!(
                () -> (@lock psub.close_lock psub.server_deleted = true),
                psub.js, psub.stream, psub.consumer, "delete pull consumer";
                timeout=_remaining_timeout_or_throw(deadline, "close pull subscription"; cancel_token),
                cancel_token)
        catch err
            push!(errors, err isa CleanupError ? err : CleanupError("delete pull consumer $(psub.consumer)", err))
        end
    end
    _throw_errors(errors)
    nothing
end

close(psub::PullSubscription; timeout::Real=psub.js.timeout,
      cancel_token::MaybeCancellationToken=nothing) =
    _close_pull_subscription(psub; timeout, cancel_token)

function _close_push_subscription(psub::PushSubscription; timeout::Real=psub.js.timeout,
                                  cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    timeout = _positive_timeout_seconds("timeout", timeout)
    deadline = time() + timeout
    already_closed = @lock psub.close_lock begin
        was_closed = psub.closed
        psub.closed = true
        was_closed
    end
    errors = Any[]
    if !already_closed
        try
            close(psub.sub)
        catch err
            push!(errors, err)
        end
        _wait_task!(errors, "stop push heartbeat monitor $(psub.consumer)", psub.heartbeat_task;
                    interrupt=true, deadline)
        _wait_task!(errors, "stop ordered push reset $(psub.consumer)", psub.ordered_reset_task;
                    interrupt=true, deadline)
    end
    server_deleted = _push_server_deleted(psub)
    if psub.delete_on_close && !server_deleted
        try
            _delete_consumer_for_close!(
                () -> _mark_push_server_deleted!(psub),
                psub.js, psub.stream, psub.consumer, "delete push consumer";
                timeout=_remaining_timeout_or_throw(deadline, "close push subscription"; cancel_token),
                cancel_token)
        catch err
            push!(errors, err isa CleanupError ? err : CleanupError("delete push consumer $(psub.consumer)", err))
        end
    end
    _throw_errors(errors)
    nothing
end

close(psub::PushSubscription; timeout::Real=psub.js.timeout,
      cancel_token::MaybeCancellationToken=nothing) =
    _close_push_subscription(psub; timeout, cancel_token)
