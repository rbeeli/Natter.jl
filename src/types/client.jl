mutable struct Subscription{C<:AbstractNatterClient}
    client::C
    lock::ReentrantLock
    sid::Int
    subject::String
    queue::Union{String,Nothing}
    has_callback::Bool
    borrowed_callback::Bool
    borrowed_callback_handler::_BorrowedCallback{C}
    messages::MsgQueue{Msg}
    condition::Base.GenericCondition{ReentrantLock}
    timeout_queue::_ConditionTimeoutQueue
    control_handler::_SubscriptionControlHandler
    pending_msgs_limit::Int
    pending_bytes_limit::Int
    pending_bytes::Int
    received::Int
    delivered::Int
    dropped_msgs::Int
    max_msgs::Int
    closed::Bool
    processor::Union{Task,Nothing}
    server_active::Bool
    server_delivered_base::Int
    processing::Int
end

function Subscription(client::C, sid::Int, subject::String, queue::Union{String,Nothing}, callback,
                      borrowed_callback::Bool,
                      lock::ReentrantLock,
                      messages::MsgQueue{Msg}, condition::Base.GenericCondition{ReentrantLock},
                      control_handler::_SubscriptionControlHandler,
                      pending_msgs_limit::Int, pending_bytes_limit::Int, pending_bytes::Int,
                      received::Int, delivered::Int, dropped_msgs::Int, max_msgs::Int,
                      closed::Bool, processor::Union{Task,Nothing}, server_active::Bool,
                      server_delivered_base::Int, processing::Int) where {C<:AbstractNatterClient}
    Subscription{C}(client, lock, sid, subject, queue, !isnothing(callback),
                    borrowed_callback, _borrowed_callback_handler(client, borrowed_callback ? callback : nothing),
                    messages, condition, _ConditionTimeoutQueue(), control_handler,
                    pending_msgs_limit, pending_bytes_limit, pending_bytes, received,
                    delivered, dropped_msgs, max_msgs, closed, processor, server_active,
                    server_delivered_base, processing)
end

struct _SubscriptionSnapshotEntry{C<:AbstractNatterClient}
    sub::Subscription{C}
    borrow_payload::Bool
    fast_control::Bool
end

function _SubscriptionSnapshotEntry(sub::Subscription{C}) where {C<:AbstractNatterClient}
    _SubscriptionSnapshotEntry{C}(sub, sub.borrowed_callback,
                                  _uses_pre_payload_control(sub.control_handler))
end

mutable struct PongWaiter
    condition::Base.GenericCondition{ReentrantLock}
    ready::Bool
    value::Bool
    active::Bool
end

PongWaiter(condition::Base.GenericCondition{ReentrantLock}) = PongWaiter(condition, false, false, true)

mutable struct PongWaiterQueue
    buffer::Vector{Union{PongWaiter,Nothing}}
    head::Int
    len::Int
end

PongWaiterQueue() = PongWaiterQueue(Union{PongWaiter,Nothing}[], 1, 0)

Base.eltype(::Type{PongWaiterQueue}) = PongWaiter
Base.length(q::PongWaiterQueue) = q.len
Base.isempty(q::PongWaiterQueue) = q.len == 0
Base.IteratorSize(::Type{PongWaiterQueue}) = Base.HasLength()
Base.firstindex(::PongWaiterQueue) = 1
Base.lastindex(q::PongWaiterQueue) = q.len

function _resize_pong_waiter_queue!(q::PongWaiterQueue, capacity::Int)
    capacity = max(capacity, q.len)
    buffer = Vector{Union{PongWaiter,Nothing}}(undef, capacity)
    fill!(buffer, nothing)
    for i in 1:q.len
        buffer[i] = q[i]
    end
    q.buffer = buffer
    q.head = 1
    q
end

function Base.getindex(q::PongWaiterQueue, i::Int)
    1 <= i <= q.len || throw(BoundsError(q, i))
    idx = mod1(q.head + i - 1, length(q.buffer))
    q.buffer[idx]::PongWaiter
end

function Base.iterate(q::PongWaiterQueue, state::Int=1)
    state > q.len && return nothing
    q[state], state + 1
end

function Base.push!(q::PongWaiterQueue, waiter::PongWaiter)
    if q.len == length(q.buffer)
        _resize_pong_waiter_queue!(q, max(8, 2 * max(q.len, 1)))
    end
    idx = mod1(q.head + q.len, length(q.buffer))
    q.buffer[idx] = waiter
    q.len += 1
    q
end

function Base.popfirst!(q::PongWaiterQueue)
    isempty(q) && throw(ArgumentError("PONG waiter queue is empty"))
    waiter = q.buffer[q.head]::PongWaiter
    q.buffer[q.head] = nothing
    q.len -= 1
    q.head = q.len == 0 ? 1 : mod1(q.head + 1, length(q.buffer))
    waiter
end

function Base.empty!(q::PongWaiterQueue)
    fill!(q.buffer, nothing)
    q.head = 1
    q.len = 0
    q
end

function _filter_pong_waiter_queue!(f::Function, q::PongWaiterQueue)
    kept = PongWaiter[]
    sizehint!(kept, q.len)
    for waiter in q
        f(waiter) && push!(kept, waiter)
    end
    empty!(q)
    for waiter in kept
        push!(q, waiter)
    end
    q
end

mutable struct RequestWaiter{C<:AbstractNatterClient}
    ready::Bool
    value::Union{Msg,Exception,Nothing}
    active::Bool
    deadline::Float64
    reply::String
end

RequestWaiter{C}(deadline::Real=Inf, reply::String="") where {C<:AbstractNatterClient} =
    RequestWaiter{C}(false, nothing, true, Float64(deadline), reply)

mutable struct RequestMux{C<:AbstractNatterClient}
    prefix::String
    sub::Subscription{C}
    waiters::Dict{Int,RequestWaiter{C}}
    condition::Base.GenericCondition{ReentrantLock}
    timeout_condition::Base.GenericCondition{ReentrantLock}
    next_token::Int
    deadline_queue::_DeadlineQueue{RequestWaiter{C}}
    timeout_task::Union{Task,Nothing}
end

mutable struct Client{Options<:ConnectOptions,ReadIO,WriteIO} <: AbstractNatterClient
    options::Options
    servers::Vector{Server}
    current_server::Union{Server,Nothing}
    connected_url::Union{String,Nothing}
    status::ConnectionStatus.T
    status_code::Threads.Atomic{Int}
    info::ServerInfo
    max_payload_value::Threads.Atomic{Int}
    headers_supported::Threads.Atomic{Bool}
    socket::Union{Sockets.TCPSocket,Nothing}
    read_io::Union{ReadIO,Nothing}
    reader::Union{ProtocolReader{<:ReadIO},Nothing}
    @atomic write_io::Union{WriteIO,Nothing}
    lock::ReentrantLock
    write_lock::ReentrantLock
    write_condition::Base.GenericCondition{ReentrantLock}
    write_waiters::Threads.Atomic{Int}
    write_reconnect_pending::Threads.Atomic{Bool}
    write_scratch::Vector{UInt8}
    write_deadline::Threads.Atomic{Float64}
    write_epoch::Threads.Atomic{Int}
    write_timed_out_epoch::Threads.Atomic{Int}
    write_timeout_io::Base.RefValue{Any}
    write_timeout_operation::Base.RefValue{String}
    write_timeout_task::Union{Task,Nothing}
    @atomic flush_signal::FlushSignal
    flusher_task::Union{Task,Nothing}
    writer_queue::Union{_WriteQueue,Nothing}
    writer_task::Union{Task,Nothing}
    sid::Int
    subscriptions::Dict{Int,Subscription{Client{Options,ReadIO,WriteIO}}}
    @atomic subscription_snapshot::Dict{Int,_SubscriptionSnapshotEntry{Client{Options,ReadIO,WriteIO}}}
    subscription_replay_lock::ReentrantLock
    @atomic request_mux::Union{RequestMux{Client{Options,ReadIO,WriteIO}},Nothing}
    request_mux_lock::ReentrantLock
    pending::PendingBuffer
    @atomic pending_bytes::Int
    pongs::PongWaiterQueue
    reader_task::Union{Task,Nothing}
    ping_task::Union{Task,Nothing}
    reconnect_task::Union{Task,Nothing}
    pings_out::Int
    stats::AtomicStats
    rng::MersenneTwister
    generation::Int
    generation_value::Threads.Atomic{Int}
    lifecycle_watchers::Vector{WeakRef}
end

function Client(options::Options, servers::Vector{Server}, current_server::Union{Server,Nothing},
                connected_url::Union{String,Nothing}, status::ConnectionStatus.T,
                info::ServerInfo, socket::Union{Sockets.TCPSocket,Nothing}, read_io, write_io,
                lock::ReentrantLock, write_lock::ReentrantLock,
                write_condition::Base.GenericCondition{ReentrantLock}, flush_signal::FlushSignal,
                flusher_task::Union{Task,Nothing}, sid::Int, subscriptions,
                subscription_replay_lock::ReentrantLock,
                request_mux::Union{RequestMux,Nothing}, request_mux_lock::ReentrantLock,
                pending, pending_bytes::Int, pongs::PongWaiterQueue,
                reader_task::Union{Task,Nothing}, ping_task::Union{Task,Nothing},
                reconnect_task::Union{Task,Nothing}, pings_out::Int, stats,
                rng::MersenneTwister, generation::Int) where {Options<:ConnectOptions}
    ReadIO = _transport_field_type(read_io)
    WriteIO = _write_transport_field_type(write_io)
    client_type = Client{Options,ReadIO,WriteIO}
    typed_subscriptions = Dict{Int,Subscription{client_type}}()
    for (sub_sid, sub) in subscriptions
        typed_subscriptions[Int(sub_sid)] = sub
    end
    subscription_snapshot = Dict{Int,_SubscriptionSnapshotEntry{client_type}}()
    sizehint!(subscription_snapshot, length(typed_subscriptions))
    for (sub_sid, sub) in typed_subscriptions
        if sub_sid > 0
            subscription_snapshot[sub_sid] = _SubscriptionSnapshotEntry(sub)
        end
    end
    typed_request_mux = if isnothing(request_mux)
        nothing
    else
        sub = typed_subscriptions[request_mux.sub.sid]
        waiters = Dict{Int,RequestWaiter{client_type}}()
        deadline_queue = _DeadlineQueue{RequestWaiter{client_type}}()
        for (token, waiter) in request_mux.waiters
            typed_waiter = RequestWaiter{client_type}(waiter.deadline, waiter.reply)
            typed_waiter.ready = waiter.ready
            typed_waiter.active = waiter.active
            if waiter.value isa Exception || isnothing(waiter.value)
                typed_waiter.value = waiter.value
            end
            waiters[token] = typed_waiter
            typed_waiter.active &&
                _deadline_queue_push!(deadline_queue, token, typed_waiter.deadline, typed_waiter)
        end
        mux_lock = ReentrantLock()
        RequestMux{client_type}(request_mux.prefix, sub, waiters,
                                Base.Threads.Condition(mux_lock),
                                Base.Threads.Condition(mux_lock),
                                request_mux.next_token, deadline_queue, nothing)
    end
    reader = isnothing(read_io) ? nothing : ProtocolReader(read_io; read_size=options.read_buffer_size,
                                                           shrink_threshold=options.read_buffer_shrink_threshold)
    pending = _pending_buffer_from(pending)
    atomic_stats = stats isa AtomicStats ? stats : AtomicStats(stats)
    writer_queue = options.write_driver ?
                   _WriteQueue(options.write_queue_msgs, options.write_queue_bytes) : nothing
    Client{Options,ReadIO,WriteIO}(options, servers, current_server, connected_url, status,
                                   Threads.Atomic{Int}(Int(status)),
                                   info, Threads.Atomic{Int}(something(info.max_payload, typemax(Int))),
                                   Threads.Atomic{Bool}(info.headers === true),
                                   socket, read_io, reader, write_io, lock, write_lock,
                                   write_condition, Threads.Atomic{Int}(0),
                                   Threads.Atomic{Bool}(false),
                                   UInt8[], Threads.Atomic{Float64}(Inf), Threads.Atomic{Int}(0),
                                   Threads.Atomic{Int}(0), Ref{Any}(nothing), Ref(""), nothing,
                                   flush_signal, flusher_task, writer_queue, nothing,
                                   sid, typed_subscriptions, subscription_snapshot,
                                   subscription_replay_lock, typed_request_mux, request_mux_lock,
                                   pending, pending_bytes, pongs,
                                   reader_task, ping_task, reconnect_task, pings_out, atomic_stats,
                                   rng, generation, Threads.Atomic{Int}(generation), WeakRef[])
end

function _set_subscription_snapshot_locked!(client::C, sid::Int,
                                            sub::Union{Subscription{C},Nothing}) where {C<:Client}
    sid > 0 || return nothing
    current = @atomic client.subscription_snapshot
    existing = get(current, sid, nothing)
    if isnothing(sub)
        isnothing(existing) && return nothing
        snapshot = copy(current)
        delete!(snapshot, sid)
        @atomic client.subscription_snapshot = snapshot
        return nothing
    end
    entry = _SubscriptionSnapshotEntry(sub)
    if !isnothing(existing) && existing.sub === sub &&
       existing.borrow_payload == entry.borrow_payload &&
       existing.fast_control == entry.fast_control
        return nothing
    end
    snapshot = copy(current)
    snapshot[sid] = entry
    @atomic client.subscription_snapshot = snapshot
    nothing
end

function _register_client_lifecycle_watcher!(client::Client, watcher)
    ref = WeakRef(watcher)
    @lock client.lock begin
        refs = client.lifecycle_watchers
        live = 1
        for i in eachindex(refs)
            if !isnothing(refs[i].value)
                refs[live] = refs[i]
                live += 1
            end
        end
        resize!(refs, live - 1)
        push!(refs, ref)
    end
    nothing
end

_client_lifecycle_error!(_watcher, _err::Exception) = nothing

function _notify_client_lifecycle_watchers!(client::Client, err::Exception)
    watchers = Any[]
    @lock client.lock begin
        refs = client.lifecycle_watchers
        live = 1
        for i in eachindex(refs)
            watcher = refs[i].value
            if !isnothing(watcher)
                refs[live] = refs[i]
                live += 1
                push!(watchers, watcher)
            end
        end
        resize!(refs, live - 1)
    end
    for watcher in watchers
        _client_lifecycle_error!(watcher, err)
    end
    nothing
end

@inline function _lookup_subscription_snapshot_entry(
    client::C, sid::Int
)::Union{_SubscriptionSnapshotEntry{C},Nothing} where {C<:Client}
    sid > 0 || return nothing
    snapshot = @atomic client.subscription_snapshot
    get(snapshot, sid, nothing)
end

@inline function _lookup_subscription(client::C, sid::Int) where {C<:Client}
    entry = _lookup_subscription_snapshot_entry(client, sid)
    isnothing(entry) ? nothing : entry.sub
end

@inline function _borrow_payload_for_sid(client::C, sid::Int)::Bool where {C<:Client}
    entry = _lookup_subscription_snapshot_entry(client, sid)
    isnothing(entry) ? false : entry.borrow_payload
end

@inline function _fast_control_subscription_for_sid(client::C, sid::Int)::Bool where {C<:Client}
    entry = _lookup_subscription_snapshot_entry(client, sid)
    isnothing(entry) ? false : entry.fast_control
end

function _wait_until_condition_locked(predicate::Function, condition::Base.GenericCondition{ReentrantLock},
                                      timeout::Real; cancel_token::MaybeCancellationToken=nothing)::Bool
    _throw_if_cancelled(cancel_token)
    predicate() && return true
    seconds = Float64(timeout)
    seconds > 0 || return false
    registration = _register_cancellation_waiter(cancel_token, condition)
    timed_out = Ref(false)
    timer = isfinite(seconds) ? Timer(seconds) do _
        lock(condition)
        try
            timed_out[] = true
            notify(condition; all=true)
        finally
            unlock(condition)
        end
    end : nothing
    try
        while !predicate()
            _throw_if_cancelled(cancel_token)
            timed_out[] && return false
            wait(condition)
        end
        _throw_if_cancelled(cancel_token)
        true
    finally
        _deregister_cancellation_waiter!(cancel_token, registration)
        isnothing(timer) || close(timer)
    end
end

function _wait_condition_timeout_queue_locked(predicate::Function,
                                             condition::Base.GenericCondition{ReentrantLock},
                                             queue::_ConditionTimeoutQueue,
                                             timeout::Real;
                                             cancel_token::MaybeCancellationToken=nothing)::Bool
    _throw_if_cancelled(cancel_token)
    predicate() && return true
    seconds = Float64(timeout)
    seconds > 0 || return false
    if !isfinite(seconds)
        return _wait_until_notified_locked(predicate, condition; cancel_token)
    end

    registration = _register_cancellation_waiter(cancel_token, condition)
    waiter = _register_condition_timeout_locked!(condition, queue, seconds)
    try
        while !predicate()
            _throw_if_cancelled(cancel_token)
            waiter.timed_out && return false
            wait(condition)
        end
        _throw_if_cancelled(cancel_token)
        true
    finally
        _deregister_condition_timeout_locked!(queue, waiter)
        _deregister_cancellation_waiter!(cancel_token, registration)
    end
end

function _wait_subscription_condition_locked(predicate::Function, sub::Subscription,
                                             timeout::Real;
                                             cancel_token::MaybeCancellationToken=nothing)::Bool
    _wait_condition_timeout_queue_locked(predicate, sub.condition, sub.timeout_queue, timeout;
                                         cancel_token)
end

function _wait_until_notified_locked(predicate::Function,
                                     condition::Base.GenericCondition{ReentrantLock};
                                     cancel_token::MaybeCancellationToken=nothing)::Bool
    _throw_if_cancelled(cancel_token)
    predicate() && return true
    registration = _register_cancellation_waiter(cancel_token, condition)
    try
        while !predicate()
            _throw_if_cancelled(cancel_token)
            wait(condition)
        end
        _throw_if_cancelled(cancel_token)
        true
    finally
        _deregister_cancellation_waiter!(cancel_token, registration)
    end
end

function _sleep_or_cancel(seconds::Real, cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    delay = Float64(seconds)
    delay <= 0 && return nothing
    lock = ReentrantLock()
    condition = Base.Threads.Condition(lock)
    lock(condition)
    try
        _wait_until_condition_locked(() -> false, condition, delay; cancel_token)
    finally
        unlock(condition)
    end
    nothing
end

function _notify_subscription_waiters_locked(sub::Subscription; all::Bool=false)
    notify(sub.condition; all)
    nothing
end

function _notify_subscription_waiters!(sub::Subscription; all::Bool=false)
    @lock sub.lock _notify_subscription_waiters_locked(sub; all)
    nothing
end

function _resolve_pong_waiter_locked!(waiter::PongWaiter, value::Bool)
    waiter.active || return false
    waiter.value = value
    waiter.ready = true
    waiter.active = false
    notify(waiter.condition)
    true
end

function _resolve_request_waiter_locked!(waiter::RequestWaiter{C},
                                         value::Union{Msg,Exception},
                                         condition::Base.GenericCondition{ReentrantLock}) where {C<:AbstractNatterClient}
    waiter.active || return false
    waiter.value = value
    waiter.ready = true
    waiter.active = false
    notify(condition; all=true)
    true
end

@inline _load_status(client::Client)::ConnectionStatus.T = ConnectionStatus.T(client.status_code[])
@inline function _store_status_locked!(client::Client, value::ConnectionStatus.T)
    value == ConnectionStatus.CONNECTED || (client.write_reconnect_pending[] = false)
    client.status = value
    client.status_code[] = Int(value)
    value
end
@inline _load_generation(client::Client)::Int = client.generation_value[]

@inline function _replace_flush_signal_locked!(client::Client)
    old = @atomic client.flush_signal
    @atomic client.flush_signal = FlushSignal()
    isopen(old) && close(old)
    @atomic client.flush_signal
end

@inline function _store_generation_locked!(client::Client, value::Int)
    client.generation == value || _replace_flush_signal_locked!(client)
    client.generation = value
    client.generation_value[] = value
    value
end
@inline _bump_generation_locked!(client::Client)::Int =
    _store_generation_locked!(client, client.generation + 1)
@inline _client_max_payload(client::Client)::Int = client.max_payload_value[]
@inline _client_headers_supported(client::Client)::Bool = client.headers_supported[]

@inline function _sync_server_info_cache_locked!(client::Client)
    client.max_payload_value[] = something(client.info.max_payload, typemax(Int))
    client.headers_supported[] = client.info.headers === true
    nothing
end

status(client::Client) = _load_status(client)
stats(client::Client) = Stats(; in_msgs=_stat_get(client.stats.in_msgs),
                              out_msgs=_stat_get(client.stats.out_msgs),
                              in_bytes=_stat_get(client.stats.in_bytes),
                              out_bytes=_stat_get(client.stats.out_bytes),
                              reconnects=_stat_get(client.stats.reconnects),
                              errors=_stat_get(client.stats.errors),
                              dropped_msgs=_stat_get(client.stats.dropped_msgs))
function stats(sub::Subscription)
    @lock sub.lock begin
        SubscriptionStats(; pending_msgs=Base.n_avail(sub.messages),
                          pending_bytes=sub.pending_bytes,
                          processing=sub.processing,
                          received=sub.received,
                          delivered=sub.delivered,
                          dropped_msgs=sub.dropped_msgs,
                          max_msgs=sub.max_msgs,
                          closed=sub.closed,
                          server_active=sub.server_active)
    end
end
connected_url(client::Client) = (@lock client.lock client.connected_url)
