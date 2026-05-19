mutable struct ProtocolReader{I}
    io::I
    buffer::Vector{UInt8}
    first::Int
    last::Int
    scratch::Vector{UInt8}
    shrink_threshold::Int
    subject_cache::Dict{Int,String}
end

ProtocolReader(io::I; read_size::Int=DEFAULT_READ_BUFFER_SIZE,
               shrink_threshold::Int=4 * read_size) where {I} =
    ProtocolReader{I}(io; read_size, shrink_threshold)

ProtocolReader{I}(io; read_size::Int=DEFAULT_READ_BUFFER_SIZE,
                  shrink_threshold::Int=4 * read_size) where {I} =
    ProtocolReader{I}(io, UInt8[], 1, 0,
                      Vector{UInt8}(undef, _connect_option_positive_int("read_size", read_size)),
                      max(_connect_option_positive_int("shrink_threshold", shrink_threshold),
                          _connect_option_positive_int("read_size", read_size)),
                      Dict{Int,String}())

abstract type _ProtocolFrame end

struct MsgFrame{M<:_ProtocolMsg} <: _ProtocolFrame
    msg::M
end

struct InfoFrame <: _ProtocolFrame
    info::ServerInfo
end

struct ErrFrame <: _ProtocolFrame
    err::String
end

struct PingFrame <: _ProtocolFrame end
struct PongFrame <: _ProtocolFrame end
struct OkFrame <: _ProtocolFrame end

@inline _protocol_msg_frame(msg::_ProtocolMsg) = MsgFrame(msg)
@inline _protocol_info_frame(info::ServerInfo) = InfoFrame(info)
@inline _protocol_err_frame(err::AbstractString) = ErrFrame(String(err))
@inline _protocol_control_frame(::Val{:PING}) = PingFrame()
@inline _protocol_control_frame(::Val{:PONG}) = PongFrame()
@inline _protocol_control_frame(::Val{:OK}) = OkFrame()
@inline _protocol_control_frame(op::Symbol) = _protocol_control_frame(Val(op))

@inline _protocol_msg(frame::MsgFrame) = frame.msg
@inline _protocol_info(frame::InfoFrame)::ServerInfo = frame.info
@inline _protocol_err(frame::ErrFrame)::String = frame.err

@inline _protocol_op(::MsgFrame) = :MSG
@inline _protocol_op(::InfoFrame) = :INFO
@inline _protocol_op(::ErrFrame) = :ERR
@inline _protocol_op(::PingFrame) = :PING
@inline _protocol_op(::PongFrame) = :PONG
@inline _protocol_op(::OkFrame) = :OK

function Base.getproperty(frame::_ProtocolFrame, name::Symbol)
    name === :op && return _protocol_op(frame)
    getfield(frame, name)
end

mutable struct BufferedWriteIO{I} <: IO
    io::I
    buffer::IOBuffer
    replayable_entries::Vector{_ReplayableEntry}
    replayable_bytes::Int
    closed::Bool
end

BufferedWriteIO(io::I) where {I} = BufferedWriteIO{I}(io, IOBuffer(), _ReplayableEntry[], 0, false)
BufferedWriteIO(io::I, capacity::Integer) where {I} =
    BufferedWriteIO{I}(io, IOBuffer(sizehint=max(0, Int(capacity))), _ReplayableEntry[], 0, false)

function _ensure_open(io::BufferedWriteIO)
    io.closed && throw(Base.IOError("buffered write transport is closed", 0))
    nothing
end

function Base.unsafe_write(io::BufferedWriteIO, p::Ptr{UInt8}, n::UInt)
    _ensure_open(io)
    unsafe_write(io.buffer, p, n)
end

function Base.write(io::BufferedWriteIO, data::Vector{UInt8})
    _ensure_open(io)
    write(io.buffer, data)
end

function Base.write(io::BufferedWriteIO, data::ImmutableBytes)
    _ensure_open(io)
    write(io.buffer, data.data)
end

function Base.write(io::BufferedWriteIO, data::Base.CodeUnits{UInt8})
    _ensure_open(io)
    write(io.buffer, data)
end

function Base.write(io::BufferedWriteIO, data::String)
    _ensure_open(io)
    write(io.buffer, data)
end

function Base.write(io::BufferedWriteIO, data::SubString{String})
    _ensure_open(io)
    write(io.buffer, data)
end

function Base.write(io::BufferedWriteIO, data::AbstractString)
    _ensure_open(io)
    write(io.buffer, data)
end

function Base.write(io::BufferedWriteIO, byte::UInt8)
    _ensure_open(io)
    write(io.buffer, byte)
end

function Base.write(io::BufferedWriteIO, ch::Char)
    _ensure_open(io)
    write(io.buffer, ch)
end

function _write_buffered_bytes(io, data::AbstractVector{UInt8}, n::Int)
    try
        unsafe_write(io, pointer(data), UInt(n))
    catch err
        if err isa MethodError || (err isa ErrorException && occursin("does not support byte I/O", err.msg))
            bytes = Vector{UInt8}(undef, n)
            copyto!(bytes, 1, data, 1, n)
            write(io, bytes)
        else
            rethrow()
        end
    end
end

function Base.flush(io::BufferedWriteIO)
    _ensure_open(io)
    n = position(io.buffer)
    if n > 0
        _write_buffered_bytes(io.io, io.buffer.data, n)
    end
    flush(io.io)
    truncate(io.buffer, 0)
    seekstart(io.buffer)
    empty!(io.replayable_entries)
    io.replayable_bytes = 0
    nothing
end

function Base.close(io::BufferedWriteIO)
    io.closed = true
    close(io.io)
end

Base.isopen(io::BufferedWriteIO) = !io.closed && isopen(io.io)

_buffered_bytes(::IO) = 0
_buffered_bytes(io::BufferedWriteIO) = position(io.buffer)
_take_replayable_writes!(::IO) = _PendingEntry[]
function _take_replayable_writes!(io::BufferedWriteIO)
    replayable = Vector{_PendingEntry}(undef, length(io.replayable_entries))
    for (i, entry) in pairs(io.replayable_entries)
        replayable[i] = _copy_replayable_entry(io.buffer.data, entry)
    end
    truncate(io.buffer, 0)
    seekstart(io.buffer)
    empty!(io.replayable_entries)
    io.replayable_bytes = 0
    replayable
end

_take_replayable_bytes!(::IO) = 0
function _take_replayable_bytes!(io::BufferedWriteIO)
    bytes = io.replayable_bytes
    truncate(io.buffer, 0)
    seekstart(io.buffer)
    empty!(io.replayable_entries)
    io.replayable_bytes = 0
    bytes
end
_underlying_transport(io) = io
_underlying_transport(io::BufferedWriteIO) = io.io

mutable struct FlushSignal
    event::Base.Event
    pending::Threads.Atomic{Bool}
    closed::Threads.Atomic{Bool}
end

FlushSignal() = FlushSignal(Base.Event(true), Threads.Atomic{Bool}(false), Threads.Atomic{Bool}(false))

Base.isopen(signal::FlushSignal) = !signal.closed[]
Base.isready(signal::FlushSignal) = signal.pending[]

function Base.close(signal::FlushSignal)
    Threads.atomic_xchg!(signal.closed, true)
    notify(signal.event)
    nothing
end

function _notify_flush_signal(signal::FlushSignal)::Bool
    signal.closed[] && return false
    was_pending = Threads.atomic_xchg!(signal.pending, true)
    was_pending || notify(signal.event)
    true
end

function _throw_closed_flush_signal()
    throw(InvalidStateException("flush signal is closed", :closed))
end

function _wait_flush_signal(signal::FlushSignal)
    while true
        signal.closed[] && _throw_closed_flush_signal()
        signal.pending[] && return nothing
        wait(signal.event)
    end
end

function _consume_flush_signal!(signal::FlushSignal)
    signal.closed[] && _throw_closed_flush_signal()
    Threads.atomic_xchg!(signal.pending, false)
    nothing
end

function Base.take!(signal::FlushSignal)
    _wait_flush_signal(signal)
    _consume_flush_signal!(signal)
    true
end

mutable struct _WriteQueue
    condition::Base.GenericCondition{ReentrantLock}
    items::Vector{Any}
    sizes::Vector{Int}
    head::Int
    tail::Int
    len::Int
    bytes::Int
    in_flight::Int
    epoch::Int
    pending_count::Threads.Atomic{Int}
    max_msgs::Int
    max_bytes::Int
    active::Threads.Atomic{Bool}
    closed::Threads.Atomic{Bool}
end

function _WriteQueue(max_msgs::Int, max_bytes::Int)
    max_msgs > 0 || throw(ArgumentError("write_queue_msgs must be positive"))
    max_bytes > 0 || throw(ArgumentError("write_queue_bytes must be positive"))
    lock = ReentrantLock()
    items = Vector{Any}(undef, max_msgs)
    fill!(items, nothing)
    _WriteQueue(Base.Threads.Condition(lock), items, fill(0, max_msgs),
                1, 1, 0, 0, 0, 0, Threads.Atomic{Int}(0), max_msgs, max_bytes,
                Threads.Atomic{Bool}(false), Threads.Atomic{Bool}(false))
end

Base.isopen(q::_WriteQueue) = !q.closed[]
Base.isready(q::_WriteQueue) = q.len > 0
Base.length(q::_WriteQueue) = q.len
_write_queue_capacity(q::_WriteQueue)::Int = length(q.items)

@inline function _write_queue_next_index(q::_WriteQueue, index::Int)::Int
    index == length(q.items) ? 1 : index + 1
end

function Base.empty!(q::_WriteQueue)
    fill!(q.items, nothing)
    fill!(q.sizes, 0)
    q.head = 1
    q.tail = 1
    q.len = 0
    q.bytes = 0
    q.in_flight = 0
    q.pending_count[] = 0
    q
end

mutable struct PendingBuffer
    chunks::Vector{_PendingEntry}
    head::Int
    len::Int
end

PendingBuffer() = PendingBuffer(_PendingEntry[], 1, 0)

Base.isempty(buffer::PendingBuffer) = buffer.len == 0

function Base.empty!(buffer::PendingBuffer)
    empty!(buffer.chunks)
    buffer.head = 1
    buffer.len = 0
    buffer
end

@inline _pending_buffer_capacity(buffer::PendingBuffer)::Int = length(buffer.chunks)

@inline function _pending_buffer_index(buffer::PendingBuffer, offset::Int)::Int
    mod1(buffer.head + offset, _pending_buffer_capacity(buffer))
end

function _resize_pending_buffer!(buffer::PendingBuffer, capacity::Int)
    capacity = max(capacity, buffer.len)
    if capacity == 0
        empty!(buffer)
        return buffer
    end

    chunks = Vector{_PendingEntry}(undef, capacity)
    fill!(chunks, EMPTY_PENDING_ENTRY)
    for i in 1:buffer.len
        chunks[i] = buffer.chunks[_pending_buffer_index(buffer, i - 1)]
    end
    buffer.chunks = chunks
    buffer.head = 1
    buffer
end

function _ensure_pending_buffer_capacity!(buffer::PendingBuffer, needed::Int)
    needed <= _pending_buffer_capacity(buffer) && return buffer
    capacity = max(8, _pending_buffer_capacity(buffer))
    while capacity < needed
        capacity *= 2
    end
    _resize_pending_buffer!(buffer, capacity)
end

function _compact_pending_buffer!(buffer::PendingBuffer)
    if buffer.len == 0
        empty!(buffer)
    elseif _pending_buffer_capacity(buffer) > max(64, 4 * buffer.len)
        _resize_pending_buffer!(buffer, max(8, 2 * buffer.len))
    end
    buffer
end

function _push_pending_chunk!(buffer::PendingBuffer, entry::_PendingEntry)
    _ensure_pending_buffer_capacity!(buffer, buffer.len + 1)
    buffer.chunks[_pending_buffer_index(buffer, buffer.len)] = entry
    buffer.len += 1
    buffer
end
_push_pending_chunk!(buffer::PendingBuffer, data::Vector{UInt8}) =
    _push_pending_chunk!(buffer, _PendingEntry(data))

function _prepend_pending_chunk!(buffer::PendingBuffer, entry::_PendingEntry)
    _ensure_pending_buffer_capacity!(buffer, buffer.len + 1)
    buffer.head = buffer.head == 1 ? _pending_buffer_capacity(buffer) : buffer.head - 1
    buffer.chunks[buffer.head] = entry
    buffer.len += 1
    buffer
end
_prepend_pending_chunk!(buffer::PendingBuffer, data::Vector{UInt8}) =
    _prepend_pending_chunk!(buffer, _PendingEntry(data))

function _prepend_pending_chunks!(buffer::PendingBuffer, entries::AbstractVector{<:_PendingEntry})
    for i in length(entries):-1:1
        _prepend_pending_chunk!(buffer, entries[i])
    end
    buffer
end

function _pop_pending_batch!(buffer::PendingBuffer, max_bytes::Int)::Vector{_PendingEntry}
    isempty(buffer) && return _PendingEntry[]
    max_bytes = max(1, max_bytes)
    count = 0
    total = 0
    while count < buffer.len
        chunk = buffer.chunks[_pending_buffer_index(buffer, count)]
        chunk_size = _pending_entry_size(chunk)
        if total > 0 && total + chunk_size > max_bytes
            break
        end
        count += 1
        total += chunk_size
        total >= max_bytes && break
    end

    out = Vector{_PendingEntry}(undef, count)
    old_head = buffer.head
    for i in 1:count
        idx = mod1(old_head + i - 1, _pending_buffer_capacity(buffer))
        chunk = buffer.chunks[idx]
        out[i] = chunk
        buffer.chunks[idx] = EMPTY_PENDING_ENTRY
    end
    buffer.len -= count
    if buffer.len == 0
        empty!(buffer)
    else
        buffer.head = mod1(old_head + count, _pending_buffer_capacity(buffer))
        _compact_pending_buffer!(buffer)
    end
    out
end

function Base.take!(buffer::PendingBuffer)
    isempty(buffer) && return UInt8[]
    total = 0
    for i in 1:buffer.len
        total += _pending_entry_size(buffer.chunks[_pending_buffer_index(buffer, i - 1)])
    end
    out = Vector{UInt8}(undef, total)
    pos = 1
    for i in 1:buffer.len
        chunk = buffer.chunks[_pending_buffer_index(buffer, i - 1)]
        pos = _copy_pending_entry_bytes!(out, pos, chunk)
    end
    empty!(buffer)
    out
end

function Base.write(buffer::PendingBuffer, data::Vector{UInt8})
    _push_pending_chunk!(buffer, _PendingEntry(copy(data)))
    length(data)
end

function Base.write(buffer::PendingBuffer, data::AbstractString)
    bytes = Vector{UInt8}(undef, ncodeunits(data))
    copyto!(bytes, 1, codeunits(data), 1, length(bytes))
    _push_pending_chunk!(buffer, _PendingEntry(bytes))
    length(bytes)
end

function _pending_buffer_from(buffer::PendingBuffer)
    buffer
end

function _pending_buffer_from(buffer::IOBuffer)
    data = take!(buffer)
    pending = PendingBuffer()
    isempty(data) || _push_pending_chunk!(pending, _PendingEntry(data))
    pending
end

const DefaultTransportIO = Union{Sockets.TCPSocket,MbedTLS.SSLContext}
const DefaultWriteTransportIO = Union{DefaultTransportIO,BufferedWriteIO{Sockets.TCPSocket},BufferedWriteIO{MbedTLS.SSLContext}}

_transport_field_type(::Nothing) = DefaultTransportIO
_transport_field_type(io) = typeof(io)
_write_transport_field_type(::Nothing) = DefaultWriteTransportIO
_write_transport_field_type(io) = typeof(io)

struct _NoSubscriptionControlHandler end
mutable struct _JetStreamPushControlHandler
    idle_heartbeat::Threads.Atomic{Float64}
    flow_control::Threads.Atomic{Bool}
    last_seen::Threads.Atomic{Float64}
    consumer_deleted::Threads.Atomic{Bool}
    flow_incoming::Threads.Atomic{UInt64}
    flow_delivered::UInt64
    flow_reply::Union{String,Nothing}
    flow_target::UInt64
    ordered::Bool
    next_consumer_seq::Int
    last_stream_seq::Int
    sequence_state_anchored::Bool
    ordered_resetting::Bool
    ordered_reset_callback::Union{Nothing,Function}
    lock::ReentrantLock
end
_JetStreamPushControlHandler(idle_heartbeat::Real=0.0; flow_control::Bool=true) =
    _JetStreamPushControlHandler(Threads.Atomic{Float64}(Float64(idle_heartbeat)),
                                 Threads.Atomic{Bool}(flow_control),
                                 Threads.Atomic{Float64}(time()),
                                 Threads.Atomic{Bool}(false),
                                 Threads.Atomic{UInt64}(UInt64(0)),
                                 UInt64(0), nothing, UInt64(0),
                                 false, 1, 0, false, false, nothing,
                                 ReentrantLock())
struct _RequestMuxControlHandler end
struct _JetStreamAsyncPublishControlHandler{S}
    state::S
end

const _SubscriptionControlHandler = Union{_NoSubscriptionControlHandler,_JetStreamPushControlHandler,_RequestMuxControlHandler,_JetStreamAsyncPublishControlHandler}

_uses_pre_payload_control(::_SubscriptionControlHandler)::Bool = false
_uses_pre_payload_control(::_RequestMuxControlHandler)::Bool = true
_uses_pre_payload_control(::_JetStreamAsyncPublishControlHandler)::Bool = true

mutable struct MsgQueue{T}
    buffer::Vector{T}
    empty::T
    head::Int
    tail::Int
    len::Int
    closed::Bool
end

function MsgQueue{T}(capacity::Int, empty::T) where {T}
    capacity > 0 || throw(ArgumentError("message queue capacity must be positive"))
    buffer = Vector{T}(undef, capacity)
    fill!(buffer, empty)
    MsgQueue{T}(buffer, empty, 1, 1, 0, false)
end

MsgQueue{Msg}(capacity::Int) = MsgQueue{Msg}(capacity, EMPTY_MSG)
MsgQueue{T}(capacity::Int) where {T} =
    throw(ArgumentError("MsgQueue{$T} requires an empty sentinel value"))

Base.isopen(q::MsgQueue) = !q.closed
Base.isready(q::MsgQueue) = q.len > 0
Base.n_avail(q::MsgQueue) = q.len
Base.length(q::MsgQueue) = q.len
_queue_capacity(q::MsgQueue) = length(q.buffer)

function Base.close(q::MsgQueue)
    q.closed = true
    q
end

@inline function _queue_next_index(q::MsgQueue, index::Int)::Int
    index == length(q.buffer) ? 1 : index + 1
end

function Base.put!(q::MsgQueue{T}, msg::T) where {T}
    q.closed && throw(InvalidStateException("message queue is closed", :closed))
    q.len < length(q.buffer) || throw(InvalidStateException("message queue is full", :open))
    q.buffer[q.tail] = msg
    q.tail = _queue_next_index(q, q.tail)
    q.len += 1
    q
end

function Base.take!(q::MsgQueue{T}) where {T}
    q.len > 0 || throw(InvalidStateException("message queue is empty", q.closed ? :closed : :open))
    msg = q.buffer[q.head]
    q.buffer[q.head] = q.empty
    q.len -= 1
    if q.len == 0
        q.head = 1
        q.tail = 1
    else
        q.head = _queue_next_index(q, q.head)
    end
    msg
end

const _BorrowedDispatchData = SubArray{UInt8,1,Vector{UInt8},Tuple{UnitRange{Int}},true}
const _BorrowedDispatchMsg = BorrowedMsg{_BorrowedDispatchData}

struct _BorrowedCallback{C<:AbstractNatterClient}
    invoke::FunctionWrappers.FunctionWrapper{Nothing,Tuple{C,_BorrowedDispatchMsg}}
end

_borrowed_callback_noop(_client, _msg::_BorrowedDispatchMsg) = nothing

function _borrowed_callback_handler(client::C, ::Nothing) where {C<:AbstractNatterClient}
    invoke = FunctionWrappers.FunctionWrapper{Nothing,Tuple{C,_BorrowedDispatchMsg}}(_borrowed_callback_noop)
    _BorrowedCallback{C}(invoke)
end

function _borrowed_callback_handler(client::C, callback) where {C<:AbstractNatterClient}
    invoke = FunctionWrappers.FunctionWrapper{Nothing,Tuple{C,_BorrowedDispatchMsg}}((client, msg) -> begin
        _invoke_borrowed_callback(client, callback, msg)
    end)
    _BorrowedCallback{C}(invoke)
end

struct _DeadlineEntry{V}
    deadline::Float64
    token::Int
    value::V
end

mutable struct _DeadlineQueue{V}
    heap::Vector{_DeadlineEntry{V}}
end

_DeadlineQueue{V}() where {V} = _DeadlineQueue{V}(_DeadlineEntry{V}[])

Base.isempty(q::_DeadlineQueue)::Bool = isempty(q.heap)
Base.length(q::_DeadlineQueue)::Int = length(q.heap)
Base.empty!(q::_DeadlineQueue) = (empty!(q.heap); q)

_deadline_queue_compaction_due(q::_DeadlineQueue, live::Int)::Bool =
    length(q.heap) > max(64, live * 2)

@inline function _deadline_entry_less(a::_DeadlineEntry, b::_DeadlineEntry)::Bool
    a.deadline < b.deadline || (a.deadline == b.deadline && a.token < b.token)
end

function _deadline_queue_sift_up!(heap::Vector{_DeadlineEntry{V}}, idx::Int) where {V}
    while idx > 1
        parent = idx >>> 1
        _deadline_entry_less(heap[idx], heap[parent]) || break
        heap[idx], heap[parent] = heap[parent], heap[idx]
        idx = parent
    end
    nothing
end

function _deadline_queue_sift_down!(heap::Vector{_DeadlineEntry{V}}, idx::Int) where {V}
    len = length(heap)
    while true
        left = idx << 1
        left <= len || break
        right = left + 1
        child = right <= len && _deadline_entry_less(heap[right], heap[left]) ? right : left
        _deadline_entry_less(heap[child], heap[idx]) || break
        heap[idx], heap[child] = heap[child], heap[idx]
        idx = child
    end
    nothing
end

function _deadline_queue_push!(q::_DeadlineQueue{V}, token::Int, deadline::Float64,
                               value::V)::Bool where {V}
    earlier = isempty(q.heap) || deadline < q.heap[1].deadline
    push!(q.heap, _DeadlineEntry{V}(deadline, token, value))
    _deadline_queue_sift_up!(q.heap, length(q.heap))
    earlier
end

_deadline_queue_peek(q::_DeadlineQueue) = isempty(q.heap) ? nothing : q.heap[1]

function _deadline_queue_pop!(q::_DeadlineQueue)
    isempty(q.heap) && return nothing
    top = q.heap[1]
    tail = pop!(q.heap)
    if !isempty(q.heap)
        q.heap[1] = tail
        _deadline_queue_sift_down!(q.heap, 1)
    end
    top
end

mutable struct _ConditionTimeoutWaiter
    active::Bool
    timed_out::Bool
    deadline::Float64
end

_ConditionTimeoutWaiter(deadline::Float64) = _ConditionTimeoutWaiter(true, false, deadline)

mutable struct _ConditionTimeoutQueue
    deadlines::_DeadlineQueue{_ConditionTimeoutWaiter}
    next_token::Int
    active::Int
    task::Union{Task,Nothing}
end

_ConditionTimeoutQueue() =
    _ConditionTimeoutQueue(_DeadlineQueue{_ConditionTimeoutWaiter}(), 0, 0, nothing)

const _CONDITION_TIMEOUT_POLL_SECONDS = 0.001

function _next_condition_timeout_token!(queue::_ConditionTimeoutQueue)::Int
    token = queue.next_token == typemax(Int) ? 1 : queue.next_token + 1
    queue.next_token = token
    token
end

function _condition_timeout_entry_valid(entry::_DeadlineEntry{_ConditionTimeoutWaiter})::Bool
    waiter = entry.value
    waiter.active && waiter.deadline == entry.deadline
end

function _next_condition_timeout_deadline_locked!(queue::_ConditionTimeoutQueue)
    while true
        entry = _deadline_queue_peek(queue.deadlines)
        isnothing(entry) && return nothing
        _condition_timeout_entry_valid(entry) && return entry
        _deadline_queue_pop!(queue.deadlines)
    end
end

function _rebuild_condition_timeout_queue_locked!(queue::_ConditionTimeoutQueue)
    deadlines = _DeadlineQueue{_ConditionTimeoutWaiter}()
    active = 0
    for entry in queue.deadlines.heap
        if _condition_timeout_entry_valid(entry)
            active += 1
            _deadline_queue_push!(deadlines, entry.token, entry.deadline, entry.value)
        end
    end
    queue.deadlines = deadlines
    queue.active = active
    nothing
end

function _compact_condition_timeout_queue_locked!(queue::_ConditionTimeoutQueue)
    _deadline_queue_compaction_due(queue.deadlines, queue.active) ||
        return nothing
    _rebuild_condition_timeout_queue_locked!(queue)
end

function _condition_timeout_loop(condition::Base.GenericCondition{ReentrantLock},
                                 queue::_ConditionTimeoutQueue)
    while true
        sleep_for = _CONDITION_TIMEOUT_POLL_SECONDS
        lock(condition)
        try
            entry = _next_condition_timeout_deadline_locked!(queue)
            if isnothing(entry)
                queue.task = nothing
                return nothing
            end

            delay = entry.deadline - time()
            if delay <= 0
                _deadline_queue_pop!(queue.deadlines)
                waiter = entry.value
                if waiter.active && waiter.deadline == entry.deadline
                    waiter.active = false
                    waiter.timed_out = true
                    queue.active = max(0, queue.active - 1)
                    notify(condition; all=true)
                end
                continue
            end
            sleep_for = min(delay, _CONDITION_TIMEOUT_POLL_SECONDS)
        finally
            unlock(condition)
        end
        sleep(sleep_for)
    end
end

function _ensure_condition_timeout_task_locked!(condition::Base.GenericCondition{ReentrantLock},
                                                queue::_ConditionTimeoutQueue)
    task = queue.task
    if isnothing(task) || istaskdone(task)
        queue.task = _spawn_control(:condition_timeout) do
            _condition_timeout_loop(condition, queue)
        end
    end
    nothing
end

function _register_condition_timeout_locked!(condition::Base.GenericCondition{ReentrantLock},
                                             queue::_ConditionTimeoutQueue,
                                             seconds::Float64)::_ConditionTimeoutWaiter
    waiter = _ConditionTimeoutWaiter(time() + seconds)
    token = _next_condition_timeout_token!(queue)
    _deadline_queue_push!(queue.deadlines, token, waiter.deadline, waiter)
    queue.active += 1
    _ensure_condition_timeout_task_locked!(condition, queue)
    waiter
end

function _deregister_condition_timeout_locked!(queue::_ConditionTimeoutQueue,
                                               waiter::_ConditionTimeoutWaiter)
    if waiter.active
        waiter.active = false
        queue.active = max(0, queue.active - 1)
        _compact_condition_timeout_queue_locked!(queue)
    end
    nothing
end
