const _WRITER_BATCH_OPERATION = "queued publish batch"
const _WRITER_ENQUEUE_INACTIVE = 0
const _WRITER_ENQUEUE_RETRY = 1
const _WRITER_ENQUEUE_ACCEPTED = 2

function _activate_writer_queue!(q::_WriteQueue, generation::Int)
    lock(q.condition)
    try
        empty!(q)
        q.epoch += 1
        q.generation = generation
        q.closed[] = false
        q.active[] = true
        notify(q.condition; all=true)
    finally
        unlock(q.condition)
    end
    nothing
end

function _deactivate_writer_queue!(q::_WriteQueue; close::Bool=false, clear::Bool=true)
    lock(q.condition)
    try
        if clear
            fill!(q.items, nothing)
            fill!(q.sizes, 0)
            q.head = 1
            q.tail = 1
            q.len = 0
            q.bytes = 0
            q.in_flight = 0
            q.epoch += 1
            q.pending_count = 0
        end
        q.generation = 0
        q.active[] = false
        close && (q.closed[] = true)
        notify(q.condition; all=true)
    finally
        unlock(q.condition)
    end
    nothing
end

function _deactivate_writer_queue!(client::Client; close::Bool=false, clear::Bool=true)
    q = client.writer_queue
    isnothing(q) && return nothing
    _deactivate_writer_queue!(q; close, clear)
end

function _signal_writer(client::Client)
    q = client.writer_queue
    isnothing(q) && return nothing
    lock(q.condition)
    try
        notify(q.condition; all=true)
    finally
        unlock(q.condition)
    end
    nothing
end

function _queued_publish_item(frame::_AbstractPublishFrame, frame_size::Int)::_QueuedWriteItem
    frame isa PublishFrame && return frame
    _pending_publish_entry(frame, frame_size)
end

function _writer_queue_has_capacity(q::_WriteQueue, frame_size::Int)::Bool
    q.len < q.max_msgs && q.bytes + frame_size <= q.max_bytes
end

function _writer_connection_admit_or_throw_locked(client::Client)::Bool
    st = client.status
    if st == ConnectionStatus.CONNECTED
        client.write_reconnect_pending[] && throw(ConnectionReconnectingError())
        return true
    end
    st == ConnectionStatus.DRAINING && throw(ConnectionDrainingError())
    st == ConnectionStatus.DISCONNECTED &&
        throw(ConnectionClosedError("connection is disconnected"))
    st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
    throw(ConnectionReconnectingError())
end

function _writer_queue_wait_ready(client::Client, q::_WriteQueue, frame_size::Int)::Bool
    !q.active[] || q.closed[] || _writer_queue_has_capacity(q, frame_size) ||
        client.write_reconnect_pending[]
end

function _write_batch_limits(client::Client, q::_WriteQueue)::Tuple{Int,Int}
    max_msgs = min(client.options.write_batch_msgs, q.max_msgs)
    max_bytes = min(client.options.write_batch_bytes, q.max_bytes)
    max(max_msgs, 1), max(max_bytes, 1)
end

function _enqueue_writer_publish_locked(client::Client, q::_WriteQueue,
                                        item::_QueuedWriteItem, frame_size::Int)::Int
    _writer_connection_admit_or_throw_locked(client) || return _WRITER_ENQUEUE_INACTIVE
    lock(q.condition)
    try
        q.active[] || return _WRITER_ENQUEUE_INACTIVE
        q.closed[] && return _WRITER_ENQUEUE_INACTIVE
        q.generation == client.generation || return _WRITER_ENQUEUE_INACTIVE
        _writer_queue_has_capacity(q, frame_size) || return _WRITER_ENQUEUE_RETRY

        was_empty = q.len == 0
        idx = q.tail
        q.items[idx] = item
        q.sizes[idx] = frame_size
        q.tail = _write_queue_next_index(q, idx)
        q.len += 1
        q.bytes += frame_size
        q.pending_count += 1
        was_empty && notify(q.condition)
        _WRITER_ENQUEUE_ACCEPTED
    finally
        unlock(q.condition)
    end
end

function _enqueue_writer_publish(client::Client, frame::_AbstractPublishFrame, frame_size::Int;
                                 cancel_token::MaybeCancellationToken=nothing)::Bool
    client.options.write_driver || return false
    q = client.writer_queue
    isnothing(q) && return false
    q.active[] || return false
    q.closed[] && return false
    frame_size <= q.max_bytes || return false

    item = _queued_publish_item(frame, frame_size)
    while true
        result = @lock client.lock _enqueue_writer_publish_locked(client, q, item, frame_size)
        result == _WRITER_ENQUEUE_ACCEPTED && return true
        result == _WRITER_ENQUEUE_INACTIVE && return false

        lock(q.condition)
        try
            while q.active[] && !q.closed[] && !_writer_queue_has_capacity(q, frame_size) &&
                  !client.write_reconnect_pending[]
                _wait_until_notified_locked(q.condition; cancel_token) do
                    _writer_queue_wait_ready(client, q, frame_size)
                end
            end
        finally
            unlock(q.condition)
        end
    end
end

function _writer_batch_count(q::_WriteQueue, max_msgs::Int, max_bytes::Int)::Int
    count = 0
    bytes = 0
    idx = q.head
    limit = min(max(1, max_msgs), q.len)
    byte_limit = max(1, max_bytes)
    while count < limit
        size = q.sizes[idx]
        if count > 0 && bytes + size > byte_limit
            break
        end
        count += 1
        bytes += size
        idx = _write_queue_next_index(q, idx)
    end
    count
end

function _take_writer_batch_locked!(items::Vector{_QueuedWriteItem}, sizes::Vector{Int},
                                    q::_WriteQueue, count::Int)::Tuple{Int,Int}
    epoch = q.epoch
    sizehint!(items, count)
    sizehint!(sizes, count)
    for _ in 1:count
        idx = q.head
        item = q.items[idx]::_QueuedWriteItem
        size = q.sizes[idx]
        push!(items, item)
        push!(sizes, size)
        q.items[idx] = nothing
        q.sizes[idx] = 0
        q.head = _write_queue_next_index(q, idx)
        q.len -= 1
        q.bytes = max(0, q.bytes - size)
    end
    if q.len == 0
        q.head = 1
        q.tail = 1
    end
    q.in_flight += count
    notify(q.condition; all=true)
    count, epoch
end

function _take_writer_batch!(items::Vector{_QueuedWriteItem}, sizes::Vector{Int}, q::_WriteQueue,
                             max_msgs::Int, max_bytes::Int)::Tuple{Int,Int}
    empty!(items)
    empty!(sizes)
    lock(q.condition)
    try
        while q.active[] && !q.closed[] && q.len == 0
            wait(q.condition)
        end
        q.len == 0 && return 0, q.epoch
        count = _writer_batch_count(q, max_msgs, max_bytes)
        _take_writer_batch_locked!(items, sizes, q, count)
    finally
        unlock(q.condition)
    end
end

function _take_writer_batch_now!(items::Vector{_QueuedWriteItem}, sizes::Vector{Int}, q::_WriteQueue,
                                 max_msgs::Int, max_bytes::Int)::Tuple{Int,Int}
    empty!(items)
    empty!(sizes)
    lock(q.condition)
    try
        q.len == 0 && return 0, q.epoch
        count = _writer_batch_count(q, max_msgs, max_bytes)
        _take_writer_batch_locked!(items, sizes, q, count)
    finally
        unlock(q.condition)
    end
end

function _finish_writer_batch!(q::_WriteQueue, count::Int, epoch::Int)
    lock(q.condition)
    try
        if q.epoch == epoch
            q.in_flight = max(0, q.in_flight - count)
            q.pending_count = max(0, q.pending_count - count)
        end
        notify(q.condition; all=true)
    finally
        unlock(q.condition)
    end
    nothing
end

function _wait_writer_in_flight!(q::_WriteQueue; deadline=nothing,
                                 cancel_token::MaybeCancellationToken=nothing)::Bool
    lock(q.condition)
    try
        ready() = q.in_flight == 0 || !q.active[] || q.closed[]
        if isnothing(deadline)
            _wait_until_notified_locked(q.condition; cancel_token) do
                ready()
            end
        else
            ok = _wait_until_condition_locked(q.condition, _remaining_timeout(deadline);
                                             cancel_token) do
                ready()
            end
            ok || throw(TimeoutError("queued writes drain timed out"))
        end
        q.in_flight == 0
    finally
        unlock(q.condition)
    end
end

function _drain_writer_queue_now!(client::Client; deadline=nothing,
                                  cancel_token::MaybeCancellationToken=nothing)::Bool
    q = client.writer_queue
    isnothing(q) && return false
    q.active[] || return false
    items = _QueuedWriteItem[]
    sizes = Int[]
    max_msgs, max_bytes = _write_batch_limits(client, q)
    while true
        _wait_writer_in_flight!(q; deadline, cancel_token) || return false
        count, epoch = _take_writer_batch_now!(items, sizes, q, max_msgs, max_bytes)
        if count == 0
            return _wait_writer_in_flight!(q; deadline, cancel_token)
        end
        try
            _write_writer_batch(client, items, sizes, count)
        finally
            _finish_writer_batch!(q, count, epoch)
        end
    end
end

@inline function _writer_queue_ready(q::_WriteQueue)::Bool
    lock(q.condition)
    try
        q.pending_count == 0
    finally
        unlock(q.condition)
    end
end

@inline function _writer_queue_pending(client::Client)::Bool
    q = client.writer_queue
    isnothing(q) && return false
    q.active[] || return false
    lock(q.condition)
    try
        q.active[] && q.pending_count != 0
    finally
        unlock(q.condition)
    end
end

function _write_queued_item(client::Client, io, item::_PendingEntry, _frame_size::Int)
    _write_raw_to_io(client, io, item.data; force_flush=false)
    nothing
end

function _write_queued_item(client::Client, io, frame::_AbstractPublishFrame, frame_size::Int)
    _write_publish_to_active_io(client, io, frame, false, false, frame_size, false)
    nothing
end

function _write_queued_item_batch(client::Client, io::BufferedWriteIO, item::_PendingEntry,
                                  _frame_size::Int)
    _write_raw_data_to_io(client, io, item.data)
    nothing
end

function _write_queued_item_batch(client::Client, io::BufferedWriteIO,
                                  frame::_AbstractPublishFrame, frame_size::Int)
    threshold = max(0, client.options.write_buffer_size)
    if _should_write_publish_direct(frame_size, threshold)
        _buffered_bytes(io) > 0 && _flush_write_io(client, io)
        transport = _underlying_transport(io)
        _write_pub_frame_direct_timed(client, transport, frame; force_flush=true)
    else
        _buffer_publish_frame(client, io, frame, false, frame_size,
                              client.write_scratch,
                              client.options.direct_write_threshold)
    end
    nothing
end

function _write_writer_batch_to_io(client::Client, io, items::Vector{_QueuedWriteItem},
                                   sizes::Vector{Int}, count::Int)
    @inbounds for i in 1:count
        _write_queued_item(client, io, items[i], sizes[i])
    end
    _buffered_bytes(io) > 0 && _flush_write_io(client, io)
    nothing
end

function _write_writer_batch_to_io(client::Client, io::BufferedWriteIO,
                                   items::Vector{_QueuedWriteItem},
                                   sizes::Vector{Int}, count::Int)
    @inbounds for i in 1:count
        _write_queued_item_batch(client, io, items[i], sizes[i])
    end
    _buffered_bytes(io) > 0 && _flush_write_io(client, io)
    nothing
end

function _write_writer_batch(client::Client, items::Vector{_QueuedWriteItem}, sizes::Vector{Int}, count::Int)
    _with_write_lock(client, _WRITER_BATCH_OPERATION) do
        st = status(client)
        if !(st == ConnectionStatus.CONNECTED || st == ConnectionStatus.DRAINING)
            st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
            st == ConnectionStatus.DISCONNECTED &&
                throw(ConnectionClosedError("connection is disconnected"))
            throw(ConnectionReconnectingError())
        end
        client.write_reconnect_pending[] && throw(ConnectionReconnectingError())
        io = @atomic client.write_io
        isnothing(io) && throw(ConnectionClosedError("connection transport is closed"))
        _write_writer_batch_to_io(client, io, items, sizes, count)
    end
    nothing
end

function _writer_loop(client::Client, generation::Int, q::_WriteQueue)
    items = _QueuedWriteItem[]
    sizes = Int[]
    max_msgs, max_bytes = _write_batch_limits(client, q)
    while _generation_matches(client, generation) &&
          status(client) in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING)
        count, epoch = _take_writer_batch!(items, sizes, q, max_msgs, max_bytes)
        count == 0 && return nothing
        try
            _write_writer_batch(client, items, sizes, count)
        catch err
            cause = err isa _PublishWriteFailure ? err.cause : err
            status(client) == ConnectionStatus.CLOSED && return nothing
            cause isa CancelledError && return nothing
            _recover_after_write_failure!(client, cause) || _report_error(client, cause)
            return nothing
        finally
            _finish_writer_batch!(q, count, epoch)
        end
    end
    nothing
end

function _start_writer_task!(client::Client, generation::Int=(@lock client.lock client.generation))
    client.options.write_driver || return nothing
    q = client.writer_queue
    isnothing(q) && return nothing
    assigned = @lock client.lock begin
        existing = client.writer_task
        if client.generation == generation &&
           client.status in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING) &&
           (isnothing(existing) || istaskdone(existing))
            :start
        else
            :skip
        end
    end
    assigned == :start || return nothing
    _activate_writer_queue!(q, generation)
    writer_task = _spawn_control(:writer) do
        _writer_loop(client, generation, q)
    end
    deactivate = @lock client.lock begin
        if client.generation == generation &&
           client.status in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING)
            client.writer_task = writer_task
            false
        else
            true
        end
    end
    deactivate && _deactivate_writer_queue!(q)
    nothing
end

function _writer_barrier!(client::Client; deadline=nothing,
                          cancel_token::MaybeCancellationToken=nothing)::Bool
    q = client.writer_queue
    isnothing(q) && return false
    q.active[] || return false
    _writer_queue_ready(q) && return true
    _drain_writer_queue_now!(client; deadline, cancel_token) && return true
    lock(q.condition)
    try
        ready() = q.len == 0 && q.in_flight == 0
        if isnothing(deadline)
            _wait_until_notified_locked(q.condition; cancel_token) do
                ready() || !q.active[] || q.closed[]
            end
        else
            ok = _wait_until_condition_locked(q.condition, _remaining_timeout(deadline);
                                             cancel_token) do
                ready() || !q.active[] || q.closed[]
            end
            ok || throw(TimeoutError("queued writes drain timed out"))
        end
        ready()
    finally
        unlock(q.condition)
    end
end
