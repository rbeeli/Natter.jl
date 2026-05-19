const _WRITER_BATCH_OPERATION = "queued publish batch"

function _activate_writer_queue!(q::_WriteQueue)
    lock(q.condition)
    try
        empty!(q)
        q.epoch += 1
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
            q.pending_count[] = 0
        end
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

function _queued_publish_item(frame::_AbstractPublishFrame, frame_size::Int)
    frame isa PublishFrame && return frame
    _pending_publish_entry(frame, frame_size)
end

function _writer_queue_has_capacity(q::_WriteQueue, frame_size::Int)::Bool
    q.len < q.max_msgs && q.bytes + frame_size <= q.max_bytes
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
    lock(q.condition)
    try
        while q.active[] && !q.closed[] && !_writer_queue_has_capacity(q, frame_size)
            _wait_until_notified_locked(q.condition; cancel_token) do
                !q.active[] || q.closed[] || _writer_queue_has_capacity(q, frame_size)
            end
        end
        q.active[] && !q.closed[] || return false
        idx = q.tail
        q.items[idx] = item
        q.sizes[idx] = frame_size
        q.tail = _write_queue_next_index(q, idx)
        q.len += 1
        q.bytes += frame_size
        Threads.atomic_add!(q.pending_count, 1)
        notify(q.condition)
        return true
    finally
        unlock(q.condition)
    end
end

function _take_writer_batch!(items::Vector{Any}, sizes::Vector{Int}, q::_WriteQueue,
                             max_msgs::Int)::Tuple{Int,Int}
    empty!(items)
    empty!(sizes)
    lock(q.condition)
    try
        while q.active[] && !q.closed[] && q.len == 0
            wait(q.condition)
        end
        q.len == 0 && return 0, q.epoch
        count = min(max(1, max_msgs), q.len)
        epoch = q.epoch
        sizehint!(items, count)
        sizehint!(sizes, count)
        for _ in 1:count
            idx = q.head
            item = q.items[idx]
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
    finally
        unlock(q.condition)
    end
end

function _take_writer_batch_now!(items::Vector{Any}, sizes::Vector{Int}, q::_WriteQueue,
                                 max_msgs::Int)::Tuple{Int,Int}
    empty!(items)
    empty!(sizes)
    lock(q.condition)
    try
        q.len == 0 && return 0, q.epoch
        count = min(max(1, max_msgs), q.len)
        epoch = q.epoch
        sizehint!(items, count)
        sizehint!(sizes, count)
        for _ in 1:count
            idx = q.head
            item = q.items[idx]
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
    finally
        unlock(q.condition)
    end
end

function _finish_writer_batch!(q::_WriteQueue, count::Int, epoch::Int)
    lock(q.condition)
    try
        if q.epoch == epoch
            q.in_flight = max(0, q.in_flight - count)
            Threads.atomic_sub!(q.pending_count, count)
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
    items = Any[]
    sizes = Int[]
    while true
        _wait_writer_in_flight!(q; deadline, cancel_token) || return false
        count, epoch = _take_writer_batch_now!(items, sizes, q, client.options.write_batch_msgs)
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
    q.pending_count[] == 0
end

@inline function _writer_queue_pending(client::Client)::Bool
    q = client.writer_queue
    isnothing(q) && return false
    q.active[] && q.pending_count[] != 0
end

function _write_queued_item(client::Client, io, item::_PendingEntry, _frame_size::Int)
    _write_raw_to_io(client, io, item.data; force_flush=false)
    nothing
end

function _write_queued_item(client::Client, io, frame::_AbstractPublishFrame, frame_size::Int)
    _write_publish_to_active_io(client, io, frame, false, false, frame_size, false)
    nothing
end

function _write_writer_batch(client::Client, items::Vector{Any}, sizes::Vector{Int}, count::Int)
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
        @inbounds for i in 1:count
            _write_queued_item(client, io, items[i], sizes[i])
        end
        _buffered_bytes(io) > 0 && _flush_write_io(client, io)
    end
    nothing
end

function _writer_loop(client::Client, generation::Int, q::_WriteQueue)
    items = Any[]
    sizes = Int[]
    while _generation_matches(client, generation) &&
          status(client) in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING)
        count, epoch = _take_writer_batch!(items, sizes, q, client.options.write_batch_msgs)
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
    _activate_writer_queue!(q)
    writer_task = _spawn_control(:writer) do
        _writer_loop(client, generation, q)
    end
    @lock client.lock begin
        if client.generation == generation &&
           client.status in (ConnectionStatus.CONNECTED, ConnectionStatus.DRAINING)
            client.writer_task = writer_task
        else
            _deactivate_writer_queue!(q)
        end
    end
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
