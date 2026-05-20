EnumX.@enumx ConnectionStatus begin
    DISCONNECTED
    CONNECTING
    CONNECTED
    RECONNECTING
    DRAINING
    CLOSED
end

EnumX.@enumx PublishMode begin
    REPLAYABLE
    DIRECT
    QUEUED
end

Base.@kwdef mutable struct Stats
    in_msgs::Int = 0
    out_msgs::Int = 0
    in_bytes::Int = 0
    out_bytes::Int = 0
    reconnects::Int = 0
    errors::Int = 0
    dropped_msgs::Int = 0
end

Base.@kwdef struct SubscriptionStats
    pending_msgs::Int = 0
    pending_bytes::Int = 0
    processing::Int = 0
    received::Int = 0
    delivered::Int = 0
    dropped_msgs::Int = 0
    max_msgs::Int = 0
    closed::Bool = false
    server_active::Bool = false
end

struct AtomicCounter
    shards::Vector{Threads.Atomic{Int}}
end

function AtomicCounter(value::Int=0)
    shards = [Threads.Atomic{Int}(0) for _ in 1:max(1, Threads.maxthreadid())]
    shards[1][] = value
    AtomicCounter(shards)
end

struct AtomicStats
    in_msgs::AtomicCounter
    out_msgs::AtomicCounter
    in_bytes::AtomicCounter
    out_bytes::AtomicCounter
    reconnects::AtomicCounter
    errors::AtomicCounter
    dropped_msgs::AtomicCounter
end

AtomicStats(; in_msgs::Int=0, out_msgs::Int=0, in_bytes::Int=0, out_bytes::Int=0,
            reconnects::Int=0, errors::Int=0, dropped_msgs::Int=0) =
    AtomicStats(AtomicCounter(in_msgs), AtomicCounter(out_msgs),
                AtomicCounter(in_bytes), AtomicCounter(out_bytes),
                AtomicCounter(reconnects), AtomicCounter(errors),
                AtomicCounter(dropped_msgs))

AtomicStats(stats::Stats) = AtomicStats(; in_msgs=stats.in_msgs, out_msgs=stats.out_msgs,
                                        in_bytes=stats.in_bytes, out_bytes=stats.out_bytes,
                                        reconnects=stats.reconnects, errors=stats.errors,
                                        dropped_msgs=stats.dropped_msgs)

@inline function _stat_add!(counter::AtomicCounter, value::Int=1)
    shards = counter.shards
    @inbounds Threads.atomic_add!(shards[Threads.threadid()], value)
    nothing
end

function _stat_get(counter::AtomicCounter)::Int
    total = 0
    @inbounds for shard in counter.shards
        total += shard[]
    end
    total
end

function _default_error_cb(err)
    @warn "Natter client error" exception=err
    nothing
end

const _ErrorCallback = FunctionWrappers.FunctionWrapper{Nothing,Tuple{Any}}
const _EventCallback = FunctionWrappers.FunctionWrapper{Nothing,Tuple{Any}}
const _ReconnectDelayCallback = FunctionWrappers.FunctionWrapper{Any,Tuple{Any}}
const _AuthCallback = FunctionWrappers.FunctionWrapper{Any,Tuple{Any}}
const _SignatureCallback = FunctionWrappers.FunctionWrapper{Any,Tuple{Vector{UInt8}}}

function _wrap_error_callback(callback)::_ErrorCallback
    callback = isnothing(callback) ? _default_error_cb : callback
    _ErrorCallback(err -> begin
        callback(err)
        nothing
    end)
end

function _wrap_event_callback(callback)::_EventCallback
    callback = isnothing(callback) ? _default_noop_event_cb : callback
    _EventCallback(event -> begin
        callback(event)
        nothing
    end)
end

function _wrap_reconnect_delay_callback(callback)::_ReconnectDelayCallback
    callback = isnothing(callback) ? _default_reconnect_delay_cb : callback
    _ReconnectDelayCallback(event -> callback(event))
end

function _wrap_auth_callback(callback)::_AuthCallback
    isnothing(callback) && throw(ArgumentError("CallbackAuth requires a callback"))
    _AuthCallback(request -> callback(request))
end

function _wrap_signature_callback(callback)
    isnothing(callback) && return nothing
    _SignatureCallback(nonce -> callback(nonce))
end
abstract type AbstractNatterClient end
abstract type AbstractMsg end
abstract type _ProtocolMsg <: AbstractMsg end
abstract type _AbstractPublishFrame end

struct ImmutableBytes <: AbstractVector{UInt8}
    data::Vector{UInt8}
    function ImmutableBytes(data::Vector{UInt8}; copy::Bool=true)
        new(copy ? Base.copy(data) : data)
    end
end

ImmutableBytes(bytes::AbstractVector{UInt8}) =
    ImmutableBytes(Vector{UInt8}(bytes); copy=false)

Base.IndexStyle(::Type{ImmutableBytes}) = IndexLinear()
Base.size(bytes::ImmutableBytes) = size(bytes.data)
Base.length(bytes::ImmutableBytes) = length(bytes.data)
Base.getindex(bytes::ImmutableBytes, i::Int) = getindex(bytes.data, i)
Base.firstindex(bytes::ImmutableBytes) = firstindex(bytes.data)
Base.lastindex(bytes::ImmutableBytes) = lastindex(bytes.data)
Base.copy(bytes::ImmutableBytes) = copy(bytes.data)
Base.copyto!(dest::Vector{UInt8}, destpos::Int, bytes::ImmutableBytes, srcpos::Int, n::Int) =
    copyto!(dest, destpos, bytes.data, srcpos, n)
Base.write(io::IO, bytes::ImmutableBytes) = write(io, bytes.data)

struct _PendingEntry
    data::Vector{UInt8}
    bytes::Int
    payload_size::Int
    header_bytes::Int
    is_publish::Bool
end

_PendingEntry(data::Vector{UInt8}) = _PendingEntry(data, length(data), 0, 0, false)
_PendingPublishEntry(data::Vector{UInt8}, payload_size::Int, header_bytes::Int) =
    _PendingEntry(data, length(data), payload_size, header_bytes, true)

const EMPTY_PENDING_ENTRY = _PendingEntry(EMPTY_BYTES)

_pending_entry_size(entry::_PendingEntry)::Int = entry.bytes

function _pending_entries_size(entries::AbstractVector{<:_PendingEntry})::Int
    bytes = 0
    for entry in entries
        bytes += _pending_entry_size(entry)
    end
    bytes
end

function _copy_pending_entry_bytes!(out::Vector{UInt8}, pos::Int, entry::_PendingEntry)::Int
    copyto!(out, pos, entry.data, 1, entry.bytes)
    pos + entry.bytes
end

function _pending_entries_bytes(entries::AbstractVector{<:_PendingEntry})::Vector{UInt8}
    out = Vector{UInt8}(undef, _pending_entries_size(entries))
    pos = 1
    for entry in entries
        pos = _copy_pending_entry_bytes!(out, pos, entry)
    end
    pos == length(out) + 1 || throw(AssertionError("pending replay size mismatch"))
    out
end

_pending_entries_write_bytes(entries::Vector{_PendingEntry}) =
    length(entries) == 1 ? entries[1].data : _pending_entries_bytes(entries)

function _pending_entries_write_bytes!(scratch::Vector{UInt8},
                                       entries::Vector{_PendingEntry})::Vector{UInt8}
    resize!(scratch, _pending_entries_size(entries))
    pos = 1
    for entry in entries
        pos = _copy_pending_entry_bytes!(scratch, pos, entry)
    end
    pos == length(scratch) + 1 || throw(AssertionError("pending replay size mismatch"))
    scratch
end

struct _ReplayableEntry
    start::Int
    bytes::Int
    payload_size::Int
    header_bytes::Int
end

function _copy_replayable_entry(buffer::AbstractVector{UInt8}, entry::_ReplayableEntry)::_PendingEntry
    data = Vector{UInt8}(undef, entry.bytes)
    copyto!(data, 1, buffer, entry.start, entry.bytes)
    _PendingPublishEntry(data, entry.payload_size, entry.header_bytes)
end
