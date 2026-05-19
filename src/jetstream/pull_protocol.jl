_pull_fetch_next_subject(js::JetStreamContext, stream::AbstractString, consumer::AbstractString)::String =
    string(js.prefix, ".CONSUMER.MSG.NEXT.", stream, ".", consumer)

mutable struct _PullStreamRequest
    remaining_messages::Int
    remaining_bytes::Int
end

struct _PullStreamReservation
    request::_PullStreamRequest
    batch::Int
    max_bytes::Union{Int,Nothing}
end

struct _PullStreamConfig
    batch::Int
    max_bytes::Union{Int,Nothing}
    expires::Float64
    heartbeat::Float64
    threshold_messages::Int
    threshold_bytes::Union{Int,Nothing}
    min_pending::Union{Int,Nothing}
    min_ack_pending::Union{Int,Nothing}
    priority_group::Union{String,Nothing}
    priority::Union{Int,Nothing}
    stop_after::Union{Int,Nothing}
    channel_size::Int
end

function _append_ascii!(buffer::Vector{UInt8}, value::AbstractString)
    append!(buffer, codeunits(value))
    buffer
end

function _append_json_int!(buffer::Vector{UInt8}, value::Int)
    value >= 0 || throw(ArgumentError("JSON integer must be non-negative"))
    digits = _decimal_digits(value)
    old = length(buffer)
    resize!(buffer, old + digits)
    pos = old + digits
    n = value
    @inbounds while pos > old
        buffer[pos] = UInt8('0') + UInt8(n % 10)
        n = div(n, 10)
        pos -= 1
    end
    buffer
end

function _append_json_hex_byte!(buffer::Vector{UInt8}, byte::UInt8)
    hi = byte >>> 4
    lo = byte & 0x0f
    push!(buffer, hi < 0x0a ? UInt8('0') + hi : UInt8('a') + (hi - 0x0a))
    push!(buffer, lo < 0x0a ? UInt8('0') + lo : UInt8('a') + (lo - 0x0a))
    buffer
end

function _append_json_string!(buffer::Vector{UInt8}, value::String)
    push!(buffer, UInt8('"'))
    for byte in codeunits(value)
        if byte == UInt8('"')
            _append_ascii!(buffer, "\\\"")
        elseif byte == UInt8('\\')
            _append_ascii!(buffer, "\\\\")
        elseif byte == UInt8('\b')
            _append_ascii!(buffer, "\\b")
        elseif byte == UInt8('\t')
            _append_ascii!(buffer, "\\t")
        elseif byte == UInt8('\n')
            _append_ascii!(buffer, "\\n")
        elseif byte == UInt8('\f')
            _append_ascii!(buffer, "\\f")
        elseif byte == UInt8('\r')
            _append_ascii!(buffer, "\\r")
        elseif byte < 0x20
            _append_ascii!(buffer, "\\u00")
            _append_json_hex_byte!(buffer, byte)
        else
            push!(buffer, byte)
        end
    end
    push!(buffer, UInt8('"'))
    buffer
end
