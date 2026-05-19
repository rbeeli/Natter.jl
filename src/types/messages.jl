struct MsgMetadata
    stream::String
    consumer::String
    delivered::Int
    stream_sequence::Int
    consumer_sequence::Int
    timestamp_ns::Int
    pending::Int
    domain::Union{String,Nothing}
end

struct Msg <: _ProtocolMsg
    subject::String
    reply::Union{String,Nothing}
    data::Vector{UInt8}
    headers::HeaderStorage
    sid::Int
    header_bytes::Int
end

struct BorrowedMsg{D<:AbstractVector{UInt8}} <: _ProtocolMsg
    subject::String
    reply::Union{String,Nothing}
    data::D
    headers::HeaderStorage
    sid::Int
    header_bytes::Int
end

const EMPTY_MSG = Msg("", nothing, EMPTY_BYTES, nothing, 0, 0)

function Msg(subject::String, reply::Union{String,Nothing}, data::Vector{UInt8};
             headers=nothing, sid=0, header_bytes::Union{Int,Nothing}=nothing)
    hdrs = isnothing(headers) ? nothing : _headers_copy(headers)
    Msg(subject, reply, data, hdrs, sid, _msg_header_bytes(hdrs, header_bytes))
end

function Msg(subject::String, reply::Union{String,Nothing}, data::Vector{UInt8},
             headers::HeaderStorage, sid::Int)
    Msg(subject, reply, data, headers, sid, _headers_wire_size(headers))
end

_msg_pending_bytes(msg::AbstractMsg)::Int = msg.header_bytes + length(msg.data)

_bytes_to_string(bytes::Vector{UInt8})::String =
    isempty(bytes) ? "" : unsafe_string(pointer(bytes), length(bytes))
_bytes_to_string(bytes::AbstractVector{UInt8})::String = String(copy(bytes))

Base.String(msg::Msg) = _bytes_to_string(msg.data)
Base.String(msg::BorrowedMsg) = _bytes_to_string(msg.data)
