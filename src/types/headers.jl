struct _HeaderEntry
    name::String
    values::Vector{String}
end

mutable struct Headers <: AbstractDict{String,Vector{String}}
    data::Dict{String,_HeaderEntry}
    Headers(data::Dict{String,_HeaderEntry}) = new(data)
end

Headers() = Headers(Dict{String,_HeaderEntry}())

_ascii_lower(byte::UInt8) = UInt8('A') <= byte <= UInt8('Z') ? byte + 0x20 : byte

function _canonical_header_key(name::AbstractString)::String
    key = String(name)
    out = nothing
    @inbounds for i in 1:ncodeunits(key)
        byte = codeunit(key, i)
        lower = _ascii_lower(byte)
        if lower != byte
            if isnothing(out)
                out = Vector{UInt8}(undef, ncodeunits(key))
                for j in 1:(i - 1)
                    out[j] = codeunit(key, j)
                end
            end
            out[i] = lower
        elseif !isnothing(out)
            out[i] = byte
        end
    end
    isnothing(out) ? key : String(out)
end

function Base.iterate(headers::Headers, state...)
    next = iterate(values(headers.data), state...)
    isnothing(next) && return nothing
    entry, next_state = next
    entry.name => entry.values, next_state
end

Base.length(headers::Headers) = length(headers.data)
Base.isempty(headers::Headers) = isempty(headers.data)
Base.eltype(::Type{Headers}) = Pair{String,Vector{String}}

function Base.getindex(headers::Headers, key::AbstractString)
    entry = headers.data[_canonical_header_key(key)]
    entry.values
end

function Base.setindex!(headers::Headers, values::Vector{String}, key::AbstractString)
    canonical = _canonical_header_key(key)
    entry = get(headers.data, canonical, nothing)
    name = isnothing(entry) ? String(key) : entry.name
    headers.data[canonical] = _HeaderEntry(name, values)
    headers
end

function Base.get(headers::Headers, key::AbstractString, default)
    entry = get(headers.data, _canonical_header_key(key), nothing)
    isnothing(entry) ? default : entry.values
end
Base.get(::Headers, _key, default) = default

function Base.get!(headers::Headers, key::AbstractString, default::Vector{String})
    canonical = _canonical_header_key(key)
    entry = get(headers.data, canonical, nothing)
    if isnothing(entry)
        headers.data[canonical] = _HeaderEntry(String(key), default)
        return default
    end
    entry.values
end

Base.haskey(headers::Headers, key::AbstractString) =
    haskey(headers.data, _canonical_header_key(key))
Base.haskey(::Headers, _key) = false

function Base.delete!(headers::Headers, key::AbstractString)
    delete!(headers.data, _canonical_header_key(key))
    headers
end
Base.delete!(headers::Headers, _key) = headers
Base.empty!(headers::Headers) = (empty!(headers.data); headers)
Base.copy(headers::Headers) = _headers_from_pairs(name => copy(values) for (name, values) in headers)

Headers(pairs::Pair...) = _headers_from_pairs(pairs)

mutable struct LazyHeaders{R<:AbstractVector{UInt8}} <: AbstractDict{String,Vector{String}}
    raw::R
    parsed::Union{Headers,Nothing}
end

LazyHeaders(raw::R) where {R<:AbstractVector{UInt8}} = LazyHeaders{R}(raw, nothing)

struct RawHeaders{R<:AbstractVector{UInt8}} <: AbstractDict{String,Vector{String}}
    raw::R
    status::Int
    description_first::Int
    description_last::Int
end

RawHeaders(raw::R) where {R<:AbstractVector{UInt8}} = RawHeaders{R}(raw, 0, 1, 0)

const AnyLazyHeaders = LazyHeaders{<:AbstractVector{UInt8}}
const AnyRawHeaders = RawHeaders{<:AbstractVector{UInt8}}
const HeaderStorage = Union{Headers,AnyLazyHeaders,AnyRawHeaders,Nothing}
function _append_header_values!(values::Vector{String}, value)
    push!(values, String(value))
    values
end

function _append_header_values!(values::Vector{String}, values_input::Union{AbstractVector,Tuple})
    for value in values_input
        push!(values, String(value))
    end
    values
end

_append_header_values!(values::Vector{String}, value::AbstractVector{UInt8}) =
    (push!(values, String(value)); values)

_headers_from_pairs(::Nothing) = Headers()
_headers_from_pairs(pair::Pair) = _headers_from_pairs((pair,))
_headers_from_pairs(nt::NamedTuple) = _headers_from_pairs(pairs(nt))

function _headers_from_pairs(header_pairs)
    h = Headers()
    for pair in header_pairs
        values = get!(h, String(first(pair)), String[])
        _append_header_values!(values, last(pair))
    end
    h
end

_headers_from_input(::Nothing) = Headers()
_headers_from_input(h::Headers) = h
_headers_from_input(h) = _headers_from_pairs(h)

function _headers_materialize!(h::LazyHeaders)::Headers
    parsed = h.parsed
    if isnothing(parsed)
        parsed = _parse_headers(h.raw)
        h.parsed = parsed
    end
    parsed
end

_headers_materialize(h::RawHeaders)::Headers = _parse_headers(h.raw)

_headers_copy(::Nothing) = Headers()
_headers_copy(h::Headers) = _headers_from_pairs(k => copy(v) for (k, v) in h)
_headers_copy(h::LazyHeaders) = _headers_copy(_headers_materialize!(h))
_headers_copy(h::RawHeaders) = _headers_copy(_headers_materialize(h))
_headers_copy(h) = _headers_from_pairs(h)

Base.length(h::LazyHeaders) = length(_headers_materialize!(h))
Base.iterate(h::LazyHeaders, state...) = iterate(_headers_materialize!(h), state...)
Base.getindex(h::LazyHeaders, key::AbstractString) = getindex(_headers_materialize!(h), key)
Base.haskey(h::LazyHeaders, key) = haskey(_headers_materialize!(h), key)
Base.get(h::LazyHeaders, key, default) = get(_headers_materialize!(h), key, default)
Base.keys(h::LazyHeaders) = keys(_headers_materialize!(h))

Base.length(h::RawHeaders) = length(_headers_materialize(h))
Base.iterate(h::RawHeaders, state...) = iterate(_headers_materialize(h), state...)
Base.getindex(h::RawHeaders, key::AbstractString) = getindex(_headers_materialize(h), key)
Base.haskey(h::RawHeaders, key) = haskey(_headers_materialize(h), key)
Base.get(h::RawHeaders, key, default) = get(_headers_materialize(h), key, default)
Base.keys(h::RawHeaders) = keys(_headers_materialize(h))

_header_first(::Nothing, _key::AbstractString) = nothing
_header_first(headers::LazyHeaders, key::AbstractString) = _lazy_header_first(headers, key)
_header_first(headers::RawHeaders, key::AbstractString) = _raw_header_first(headers, key)
function _header_first(headers::Headers, key::AbstractString)
    values = get(headers, key, nothing)
    isnothing(values) || isempty(values) ? nothing : first(values)
end

function _delete_header!(headers::Headers, key::AbstractString)
    delete!(headers, key)
    headers
end
_delete_header!(::Nothing, _key::AbstractString) = nothing

function _headers_wire_size(headers::HeaderStorage)::Int
    (isnothing(headers) || isempty(headers)) && return 0
    bytes = ncodeunits("NATS/1.0") + 2 + 2
    for (name, values) in headers
        for value in values
            bytes += ncodeunits(name) + 2 + ncodeunits(value) + 2
        end
    end
    bytes
end

_headers_wire_size(headers::RawHeaders)::Int = length(headers.raw)

function _msg_header_bytes(headers::HeaderStorage, header_bytes::Union{Int,Nothing})::Int
    isnothing(header_bytes) && return _headers_wire_size(headers)
    header_bytes >= 0 || throw(ArgumentError("header_bytes must be non-negative"))
    header_bytes
end

headers(msg::AbstractMsg) = _headers_copy(msg.headers)
header(msg::AbstractMsg, key::AbstractString) = _header_first(msg.headers, key)
