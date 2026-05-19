EnumX.@enumx ConnectionStatus begin
    DISCONNECTED
    CONNECTING
    CONNECTED
    RECONNECTING
    DRAINING
    CLOSED
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

function _connect_option_servers(servers)::Vector{String}
    servers isa Union{AbstractVector,Tuple} || throw(ArgumentError("servers must be a vector or tuple of strings"))
    result = String[]
    for raw in servers
        raw isa AbstractString || throw(ArgumentError("servers must be a vector or tuple of strings"))
        push!(result, strip(String(raw)))
    end
    isempty(result) && throw(ArgumentError("at least one server URL is required"))
    any(isempty, result) && throw(ArgumentError("server URL cannot be empty"))
    result
end

function _connect_option_float(name::String, value)::Float64
    value isa Real && !(value isa Bool) ||
        throw(ArgumentError("$name must be a finite number"))
    result = Float64(value)
    isfinite(result) || throw(ArgumentError("$name must be a finite number"))
    result
end

function _connect_option_positive_float(name::String, value)::Float64
    result = _connect_option_float(name, value)
    result > 0 || throw(ArgumentError("$name must be positive"))
    result
end

function _connect_option_positive_or_infinite_float(name::String, value)::Float64
    value isa Real && !(value isa Bool) ||
        throw(ArgumentError("$name must be a positive number or Inf"))
    result = Float64(value)
    (result > 0 || result == Inf) && !isnan(result) ||
        throw(ArgumentError("$name must be a positive number or Inf"))
    result
end

function _positive_timeout_seconds(name::AbstractString, value)::Float64
    value isa Real && !(value isa Bool) ||
        throw(ArgumentError("$name must be a positive finite number of seconds"))
    seconds = Float64(value)
    isfinite(seconds) && seconds > 0 ||
        throw(ArgumentError("$name must be a positive finite number of seconds"))
    seconds
end

function _connect_option_nonnegative_float(name::String, value)::Float64
    result = _connect_option_float(name, value)
    result >= 0 || throw(ArgumentError("$name must be non-negative"))
    result
end

function _connect_option_int(name::String, value)::Int
    value isa Integer && !(value isa Bool) ||
        throw(ArgumentError("$name must be an integer"))
    try
        return Int(value)
    catch
        throw(ArgumentError("$name must fit in Int"))
    end
end

function _integer_range_option(name::AbstractString, value, min::Integer, max::Integer,
                               requirement::AbstractString)::Int
    value isa Bool && throw(ArgumentError("$name must be $requirement"))
    value isa Integer || throw(ArgumentError("$name must be $requirement"))
    min <= value <= max || throw(ArgumentError("$name must be $requirement"))
    Int(value)
end

_positive_integer_option(name::AbstractString, value)::Int =
    _integer_range_option(name, value, 1, typemax(Int), "a positive integer")
_nonnegative_integer_option(name::AbstractString, value)::Int =
    _integer_range_option(name, value, 0, typemax(Int), "a non-negative integer")

function _connect_option_positive_int(name::String, value)::Int
    result = _connect_option_int(name, value)
    result > 0 || throw(ArgumentError("$name must be positive"))
    result
end

function _connect_option_nonnegative_int(name::String, value)::Int
    result = _connect_option_int(name, value)
    result >= 0 || throw(ArgumentError("$name must be non-negative"))
    result
end

function _connect_option_reconnect_attempts(value)::Int
    result = _connect_option_int("max_reconnect_attempts", value)
    (result == -1 || result >= 0) ||
        throw(ArgumentError("max_reconnect_attempts must be -1 or non-negative"))
    result
end

function _connect_option_bool(name::AbstractString, value)::Bool
    value isa Bool || throw(ArgumentError("$name must be a Bool"))
    value
end

function _connect_option_optional_bool(name::AbstractString, value)::Union{Bool,Nothing}
    isnothing(value) && return nothing
    _connect_option_bool(name, value)
end

function _connect_option_optional_string(name::AbstractString, value)::Union{String,Nothing}
    isnothing(value) && return nothing
    value isa AbstractString || throw(ArgumentError("$name must be a string"))
    result = String(value)
    isempty(result) && throw(ArgumentError("$name cannot be empty"))
    result
end

@inline _invalid_inbox_prefix_byte(byte::UInt8)::Bool =
    byte <= 0x20 || byte == 0x7f

function _validate_inbox_prefix(prefix::AbstractString)::String
    value = String(prefix)
    isempty(value) && throw(ArgumentError("inbox_prefix cannot be empty"))
    token_chars = 0
    previous_dot = false
    for byte in codeunits(value)
        _invalid_inbox_prefix_byte(byte) &&
            throw(ArgumentError("inbox_prefix cannot contain whitespace or control characters"))
        if byte == UInt8('.')
            if token_chars == 0
                msg = previous_dot ? "inbox_prefix cannot contain consecutive dots" :
                      "inbox_prefix cannot start with '.'"
                throw(ArgumentError(msg))
            end
            token_chars = 0
            previous_dot = true
        else
            (byte == UInt8('>') || byte == UInt8('*')) &&
                throw(ArgumentError("inbox_prefix cannot contain wildcards"))
            token_chars += 1
            previous_dot = false
        end
    end
    previous_dot && throw(ArgumentError("inbox_prefix cannot end with '.'"))
    value
end
_validate_inbox_prefix(_prefix) =
    throw(ArgumentError("inbox_prefix must be a string"))

_connect_option_present(value)::Bool = !isnothing(value)
_connect_option_count_present(values...)::Int = count(_connect_option_present, values)

function _connect_option_required_secret(name::AbstractString, value)::SecretBytes
    secret = _secret_bytes(value)
    isnothing(secret) && throw(ArgumentError("$name is required"))
    isempty(secret) && throw(ArgumentError("$name cannot be empty"))
    secret
end

function _validate_connect_option_tls(tls_cert_path, tls_key_path)
    if isnothing(tls_cert_path) != isnothing(tls_key_path)
        throw(ArgumentError("tls_cert_path and tls_key_path must be provided together"))
    end
    nothing
end

abstract type AbstractAuth end

struct NoAuth <: AbstractAuth end

struct TokenAuth <: AbstractAuth
    token::SecretBytes
    function TokenAuth(token)
        new(_connect_option_required_secret("token", token))
    end
end

struct UserPassAuth <: AbstractAuth
    user::String
    password::SecretBytes
    function UserPassAuth(user, password)
        normalized_user = _connect_option_optional_string("user", user)
        isnothing(normalized_user) && throw(ArgumentError("user is required"))
        new(normalized_user, _connect_option_required_secret("password", password))
    end
end

struct NKeyAuth <: AbstractAuth
    nkey::Union{String,Nothing}
    seed::Union{SecretBytes,Nothing}
    seed_path::Union{String,Nothing}
    signature_cb::Union{_SignatureCallback,Nothing}
    function NKeyAuth(nkey, seed, seed_path, signature_cb)
        normalized_nkey = _connect_option_optional_string("nkey", nkey)
        normalized_seed = _secret_bytes(seed)
        normalized_seed_path = _connect_option_optional_string("seed_path", seed_path)
        normalized_signature_cb = _wrap_signature_callback(signature_cb)
        seed_sources = _connect_option_count_present(normalized_seed, normalized_seed_path)
        seed_sources <= 1 ||
            throw(ArgumentError("NKeyAuth must use either seed or seed_path, not both"))
        signature_sources = seed_sources + (isnothing(normalized_signature_cb) ? 0 : 1)
        signature_sources == 1 ||
            throw(ArgumentError("NKeyAuth requires exactly one of seed, seed_path, or signature_cb"))
        if !isnothing(normalized_signature_cb) && isnothing(normalized_nkey)
            throw(ArgumentError("NKeyAuth with signature_cb requires nkey"))
        end
        new(normalized_nkey, normalized_seed, normalized_seed_path, normalized_signature_cb)
    end
end
function NKeyAuth(; nkey=nothing, seed=nothing, seed_path=nothing, signature_cb=nothing)
    NKeyAuth(nkey, seed, seed_path, signature_cb)
end

struct JwtAuth <: AbstractAuth
    jwt::Union{SecretBytes,Nothing}
    jwt_path::Union{String,Nothing}
    nkey::Union{String,Nothing}
    seed::Union{SecretBytes,Nothing}
    seed_path::Union{String,Nothing}
    signature_cb::Union{_SignatureCallback,Nothing}
    function JwtAuth(jwt, jwt_path, nkey, seed, seed_path, signature_cb)
        normalized_jwt = _secret_bytes(jwt)
        normalized_jwt_path = _connect_option_optional_string("jwt_path", jwt_path)
        jwt_sources = _connect_option_count_present(normalized_jwt, normalized_jwt_path)
        jwt_sources == 1 ||
            throw(ArgumentError("JwtAuth requires exactly one of jwt or jwt_path"))

        normalized_nkey = _connect_option_optional_string("nkey", nkey)
        normalized_seed = _secret_bytes(seed)
        normalized_seed_path = _connect_option_optional_string("seed_path", seed_path)
        normalized_signature_cb = _wrap_signature_callback(signature_cb)
        seed_sources = _connect_option_count_present(normalized_seed, normalized_seed_path)
        seed_sources <= 1 ||
            throw(ArgumentError("JwtAuth must use either seed or seed_path, not both"))
        signature_sources = seed_sources + (isnothing(normalized_signature_cb) ? 0 : 1)
        signature_sources == 1 ||
            throw(ArgumentError("JwtAuth requires exactly one of seed, seed_path, or signature_cb"))
        new(normalized_jwt, normalized_jwt_path, normalized_nkey,
            normalized_seed, normalized_seed_path, normalized_signature_cb)
    end
end
function JwtAuth(; jwt=nothing, jwt_path=nothing, nkey=nothing, seed=nothing,
                 seed_path=nothing, signature_cb=nothing)
    JwtAuth(jwt, jwt_path, nkey, seed, seed_path, signature_cb)
end

struct CredentialsAuth <: AbstractAuth
    credentials::Union{SecretBytes,Nothing}
    path::Union{String,Nothing}
    function CredentialsAuth(credentials, path)
        normalized_credentials = _secret_bytes(credentials)
        normalized_path = _connect_option_optional_string("path", path)
        sources = _connect_option_count_present(normalized_credentials, normalized_path)
        sources == 1 ||
            throw(ArgumentError("CredentialsAuth requires exactly one of credentials or path"))
        new(normalized_credentials, normalized_path)
    end
end
CredentialsAuth(; credentials=nothing, path=nothing) = CredentialsAuth(credentials, path)
CredentialsAuth(credentials) = CredentialsAuth(; credentials)

struct CallbackAuth <: AbstractAuth
    callback::_AuthCallback
    function CallbackAuth(callback)
        new(_wrap_auth_callback(callback))
    end
end

Base.show(io::IO, ::NoAuth) = print(io, "NoAuth()")
Base.show(io::IO, ::TokenAuth) = print(io, "TokenAuth(<redacted>)")
Base.show(io::IO, ::UserPassAuth) = print(io, "UserPassAuth(<redacted>)")
Base.show(io::IO, ::NKeyAuth) = print(io, "NKeyAuth(<redacted>)")
Base.show(io::IO, ::JwtAuth) = print(io, "JwtAuth(<redacted>)")
Base.show(io::IO, ::CredentialsAuth) = print(io, "CredentialsAuth(<redacted>)")
Base.show(io::IO, ::CallbackAuth) = print(io, "CallbackAuth(...)")

_default_noop_event_cb(_event) = nothing
_default_reconnect_delay_cb(_event) = nothing

struct ConnectOptions{Auth<:AbstractAuth}
    # Deliberately mutable: ConnectOptions is a configuration handle, and
    # connect(options) snapshots the current server list into Client.servers.
    servers::Vector{String}
    randomize_servers::Bool
    name::Union{String,Nothing}
    verbose::Bool
    pedantic::Bool
    auth::Auth
    no_echo::Bool
    tls_required::Bool
    tls_first::Union{Bool,Nothing}
    tls_verify::Bool
    tls_server_name::Union{String,Nothing}
    tls_ca_path::Union{String,Nothing}
    tls_cert_path::Union{String,Nothing}
    tls_key_path::Union{String,Nothing}
    connect_timeout::Float64
    ping_interval::Float64
    max_outstanding_pings::Int
    allow_reconnect::Bool
    retry_on_initial_connect::Bool
    reconnect_wait::Float64
    reconnect_max_wait::Float64
    reconnect_jitter::Float64
    max_reconnect_attempts::Int
    pending_size::Int
    read_buffer_size::Int
    read_buffer_shrink_threshold::Int
    write_buffer_size::Int
    direct_write_threshold::Int
    write_buffer_latency::Float64
    write_timeout::Float64
    record_stats::Bool
    max_control_line::Int
    max_inbound_payload::Int
    max_header_bytes::Int
    max_stale_pong_waiters::Int
    sub_pending_msgs_limit::Int
    sub_pending_bytes_limit::Int
    drain_timeout::Float64
    close_callback_timeout::Float64
    inbox_prefix::String
    error_cb::_ErrorCallback
    event_cb::_EventCallback
    reconnect_delay_cb::_ReconnectDelayCallback

    function ConnectOptions(
        servers, randomize_servers, name, verbose, pedantic, auth, no_echo, tls_required, tls_first,
        tls_verify, tls_server_name, tls_ca_path, tls_cert_path, tls_key_path, connect_timeout, ping_interval,
        max_outstanding_pings, allow_reconnect, retry_on_initial_connect,
        reconnect_wait, reconnect_max_wait, reconnect_jitter, max_reconnect_attempts,
        pending_size, read_buffer_size, read_buffer_shrink_threshold, write_buffer_size, direct_write_threshold,
        write_buffer_latency, write_timeout, record_stats,
        max_control_line, max_inbound_payload, max_header_bytes, max_stale_pong_waiters,
        sub_pending_msgs_limit, sub_pending_bytes_limit, drain_timeout, close_callback_timeout,
        inbox_prefix,
        error_cb, event_cb, reconnect_delay_cb,
    )
        servers = _connect_option_servers(servers)
        randomize_servers = _connect_option_bool("randomize_servers", randomize_servers)
        name = _connect_option_optional_string("name", name)
        verbose = _connect_option_bool("verbose", verbose)
        pedantic = _connect_option_bool("pedantic", pedantic)
        auth isa AbstractAuth || throw(ArgumentError("auth must be an AbstractAuth"))
        no_echo = _connect_option_bool("no_echo", no_echo)
        tls_required = _connect_option_bool("tls_required", tls_required)
        tls_first = _connect_option_optional_bool("tls_first", tls_first)
        tls_verify = _connect_option_bool("tls_verify", tls_verify)
        tls_server_name = _connect_option_optional_string("tls_server_name", tls_server_name)
        tls_ca_path = _connect_option_optional_string("tls_ca_path", tls_ca_path)
        tls_cert_path = _connect_option_optional_string("tls_cert_path", tls_cert_path)
        tls_key_path = _connect_option_optional_string("tls_key_path", tls_key_path)
        _validate_connect_option_tls(tls_cert_path, tls_key_path)
        connect_timeout = _connect_option_positive_float("connect_timeout", connect_timeout)
        ping_interval = _connect_option_positive_float("ping_interval", ping_interval)
        max_outstanding_pings = _connect_option_positive_int("max_outstanding_pings", max_outstanding_pings)
        allow_reconnect = _connect_option_bool("allow_reconnect", allow_reconnect)
        retry_on_initial_connect = _connect_option_bool("retry_on_initial_connect", retry_on_initial_connect)
        reconnect_wait = _connect_option_positive_float("reconnect_wait", reconnect_wait)
        reconnect_max_wait = _connect_option_positive_float("reconnect_max_wait", reconnect_max_wait)
        reconnect_max_wait >= reconnect_wait ||
            throw(ArgumentError("reconnect_max_wait must be greater than or equal to reconnect_wait"))
        reconnect_jitter = _connect_option_nonnegative_float("reconnect_jitter", reconnect_jitter)
        max_reconnect_attempts = _connect_option_reconnect_attempts(max_reconnect_attempts)
        pending_size = _connect_option_nonnegative_int("pending_size", pending_size)
        read_buffer_size = _connect_option_positive_int("read_buffer_size", read_buffer_size)
        read_buffer_shrink_threshold = _connect_option_positive_int("read_buffer_shrink_threshold",
                                                                    read_buffer_shrink_threshold)
        read_buffer_shrink_threshold >= read_buffer_size ||
            throw(ArgumentError("read_buffer_shrink_threshold must be greater than or equal to read_buffer_size"))
        write_buffer_size = _connect_option_nonnegative_int("write_buffer_size", write_buffer_size)
        direct_write_threshold = _connect_option_nonnegative_int("direct_write_threshold", direct_write_threshold)
        write_buffer_latency = _connect_option_nonnegative_float("write_buffer_latency", write_buffer_latency)
        write_timeout = _connect_option_positive_or_infinite_float("write_timeout", write_timeout)
        record_stats = _connect_option_bool("record_stats", record_stats)
        max_control_line = _connect_option_positive_int("max_control_line", max_control_line)
        max_inbound_payload = _connect_option_positive_int("max_inbound_payload", max_inbound_payload)
        max_header_bytes = _connect_option_positive_int("max_header_bytes", max_header_bytes)
        max_stale_pong_waiters = _connect_option_positive_int("max_stale_pong_waiters", max_stale_pong_waiters)
        sub_pending_msgs_limit = _connect_option_positive_int("sub_pending_msgs_limit", sub_pending_msgs_limit)
        sub_pending_bytes_limit = _connect_option_positive_int("sub_pending_bytes_limit", sub_pending_bytes_limit)
        drain_timeout = _connect_option_positive_float("drain_timeout", drain_timeout)
        close_callback_timeout = _connect_option_nonnegative_float("close_callback_timeout", close_callback_timeout)
        inbox_prefix = _validate_inbox_prefix(inbox_prefix)

        error_cb = _wrap_error_callback(error_cb)
        event_cb = _wrap_event_callback(event_cb)
        reconnect_delay_cb = _wrap_reconnect_delay_callback(reconnect_delay_cb)

        new{typeof(auth)}(
            servers, randomize_servers, name, verbose, pedantic, auth, no_echo, tls_required, tls_first,
            tls_verify, tls_server_name, tls_ca_path, tls_cert_path, tls_key_path, connect_timeout, ping_interval,
            max_outstanding_pings, allow_reconnect, retry_on_initial_connect,
            reconnect_wait, reconnect_max_wait, reconnect_jitter, max_reconnect_attempts,
            pending_size, read_buffer_size, read_buffer_shrink_threshold, write_buffer_size, direct_write_threshold,
            write_buffer_latency, write_timeout, record_stats,
            max_control_line, max_inbound_payload, max_header_bytes, max_stale_pong_waiters,
            sub_pending_msgs_limit, sub_pending_bytes_limit, drain_timeout, close_callback_timeout,
            inbox_prefix,
            error_cb, event_cb, reconnect_delay_cb)
    end
end

function _redacted_server_url(url::AbstractString)::String
    text = String(url)
    scheme = findfirst("://", text)
    start = isnothing(scheme) ? firstindex(text) : nextind(text, last(scheme))
    stop = lastindex(text)
    for delimiter in ('/', '?', '#')
        index = findnext(==(delimiter), text, start)
        if !isnothing(index)
            stop = min(stop, prevind(text, index))
        end
    end
    stop < start && return text
    at = nothing
    index = findnext(==('@'), text, start)
    while !isnothing(index) && index <= stop
        at = index
        index = findnext(==('@'), text, nextind(text, index))
    end
    isnothing(at) && return text

    prefix = start == firstindex(text) ? "" : text[firstindex(text):prevind(text, start)]
    suffix = at == lastindex(text) ? "" : text[nextind(text, at):lastindex(text)]
    "$prefix<redacted>@$suffix"
end

function _show_connect_option_value(io::IO, name::Symbol, value)
    if name === :servers
        show(io, map(_redacted_server_url, value))
    elseif name in (:error_cb, :event_cb, :reconnect_delay_cb)
        print(io, "<callback>")
    else
        show(io, value)
    end
    nothing
end

function Base.show(io::IO, opts::ConnectOptions)
    print(io, "ConnectOptions(")
    names = fieldnames(typeof(opts))
    for (i, name) in pairs(names)
        i == 1 || print(io, ", ")
        print(io, name, "=")
        _show_connect_option_value(io, name, getfield(opts, name))
    end
    print(io, ")")
end

function Base.show(io::IO, ::MIME"text/plain", opts::ConnectOptions)
    show(io, opts)
end

function ConnectOptions(; servers=(DEFAULT_URL,), randomize_servers=true, name=nothing,
                        verbose=false, pedantic=false, auth=NoAuth(), no_echo=false, tls_required=false,
                        tls_first=nothing, tls_verify=true,
                        tls_server_name=nothing, tls_ca_path=nothing,
                        tls_cert_path=nothing, tls_key_path=nothing,
                        connect_timeout=2.0, ping_interval=120.0, max_outstanding_pings=2,
                        allow_reconnect=true, retry_on_initial_connect=false,
                        reconnect_wait=0.5, reconnect_max_wait=5.0, reconnect_jitter=0.1,
                        max_reconnect_attempts=-1,
                        pending_size=2 * 1024 * 1024,
                        read_buffer_size=DEFAULT_READ_BUFFER_SIZE,
                        read_buffer_shrink_threshold=4 * read_buffer_size,
                        write_buffer_size=DEFAULT_WRITE_BUFFER_SIZE,
                        direct_write_threshold=DEFAULT_DIRECT_WRITE_THRESHOLD,
                        write_buffer_latency=0.001, write_timeout=DEFAULT_WRITE_TIMEOUT,
                        record_stats=false,
                        max_control_line=DEFAULT_MAX_CONTROL_LINE,
                        max_inbound_payload=DEFAULT_MAX_INBOUND_PAYLOAD,
                        max_header_bytes=DEFAULT_MAX_HEADER_BYTES,
                        max_stale_pong_waiters=1024, sub_pending_msgs_limit=1024,
                        sub_pending_bytes_limit=128 * 1024 * 1024, drain_timeout=30.0,
                        close_callback_timeout=5.0, inbox_prefix=DEFAULT_INBOX_PREFIX,
                        error_cb=_default_error_cb, event_cb=_default_noop_event_cb,
                        reconnect_delay_cb=_default_reconnect_delay_cb)
    ConnectOptions(servers, randomize_servers, name, verbose, pedantic, auth, no_echo, tls_required,
                   tls_first, tls_verify, tls_server_name, tls_ca_path,
                   tls_cert_path, tls_key_path, connect_timeout,
                   ping_interval, max_outstanding_pings, allow_reconnect,
                   retry_on_initial_connect, reconnect_wait, reconnect_max_wait,
                   reconnect_jitter, max_reconnect_attempts, pending_size,
                   read_buffer_size, read_buffer_shrink_threshold, write_buffer_size,
                   direct_write_threshold, write_buffer_latency, write_timeout,
                   record_stats, max_control_line, max_inbound_payload,
                   max_header_bytes, max_stale_pong_waiters, sub_pending_msgs_limit,
                   sub_pending_bytes_limit, drain_timeout, close_callback_timeout,
                   inbox_prefix, error_cb, event_cb, reconnect_delay_cb)
end

mutable struct Server
    url::String
    reconnects::Int
    last_attempt::Float64
    discovered::Bool
    tls_name::Union{String,Nothing}
    last_auth_error::Union{AuthenticationError,Nothing}
end
function Server(url::String; discovered=false, tls_name=nothing)
    normalized_tls_name = isnothing(tls_name) ? nothing : String(tls_name)
    Server(url, 0, 0.0, discovered, normalized_tls_name, nothing)
end

EnumX.@enumx ConnectionEventKind begin
    CONNECTED
    DISCONNECTED
    RECONNECT_ATTEMPT
    RECONNECT_DELAY
    RECONNECTED
    DISCOVERED_SERVERS
    TERMINAL_DISCONNECT
    CLOSED
end

struct ConnectionEvent
    kind::ConnectionEventKind.T
    status::ConnectionStatus.T
    server::Union{Server,Nothing}
    url::Union{String,Nothing}
    attempt::Int
    delay::Union{Float64,Nothing}
    error::Union{Exception,Nothing}
    generation::Int
end

Base.@kwdef mutable struct ServerInfo
    max_payload::Union{Int,Nothing} = nothing
    tls_required::Union{Bool,Nothing} = nothing
    tls_available::Union{Bool,Nothing} = nothing
    connect_urls::Union{Vector{String},Nothing} = nothing
    version::Union{String,Nothing} = nothing
    proto::Union{Int,Nothing} = nothing
    headers::Union{Bool,Nothing} = nothing
    nonce::Union{String,Nothing} = nothing
    ldm::Bool = false
end

struct AuthRequest
    server::Server
    url::String
    nonce::Union{String,Nothing}
    info::ServerInfo
    attempt::Int
    reconnect::Bool
end

function _merge_server_info!(dest::ServerInfo, src::ServerInfo)
    isnothing(src.max_payload) || (dest.max_payload = src.max_payload)
    isnothing(src.tls_required) || (dest.tls_required = src.tls_required)
    isnothing(src.tls_available) || (dest.tls_available = src.tls_available)
    isnothing(src.connect_urls) || (dest.connect_urls = copy(src.connect_urls))
    isnothing(src.version) || (dest.version = src.version)
    isnothing(src.proto) || (dest.proto = src.proto)
    isnothing(src.headers) || (dest.headers = src.headers)
    isnothing(src.nonce) || (dest.nonce = src.nonce)
    dest.ldm = src.ldm
    dest
end

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
    Client{Options,ReadIO,WriteIO}(options, servers, current_server, connected_url, status,
                                   Threads.Atomic{Int}(Int(status)),
                                   info, Threads.Atomic{Int}(something(info.max_payload, typemax(Int))),
                                   Threads.Atomic{Bool}(info.headers === true),
                                   socket, read_io, reader, write_io, lock, write_lock,
                                   write_condition, Threads.Atomic{Int}(0),
                                   Threads.Atomic{Bool}(false),
                                   UInt8[], Threads.Atomic{Float64}(Inf), Threads.Atomic{Int}(0),
                                   Threads.Atomic{Int}(0), Ref{Any}(nothing), Ref(""), nothing,
                                   flush_signal, flusher_task,
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
