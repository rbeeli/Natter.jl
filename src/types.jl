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

struct AtomicCounter
    shards::Vector{Threads.Atomic{Int}}
end

function AtomicCounter(value::Int=0)
    shards = [Threads.Atomic{Int}(0) for _ in 1:max(1, Threads.nthreads())]
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
    @inbounds Threads.atomic_add!(shards[min(Threads.threadid(), length(shards))], value)
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

_default_noop_cb() = nothing

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

mutable struct LazyHeaders <: AbstractDict{String,Vector{String}}
    raw::Vector{UInt8}
    parsed::Union{Headers,Nothing}
end

LazyHeaders(raw::Vector{UInt8}) = LazyHeaders(raw, nothing)

const HeaderStorage = Union{Headers,LazyHeaders,Nothing}

abstract type AbstractNatterClient end
abstract type AbstractMsg end

function _append_header_value!(values::Vector{String}, value)
    push!(values, String(value))
    values
end

function _append_header_values!(values::Vector{String}, value)
    _append_header_value!(values, value)
end

function _append_header_values!(values::Vector{String}, values_input::Union{AbstractVector,Tuple})
    for value in values_input
        _append_header_value!(values, value)
    end
    values
end

function _append_header_values!(values::Vector{String}, value::AbstractVector{UInt8})
    _append_header_value!(values, value)
end

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

_headers_copy(::Nothing) = Headers()
_headers_copy(h::Headers) = _headers_from_pairs(k => copy(v) for (k, v) in h)
_headers_copy(h::LazyHeaders) = _headers_copy(_headers_materialize!(h))
_headers_copy(h) = _headers_from_pairs(h)

Base.length(h::LazyHeaders) = length(_headers_materialize!(h))
Base.iterate(h::LazyHeaders, state...) = iterate(_headers_materialize!(h), state...)
Base.getindex(h::LazyHeaders, key::AbstractString) = getindex(_headers_materialize!(h), key)
Base.haskey(h::LazyHeaders, key) = haskey(_headers_materialize!(h), key)
Base.get(h::LazyHeaders, key, default) = get(_headers_materialize!(h), key, default)
Base.keys(h::LazyHeaders) = keys(_headers_materialize!(h))

_header_values(::Nothing, _key::AbstractString) = nothing
_header_values(headers::LazyHeaders, key::AbstractString) =
    _header_values(_headers_materialize!(headers), key)

function _header_values(headers::Headers, key::AbstractString)
    get(headers, key, nothing)
end

_header_first(::Nothing, _key::AbstractString) = nothing
_header_first(headers::LazyHeaders, key::AbstractString) = _lazy_header_first(headers, key)
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

struct Msg <: AbstractMsg
    subject::String
    reply::Union{String,Nothing}
    data::Vector{UInt8}
    headers::HeaderStorage
    sid::Int
    header_bytes::Int
end

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

function _connect_option_servers(servers)
    servers isa Union{AbstractVector,Tuple} || throw(ArgumentError("servers must be a vector or tuple of strings"))
    result = Tuple(String.(servers))
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

_connect_option_present(value)::Bool = !isnothing(value)
_connect_option_count_present(values...)::Int = count(_connect_option_present, values)

function _validate_connect_option_security(token, user, password, nkey, nkey_seed,
                                           nkey_seed_path, jwt, jwt_path, credentials,
                                           credentials_path, signature_cb,
                                           tls_cert_path, tls_key_path)
    if isnothing(tls_cert_path) != isnothing(tls_key_path)
        throw(ArgumentError("tls_cert_path and tls_key_path must be provided together"))
    end
    if isnothing(user) != isnothing(password)
        throw(ArgumentError("user and password must be provided together"))
    end
    jwt_source_count = _connect_option_count_present(jwt, jwt_path, credentials, credentials_path)
    jwt_source_count <= 1 ||
        throw(ArgumentError("JWT authentication must use only one of jwt, jwt_path, credentials, or credentials_path"))
    seed_source_count = _connect_option_count_present(nkey_seed, nkey_seed_path)
    seed_source_count <= 1 ||
        throw(ArgumentError("nkey seed authentication must use either nkey_seed or nkey_seed_path, not both"))

    has_userpass = !isnothing(user) || !isnothing(password)
    has_jwt = jwt_source_count > 0
    has_credentials = !isnothing(credentials) || !isnothing(credentials_path)
    has_nkey = !isnothing(nkey) || (!has_jwt && seed_source_count > 0)
    has_signature_cb = !isnothing(signature_cb)
    has_nkey_jwt = has_jwt || has_nkey || has_signature_cb

    if !isnothing(token) && has_userpass
        throw(ArgumentError("token authentication cannot be combined with user/password authentication"))
    end
    if !isnothing(token) && has_nkey_jwt
        throw(ArgumentError("token authentication cannot be combined with nkey or JWT authentication"))
    end
    if has_userpass && has_nkey_jwt
        throw(ArgumentError("user/password authentication cannot be combined with nkey or JWT authentication"))
    end
    if has_jwt && !isnothing(nkey)
        throw(ArgumentError("JWT authentication cannot be combined with nkey authentication"))
    end
    if !has_jwt && isnothing(nkey) && has_signature_cb
        throw(ArgumentError("signature_cb requires nkey or JWT authentication"))
    end

    signature_source_count = seed_source_count + (has_signature_cb ? 1 : 0)
    if has_jwt
        jwt_signature_sources = signature_source_count + (has_credentials ? 1 : 0)
        jwt_signature_sources > 0 ||
            throw(ArgumentError("JWT authentication requires a signature source"))
        jwt_signature_sources == 1 ||
            throw(ArgumentError("JWT authentication must use only one signature source"))
    elseif !isnothing(nkey)
        signature_source_count > 0 ||
            throw(ArgumentError("nkey authentication requires nkey_seed, nkey_seed_path, or signature_cb"))
        signature_source_count == 1 ||
            throw(ArgumentError("nkey authentication must use only one signature source"))
    end
    nothing
end

struct ConnectOptions{Servers<:Tuple{Vararg{String}},SignatureCallback,ErrorCallback,DisconnectedCallback,ReconnectedCallback,ClosedCallback,DiscoveredServerCallback}
    servers::Servers
    name::Union{String,Nothing}
    verbose::Bool
    pedantic::Bool
    token::Union{String,Nothing}
    user::Union{String,Nothing}
    password::Union{String,Nothing}
    nkey::Union{String,Nothing}
    nkey_seed::Union{SecretBytes,Nothing}
    nkey_seed_path::Union{String,Nothing}
    jwt::Union{SecretBytes,Nothing}
    jwt_path::Union{String,Nothing}
    credentials::Union{SecretBytes,Nothing}
    credentials_path::Union{String,Nothing}
    signature_cb::SignatureCallback
    no_echo::Bool
    tls_required::Bool
    tls_first::Union{Bool,Nothing}
    tls_verify::Bool
    tls_ca_path::Union{String,Nothing}
    tls_cert_path::Union{String,Nothing}
    tls_key_path::Union{String,Nothing}
    connect_timeout::Float64
    ping_interval::Float64
    max_outstanding_pings::Int
    allow_reconnect::Bool
    reconnect_wait::Float64
    reconnect_max_wait::Float64
    reconnect_jitter::Float64
    max_reconnect_attempts::Int
    pending_size::Int
    write_buffer_size::Int
    write_buffer_latency::Float64
    write_timeout::Float64
    max_control_line::Int
    max_inbound_payload::Int
    max_header_bytes::Int
    max_stale_pong_waiters::Int
    sub_pending_msgs_limit::Int
    sub_pending_bytes_limit::Int
    drain_timeout::Float64
    close_callback_timeout::Float64
    inbox_prefix::String
    error_cb::ErrorCallback
    disconnected_cb::DisconnectedCallback
    reconnected_cb::ReconnectedCallback
    closed_cb::ClosedCallback
    discovered_server_cb::DiscoveredServerCallback

    function ConnectOptions(
        servers, name, verbose, pedantic, token, user, password, nkey, nkey_seed,
        nkey_seed_path, jwt, jwt_path, credentials, credentials_path, signature_cb,
        no_echo, tls_required, tls_first,
        tls_verify, tls_ca_path, tls_cert_path, tls_key_path, connect_timeout, ping_interval,
        max_outstanding_pings, allow_reconnect, reconnect_wait, reconnect_max_wait, reconnect_jitter,
        max_reconnect_attempts, pending_size, write_buffer_size, write_buffer_latency, write_timeout,
        max_control_line, max_inbound_payload, max_header_bytes, max_stale_pong_waiters,
        sub_pending_msgs_limit, sub_pending_bytes_limit, drain_timeout, close_callback_timeout,
        inbox_prefix,
        error_cb, disconnected_cb, reconnected_cb, closed_cb,
        discovered_server_cb,
    )
        servers = _connect_option_servers(servers)
        nkey_seed = _secret_bytes(nkey_seed)
        jwt = _secret_bytes(jwt)
        credentials = _secret_bytes(credentials)
        _validate_connect_option_security(token, user, password, nkey, nkey_seed, nkey_seed_path,
                                          jwt, jwt_path, credentials, credentials_path,
                                          signature_cb, tls_cert_path, tls_key_path)
        connect_timeout = _connect_option_positive_float("connect_timeout", connect_timeout)
        ping_interval = _connect_option_positive_float("ping_interval", ping_interval)
        max_outstanding_pings = _connect_option_positive_int("max_outstanding_pings", max_outstanding_pings)
        reconnect_wait = _connect_option_positive_float("reconnect_wait", reconnect_wait)
        reconnect_max_wait = _connect_option_positive_float("reconnect_max_wait", reconnect_max_wait)
        reconnect_max_wait >= reconnect_wait ||
            throw(ArgumentError("reconnect_max_wait must be greater than or equal to reconnect_wait"))
        reconnect_jitter = _connect_option_nonnegative_float("reconnect_jitter", reconnect_jitter)
        max_reconnect_attempts = _connect_option_reconnect_attempts(max_reconnect_attempts)
        pending_size = _connect_option_positive_int("pending_size", pending_size)
        write_buffer_size = _connect_option_nonnegative_int("write_buffer_size", write_buffer_size)
        write_buffer_latency = _connect_option_nonnegative_float("write_buffer_latency", write_buffer_latency)
        write_timeout = _connect_option_positive_float("write_timeout", write_timeout)
        max_control_line = _connect_option_positive_int("max_control_line", max_control_line)
        max_inbound_payload = _connect_option_positive_int("max_inbound_payload", max_inbound_payload)
        max_header_bytes = _connect_option_positive_int("max_header_bytes", max_header_bytes)
        max_stale_pong_waiters = _connect_option_positive_int("max_stale_pong_waiters", max_stale_pong_waiters)
        sub_pending_msgs_limit = _connect_option_positive_int("sub_pending_msgs_limit", sub_pending_msgs_limit)
        sub_pending_bytes_limit = _connect_option_positive_int("sub_pending_bytes_limit", sub_pending_bytes_limit)
        drain_timeout = _connect_option_positive_float("drain_timeout", drain_timeout)
        close_callback_timeout = _connect_option_nonnegative_float("close_callback_timeout", close_callback_timeout)

        new{typeof(servers),typeof(signature_cb),typeof(error_cb),typeof(disconnected_cb),typeof(reconnected_cb),typeof(closed_cb),
            typeof(discovered_server_cb)}(
            servers, name, verbose, pedantic, token, user, password, nkey, nkey_seed,
            nkey_seed_path, jwt, jwt_path, credentials, credentials_path, signature_cb,
            no_echo, tls_required, tls_first,
            tls_verify, tls_ca_path, tls_cert_path, tls_key_path, connect_timeout, ping_interval,
            max_outstanding_pings, allow_reconnect, reconnect_wait, reconnect_max_wait, reconnect_jitter,
            max_reconnect_attempts, pending_size, write_buffer_size, write_buffer_latency, write_timeout,
            max_control_line, max_inbound_payload, max_header_bytes, max_stale_pong_waiters,
            sub_pending_msgs_limit, sub_pending_bytes_limit, drain_timeout, close_callback_timeout,
            inbox_prefix,
            error_cb, disconnected_cb, reconnected_cb, closed_cb,
            discovered_server_cb)
    end
end

function ConnectOptions(; servers=(DEFAULT_URL,), name=nothing, verbose=false, pedantic=false,
                        token=nothing, user=nothing, password=nothing, nkey=nothing,
                        nkey_seed=nothing, nkey_seed_path=nothing, jwt=nothing,
                        jwt_path=nothing, credentials=nothing, credentials_path=nothing,
                        signature_cb=nothing, no_echo=false, tls_required=false,
                        tls_first=nothing, tls_verify=true,
                        tls_ca_path=nothing, tls_cert_path=nothing, tls_key_path=nothing,
                        connect_timeout=2.0, ping_interval=120.0, max_outstanding_pings=2,
                        allow_reconnect=true, reconnect_wait=0.5, reconnect_max_wait=5.0,
                        reconnect_jitter=0.1, max_reconnect_attempts=-1,
                        pending_size=2 * 1024 * 1024, write_buffer_size=DEFAULT_WRITE_BUFFER_SIZE,
                        write_buffer_latency=0.001, write_timeout=DEFAULT_WRITE_TIMEOUT,
                        max_control_line=DEFAULT_MAX_CONTROL_LINE,
                        max_inbound_payload=DEFAULT_MAX_INBOUND_PAYLOAD,
                        max_header_bytes=DEFAULT_MAX_HEADER_BYTES,
                        max_stale_pong_waiters=1024, sub_pending_msgs_limit=1024,
                        sub_pending_bytes_limit=128 * 1024 * 1024, drain_timeout=30.0,
                        close_callback_timeout=5.0, inbox_prefix=DEFAULT_INBOX_PREFIX,
                        error_cb=_default_error_cb, disconnected_cb=_default_noop_cb,
                        reconnected_cb=_default_noop_cb, closed_cb=_default_noop_cb,
                        discovered_server_cb=_default_noop_cb)
    ConnectOptions(servers, name, verbose, pedantic, token, user, password, nkey, nkey_seed,
                   nkey_seed_path, jwt, jwt_path, credentials, credentials_path, signature_cb,
                   no_echo, tls_required, tls_first, tls_verify, tls_ca_path, tls_cert_path, tls_key_path, connect_timeout,
                   ping_interval, max_outstanding_pings, allow_reconnect, reconnect_wait,
                   reconnect_max_wait, reconnect_jitter, max_reconnect_attempts, pending_size,
                   write_buffer_size, write_buffer_latency, write_timeout,
                   max_control_line, max_inbound_payload,
                   max_header_bytes, max_stale_pong_waiters, sub_pending_msgs_limit,
                   sub_pending_bytes_limit, drain_timeout, close_callback_timeout,
                   inbox_prefix, error_cb, disconnected_cb, reconnected_cb, closed_cb,
                   discovered_server_cb)
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

Base.@kwdef mutable struct ServerInfo
    max_payload::Union{Int,Nothing} = nothing
    tls_required::Union{Bool,Nothing} = nothing
    tls_available::Union{Bool,Nothing} = nothing
    connect_urls::Union{Vector{String},Nothing} = nothing
    version::Union{String,Nothing} = nothing
    headers::Union{Bool,Nothing} = nothing
    nonce::Union{String,Nothing} = nothing
    ldm::Bool = false
end

function _merge_server_info!(dest::ServerInfo, src::ServerInfo)
    isnothing(src.max_payload) || (dest.max_payload = src.max_payload)
    isnothing(src.tls_required) || (dest.tls_required = src.tls_required)
    isnothing(src.tls_available) || (dest.tls_available = src.tls_available)
    isnothing(src.connect_urls) || (dest.connect_urls = copy(src.connect_urls))
    isnothing(src.version) || (dest.version = src.version)
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
    subject_cache::Dict{Int,String}
end

ProtocolReader(io::I; read_size::Int=4096) where {I} =
    ProtocolReader{I}(io; read_size)

ProtocolReader{I}(io; read_size::Int=4096) where {I} =
    ProtocolReader{I}(io, UInt8[], 1, 0, Vector{UInt8}(undef, read_size), Dict{Int,String}())

struct _ProtocolFrame
    op::Symbol
    msg::Union{Msg,Nothing}
    info::Union{ServerInfo,Nothing}
    err::Union{String,Nothing}
end

@inline _protocol_msg_frame(msg::Msg) = _ProtocolFrame(:MSG, msg, nothing, nothing)
@inline _protocol_info_frame(info::ServerInfo) = _ProtocolFrame(:INFO, nothing, info, nothing)
@inline _protocol_err_frame(err::AbstractString) = _ProtocolFrame(:ERR, nothing, nothing, String(err))
@inline _protocol_control_frame(op::Symbol) = _ProtocolFrame(op, nothing, nothing, nothing)

@inline _protocol_msg(frame::_ProtocolFrame) = something(frame.msg)
@inline _protocol_info(frame::_ProtocolFrame)::ServerInfo = something(frame.info)
@inline _protocol_err(frame::_ProtocolFrame)::String = something(frame.err)

mutable struct BufferedWriteIO{I} <: IO
    io::I
    buffer::IOBuffer
    replayable_ranges::Vector{Tuple{Int,Int}}
    closed::Bool
end

BufferedWriteIO(io::I) where {I} = BufferedWriteIO{I}(io, IOBuffer(), Tuple{Int,Int}[], false)

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
    empty!(io.replayable_ranges)
    nothing
end

function Base.close(io::BufferedWriteIO)
    io.closed = true
    close(io.io)
end

Base.isopen(io::BufferedWriteIO) = !io.closed && isopen(io.io)

_buffered_bytes(::IO) = 0
_buffered_bytes(io::BufferedWriteIO) = position(io.buffer)
_take_replayable_writes!(::IO) = UInt8[]
function _take_replayable_writes!(io::BufferedWriteIO)
    ranges = io.replayable_ranges
    data = io.buffer.data
    if isempty(ranges)
        truncate(io.buffer, 0)
        seekstart(io.buffer)
        empty!(io.replayable_ranges)
        return UInt8[]
    end

    bytes = 0
    for (first, last) in ranges
        bytes += last - first + 1
    end
    replayable = Vector{UInt8}(undef, bytes)
    pos = 1
    for (first, last) in ranges
        n = last - first + 1
        copyto!(replayable, pos, data, first, n)
        pos += n
    end
    truncate(io.buffer, 0)
    seekstart(io.buffer)
    empty!(io.replayable_ranges)
    replayable
end
_underlying_transport(io) = io
_underlying_transport(io::BufferedWriteIO) = io.io

mutable struct PendingBuffer
    chunks::Vector{Vector{UInt8}}
    head::Int
end

PendingBuffer() = PendingBuffer(Vector{UInt8}[], 1)

Base.isempty(buffer::PendingBuffer) = buffer.head > length(buffer.chunks)

function Base.empty!(buffer::PendingBuffer)
    empty!(buffer.chunks)
    buffer.head = 1
    buffer
end

function _compact_pending_buffer!(buffer::PendingBuffer)
    if buffer.head > 1
        deleteat!(buffer.chunks, 1:(buffer.head - 1))
        buffer.head = 1
    end
    buffer
end

function _push_pending_chunk!(buffer::PendingBuffer, data::Vector{UInt8})
    push!(buffer.chunks, data)
    buffer
end

function _prepend_pending_chunk!(buffer::PendingBuffer, data::Vector{UInt8})
    if isempty(buffer)
        empty!(buffer)
        push!(buffer.chunks, data)
    elseif buffer.head > 1
        buffer.head -= 1
        buffer.chunks[buffer.head] = data
    else
        pushfirst!(buffer.chunks, data)
    end
    buffer
end

function _pop_pending_batch!(buffer::PendingBuffer, max_bytes::Int)::Vector{UInt8}
    isempty(buffer) && return UInt8[]
    max_bytes = max(1, max_bytes)
    stop = buffer.head - 1
    total = 0
    while stop < length(buffer.chunks)
        chunk = buffer.chunks[stop + 1]
        if total > 0 && total + length(chunk) > max_bytes
            break
        end
        stop += 1
        total += length(chunk)
        total >= max_bytes && break
    end

    out = Vector{UInt8}(undef, total)
    pos = 1
    for i in buffer.head:stop
        chunk = buffer.chunks[i]
        n = length(chunk)
        copyto!(out, pos, chunk, 1, n)
        pos += n
        buffer.chunks[i] = EMPTY_BYTES
    end
    buffer.head = stop + 1
    if buffer.head > length(buffer.chunks)
        empty!(buffer)
    elseif buffer.head > 32 && buffer.head > length(buffer.chunks) ÷ 2
        _compact_pending_buffer!(buffer)
    end
    out
end

function Base.take!(buffer::PendingBuffer)
    isempty(buffer) && return UInt8[]
    total = 0
    for i in buffer.head:length(buffer.chunks)
        total += length(buffer.chunks[i])
    end
    out = Vector{UInt8}(undef, total)
    pos = 1
    for i in buffer.head:length(buffer.chunks)
        chunk = buffer.chunks[i]
        n = length(chunk)
        copyto!(out, pos, chunk, 1, n)
        pos += n
    end
    empty!(buffer)
    out
end

function Base.write(buffer::PendingBuffer, data::Vector{UInt8})
    _push_pending_chunk!(buffer, copy(data))
    length(data)
end

function Base.write(buffer::PendingBuffer, data::AbstractString)
    bytes = Vector{UInt8}(undef, ncodeunits(data))
    copyto!(bytes, 1, codeunits(data), 1, length(bytes))
    _push_pending_chunk!(buffer, bytes)
    length(bytes)
end

function _pending_buffer_from(buffer::PendingBuffer)
    buffer
end

function _pending_buffer_from(buffer::IOBuffer)
    data = take!(buffer)
    pending = PendingBuffer()
    isempty(data) || _push_pending_chunk!(pending, data)
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
    lock::ReentrantLock
end
_JetStreamPushControlHandler(idle_heartbeat::Real=0.0; flow_control::Bool=true) =
    _JetStreamPushControlHandler(Threads.Atomic{Float64}(Float64(idle_heartbeat)),
                                 Threads.Atomic{Bool}(flow_control),
                                 Threads.Atomic{Float64}(time()),
                                 Threads.Atomic{Bool}(false),
                                 Threads.Atomic{UInt64}(UInt64(0)),
                                 UInt64(0), nothing, UInt64(0),
                                 ReentrantLock())
struct _RequestMuxControlHandler end

const _SubscriptionControlHandler = Union{_NoSubscriptionControlHandler,_JetStreamPushControlHandler,_RequestMuxControlHandler}

mutable struct MsgQueue{T}
    buffer::Vector{Union{T,Nothing}}
    head::Int
    len::Int
    closed::Bool
end

function MsgQueue{T}(capacity::Int) where {T}
    capacity > 0 || throw(ArgumentError("message queue capacity must be positive"))
    buffer = Vector{Union{T,Nothing}}(undef, capacity)
    fill!(buffer, nothing)
    MsgQueue{T}(buffer, 1, 0, false)
end

Base.isopen(q::MsgQueue) = !q.closed
Base.isready(q::MsgQueue) = q.len > 0
Base.n_avail(q::MsgQueue) = q.len
Base.length(q::MsgQueue) = q.len

function Base.close(q::MsgQueue)
    q.closed = true
    q
end

function Base.put!(q::MsgQueue{T}, msg::T) where {T}
    q.closed && throw(InvalidStateException("message queue is closed", :closed))
    q.len < length(q.buffer) || throw(InvalidStateException("message queue is full", :open))
    idx = mod1(q.head + q.len, length(q.buffer))
    q.buffer[idx] = msg
    q.len += 1
    q
end

function Base.take!(q::MsgQueue{T}) where {T}
    q.len > 0 || throw(InvalidStateException("message queue is empty", q.closed ? :closed : :open))
    msg = q.buffer[q.head]::T
    q.buffer[q.head] = nothing
    q.len -= 1
    q.head = q.len == 0 ? 1 : mod1(q.head + 1, length(q.buffer))
    msg
end

mutable struct Subscription{C<:AbstractNatterClient}
    client::C
    lock::ReentrantLock
    sid::Int
    subject::String
    queue::Union{String,Nothing}
    has_callback::Bool
    messages::MsgQueue{Msg}
    condition::Base.GenericCondition{ReentrantLock}
    control_handler::_SubscriptionControlHandler
    pending_msgs_limit::Int
    pending_bytes_limit::Int
    pending_bytes::Int
    received::Int
    max_msgs::Int
    closed::Bool
    processor::Union{Task,Nothing}
    server_active::Bool
    processing::Int
end

function Subscription(client::C, sid::Int, subject::String, queue::Union{String,Nothing}, callback,
                      lock::ReentrantLock,
                      messages::MsgQueue{Msg}, condition::Base.GenericCondition{ReentrantLock},
                      control_handler::_SubscriptionControlHandler,
                      pending_msgs_limit::Int, pending_bytes_limit::Int, pending_bytes::Int,
                      received::Int, max_msgs::Int, closed::Bool, processor::Union{Task,Nothing},
                      server_active::Bool, processing::Int) where {C<:AbstractNatterClient}
    Subscription{C}(client, lock, sid, subject, queue, !isnothing(callback), messages, condition,
                    control_handler, pending_msgs_limit, pending_bytes_limit, pending_bytes,
                    received, max_msgs, closed, processor, server_active, processing)
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
end

RequestWaiter{C}(deadline::Real=Inf) where {C<:AbstractNatterClient} =
    RequestWaiter{C}(false, nothing, true, Float64(deadline))

mutable struct RequestMux{C<:AbstractNatterClient}
    prefix::String
    sub::Subscription{C}
    waiters::Dict{String,RequestWaiter{C}}
    condition::Base.GenericCondition{ReentrantLock}
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
    write_io::Union{WriteIO,Nothing}
    lock::ReentrantLock
    write_lock::ReentrantLock
    write_scratch::Vector{UInt8}
    write_deadline::Threads.Atomic{Float64}
    write_epoch::Threads.Atomic{Int}
    write_timed_out_epoch::Threads.Atomic{Int}
    write_timeout_io::Base.RefValue{Any}
    write_timeout_operation::Base.RefValue{String}
    write_timeout_task::Union{Task,Nothing}
    flush_signal::Channel{Bool}
    flusher_task::Union{Task,Nothing}
    sid::Int
    subscriptions::Dict{Int,Subscription{Client{Options,ReadIO,WriteIO}}}
    @atomic subscription_snapshot::Vector{Union{Subscription{Client{Options,ReadIO,WriteIO}},Nothing}}
    @atomic request_mux::Union{RequestMux{Client{Options,ReadIO,WriteIO}},Nothing}
    request_mux_lock::ReentrantLock
    pending::PendingBuffer
    pending_bytes::Int
    pongs::PongWaiterQueue
    reader_task::Union{Task,Nothing}
    ping_task::Union{Task,Nothing}
    reconnect_task::Union{Task,Nothing}
    pings_out::Int
    stats::AtomicStats
    rng::MersenneTwister
    generation::Int
    generation_value::Threads.Atomic{Int}
end

function Client(options::Options, servers::Vector{Server}, current_server::Union{Server,Nothing},
                connected_url::Union{String,Nothing}, status::ConnectionStatus.T,
                info::ServerInfo, socket::Union{Sockets.TCPSocket,Nothing}, read_io, write_io,
                lock::ReentrantLock, write_lock::ReentrantLock, flush_signal::Channel{Bool},
                flusher_task::Union{Task,Nothing}, sid::Int, subscriptions,
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
    max_sid = 0
    for sub_sid in keys(typed_subscriptions)
        max_sid = max(max_sid, sub_sid)
    end
    subscription_snapshot = Vector{Union{Subscription{client_type},Nothing}}(undef, max_sid)
    fill!(subscription_snapshot, nothing)
    for (sub_sid, sub) in typed_subscriptions
        sub_sid > 0 && (subscription_snapshot[sub_sid] = sub)
    end
    typed_request_mux = if isnothing(request_mux)
        nothing
    else
        sub = typed_subscriptions[request_mux.sub.sid]
        waiters = Dict{String,RequestWaiter{client_type}}()
        for (token, waiter) in request_mux.waiters
            typed_waiter = RequestWaiter{client_type}(waiter.deadline)
            typed_waiter.ready = waiter.ready
            typed_waiter.active = waiter.active
            if waiter.value isa Exception || isnothing(waiter.value)
                typed_waiter.value = waiter.value
            end
            waiters[token] = typed_waiter
        end
        RequestMux{client_type}(request_mux.prefix, sub, waiters, Base.Threads.Condition(ReentrantLock()), nothing)
    end
    reader = isnothing(read_io) ? nothing : ProtocolReader(read_io)
    pending = _pending_buffer_from(pending)
    atomic_stats = stats isa AtomicStats ? stats : AtomicStats(stats)
    Client{Options,ReadIO,WriteIO}(options, servers, current_server, connected_url, status,
                                   Threads.Atomic{Int}(Int(status)),
                                   info, Threads.Atomic{Int}(something(info.max_payload, typemax(Int))),
                                   Threads.Atomic{Bool}(info.headers === true),
                                   socket, read_io, reader, write_io, lock, write_lock,
                                   UInt8[], Threads.Atomic{Float64}(Inf), Threads.Atomic{Int}(0),
                                   Threads.Atomic{Int}(0), Ref{Any}(nothing), Ref(""), nothing,
                                   flush_signal, flusher_task,
                                   sid, typed_subscriptions, subscription_snapshot,
                                   typed_request_mux, request_mux_lock,
                                   pending, pending_bytes, pongs,
                                   reader_task, ping_task, reconnect_task, pings_out, atomic_stats,
                                   rng, generation, Threads.Atomic{Int}(generation))
end

function _set_subscription_snapshot_locked!(client::C, sid::Int,
                                            sub::Union{Subscription{C},Nothing}) where {C<:Client}
    sid > 0 || return nothing
    current = @atomic client.subscription_snapshot
    isnothing(sub) && sid > length(current) && return nothing
    if sid <= length(current) && current[sid] === sub
        return nothing
    end
    snapshot = if sid <= length(current)
        copy(current)
    else
        expanded = Vector{Union{Subscription{C},Nothing}}(undef, sid)
        fill!(expanded, nothing)
        isempty(current) || copyto!(expanded, 1, current, 1, length(current))
        expanded
    end
    snapshot[sid] = sub
    if isnothing(sub)
        last = length(snapshot)
        while last > 0 && isnothing(snapshot[last])
            last -= 1
        end
        last < length(snapshot) && resize!(snapshot, last)
    end
    @atomic client.subscription_snapshot = snapshot
    nothing
end

@inline function _lookup_subscription(client::Client, sid::Int)
    snapshot = @atomic client.subscription_snapshot
    if 0 < sid <= length(snapshot)
        @inbounds return snapshot[sid]
    end
    nothing
end

function _wait_until_condition_locked(predicate::Function, condition::Base.GenericCondition{ReentrantLock},
                                      timeout::Real)::Bool
    predicate() && return true
    seconds = Float64(timeout)
    seconds > 0 || return false
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
            timed_out[] && return false
            wait(condition)
        end
        true
    finally
        isnothing(timer) || close(timer)
    end
end

function _notify_subscription_waiters_locked(sub::Subscription; all::Bool=false)
    notify(sub.condition; all)
    nothing
end

function _notify_subscription_waiters!(sub::Subscription; all::Bool=false)
    @lock sub.lock _notify_subscription_waiters_locked(sub; all)
    nothing
end

function _notify_all_subscription_waiters_locked(client; all::Bool=true)
    for sub in values(client.subscriptions)
        _notify_subscription_waiters!(sub; all)
    end
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
@inline _store_status_locked!(client::Client, value::ConnectionStatus.T) =
    (client.status = value; client.status_code[] = Int(value); value)
@inline _load_generation(client::Client)::Int = client.generation_value[]
@inline _store_generation_locked!(client::Client, value::Int) =
    (client.generation = value; client.generation_value[] = value; value)
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
connected_url(client::Client) = (@lock client.lock client.connected_url)
