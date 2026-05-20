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
    tcp_nodelay::Bool
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
    write_driver::Bool
    write_queue_msgs::Int
    write_queue_bytes::Int
    write_batch_msgs::Int
    write_batch_bytes::Int
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
        tls_verify, tls_server_name, tls_ca_path, tls_cert_path, tls_key_path, tcp_nodelay, connect_timeout, ping_interval,
        max_outstanding_pings, allow_reconnect, retry_on_initial_connect,
        reconnect_wait, reconnect_max_wait, reconnect_jitter, max_reconnect_attempts,
        pending_size, read_buffer_size, read_buffer_shrink_threshold, write_buffer_size, direct_write_threshold,
        write_buffer_latency, write_timeout, write_driver, write_queue_msgs,
        write_queue_bytes, write_batch_msgs, write_batch_bytes, record_stats,
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
        tcp_nodelay = _connect_option_bool("tcp_nodelay", tcp_nodelay)
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
        write_driver = _connect_option_bool("write_driver", write_driver)
        write_queue_msgs = _connect_option_positive_int("write_queue_msgs", write_queue_msgs)
        write_queue_bytes = _connect_option_positive_int("write_queue_bytes", write_queue_bytes)
        write_batch_msgs = _connect_option_positive_int("write_batch_msgs", write_batch_msgs)
        write_batch_bytes = _connect_option_positive_int("write_batch_bytes", write_batch_bytes)
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
            tls_verify, tls_server_name, tls_ca_path, tls_cert_path, tls_key_path, tcp_nodelay, connect_timeout, ping_interval,
            max_outstanding_pings, allow_reconnect, retry_on_initial_connect,
            reconnect_wait, reconnect_max_wait, reconnect_jitter, max_reconnect_attempts,
            pending_size, read_buffer_size, read_buffer_shrink_threshold, write_buffer_size, direct_write_threshold,
            write_buffer_latency, write_timeout, write_driver, write_queue_msgs,
            write_queue_bytes, write_batch_msgs, write_batch_bytes, record_stats,
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
                        tcp_nodelay=true,
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
                        write_driver=true, write_queue_msgs=8192,
                        write_queue_bytes=2 * 1024 * 1024, write_batch_msgs=4096,
                        write_batch_bytes=DEFAULT_WRITE_BATCH_BYTES,
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
                   tls_cert_path, tls_key_path, tcp_nodelay, connect_timeout,
                   ping_interval, max_outstanding_pings, allow_reconnect,
                   retry_on_initial_connect, reconnect_wait, reconnect_max_wait,
                   reconnect_jitter, max_reconnect_attempts, pending_size,
                   read_buffer_size, read_buffer_shrink_threshold, write_buffer_size,
                   direct_write_threshold, write_buffer_latency, write_timeout,
                   write_driver, write_queue_msgs, write_queue_bytes, write_batch_msgs,
                   write_batch_bytes,
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
