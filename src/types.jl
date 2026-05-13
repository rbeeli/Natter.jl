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

function _default_error_cb(err)
    @warn "Natter client error" exception=err
    nothing
end

_default_noop_cb() = nothing

const Headers = Dict{String,Vector{String}}

abstract type AbstractNatterClient end

function _headers_copy(h::Union{Headers,Nothing})
    isnothing(h) ? Headers() : Headers(k => copy(v) for (k, v) in h)
end

function _headers_from_pairs(pairs::Union{Nothing,AbstractVector})
    h = Headers()
    isnothing(pairs) && return h
    for pair in pairs
        push!(get!(h, String(first(pair)), String[]), String(last(pair)))
    end
    h
end

headers(msg) = _headers_copy(msg.headers)
header(msg, key::AbstractString) = begin
    values = get(msg.headers, String(key), String[])
    isempty(values) ? nothing : first(values)
end

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

mutable struct Msg
    subject::String
    reply::Union{String,Nothing}
    data::Vector{UInt8}
    headers::Headers
    client::Union{AbstractNatterClient,Nothing}
    sid::Int
    acked::Bool
end

Msg(subject::String, reply::Union{String,Nothing}, data::Vector{UInt8}; headers=Headers(), client=nothing, sid=0) =
    Msg(subject, reply, data, _headers_copy(headers), client, sid, false)

Base.String(msg::Msg) = String(msg.data)

Base.@kwdef mutable struct ConnectOptions{ErrorCallback,DisconnectedCallback,ReconnectedCallback,ClosedCallback,DiscoveredServerCallback}
    servers::Vector{String} = [DEFAULT_URL]
    name::Union{String,Nothing} = nothing
    verbose::Bool = false
    pedantic::Bool = false
    token::Union{String,Nothing} = nothing
    user::Union{String,Nothing} = nothing
    password::Union{String,Nothing} = nothing
    no_echo::Bool = false
    tls_required::Bool = false
    tls_first::Union{Bool,Nothing} = nothing
    tls_verify::Bool = true
    tls_ca_path::Union{String,Nothing} = nothing
    tls_cert_path::Union{String,Nothing} = nothing
    tls_key_path::Union{String,Nothing} = nothing
    connect_timeout::Float64 = 2.0
    ping_interval::Float64 = 120.0
    max_outstanding_pings::Int = 2
    allow_reconnect::Bool = true
    reconnect_wait::Float64 = 0.5
    reconnect_max_wait::Float64 = 5.0
    reconnect_jitter::Float64 = 0.1
    max_reconnect_attempts::Int = -1
    pending_size::Int = 2 * 1024 * 1024
    max_control_line::Int = DEFAULT_MAX_CONTROL_LINE
    max_inbound_payload::Int = DEFAULT_MAX_INBOUND_PAYLOAD
    max_header_bytes::Int = DEFAULT_MAX_HEADER_BYTES
    max_stale_pong_waiters::Int = 1024
    sub_pending_msgs_limit::Int = 1024
    sub_pending_bytes_limit::Int = 128 * 1024 * 1024
    drain_timeout::Float64 = 30.0
    inbox_prefix::String = DEFAULT_INBOX_PREFIX
    error_cb::ErrorCallback = _default_error_cb
    disconnected_cb::DisconnectedCallback = _default_noop_cb
    reconnected_cb::ReconnectedCallback = _default_noop_cb
    closed_cb::ClosedCallback = _default_noop_cb
    discovered_server_cb::DiscoveredServerCallback = _default_noop_cb
end

mutable struct Server
    url::String
    reconnects::Int
    last_attempt::Float64
    discovered::Bool
end
Server(url::String; discovered=false) = Server(url, 0, 0.0, discovered)

Base.@kwdef mutable struct ServerInfo
    max_payload::Union{Int,Nothing} = nothing
    tls_required::Union{Bool,Nothing} = nothing
    tls_available::Union{Bool,Nothing} = nothing
    connect_urls::Union{Vector{String},Nothing} = nothing
    version::Union{String,Nothing} = nothing
    ldm::Bool = false
end

function _merge_server_info!(dest::ServerInfo, src::ServerInfo)
    isnothing(src.max_payload) || (dest.max_payload = src.max_payload)
    isnothing(src.tls_required) || (dest.tls_required = src.tls_required)
    isnothing(src.tls_available) || (dest.tls_available = src.tls_available)
    isnothing(src.connect_urls) || (dest.connect_urls = copy(src.connect_urls))
    isnothing(src.version) || (dest.version = src.version)
    dest.ldm = src.ldm
    dest
end

mutable struct ProtocolReader{I}
    io::I
    buffer::Vector{UInt8}
    first::Int
    last::Int
    scratch::Vector{UInt8}
end

ProtocolReader(io::I; read_size::Int=4096) where {I} =
    ProtocolReader{I}(io; read_size)

ProtocolReader{I}(io; read_size::Int=4096) where {I} =
    ProtocolReader{I}(io, UInt8[], 1, 0, Vector{UInt8}(undef, read_size))

const DefaultTransportIO = Union{Sockets.TCPSocket,MbedTLS.SSLContext}

_transport_field_type(::Nothing) = DefaultTransportIO
_transport_field_type(io) = typeof(io)

struct _NoSubscriptionControlHandler end
struct _JetStreamPushControlHandler end
struct _RequestMuxControlHandler end

const _SubscriptionControlHandler = Union{_NoSubscriptionControlHandler,_JetStreamPushControlHandler,_RequestMuxControlHandler}

mutable struct Subscription{C<:AbstractNatterClient}
    client::C
    sid::Int
    subject::String
    queue::Union{String,Nothing}
    has_callback::Bool
    messages::Channel{Msg}
    control_handler::_SubscriptionControlHandler
    pending_msgs_limit::Int
    pending_bytes_limit::Int
    pending_bytes::Int
    received::Int
    max_msgs::Int
    closed::Bool
    processor::Union{Task,Nothing}
    auto_ack::Bool
    server_active::Bool
    processing::Int
end

function Subscription(client::C, sid::Int, subject::String, queue::Union{String,Nothing}, callback,
                      messages::Channel{Msg}, control_handler::_SubscriptionControlHandler,
                      pending_msgs_limit::Int, pending_bytes_limit::Int, pending_bytes::Int,
                      received::Int, max_msgs::Int, closed::Bool, processor::Union{Task,Nothing},
                      auto_ack::Bool, server_active::Bool, processing::Int) where {C<:AbstractNatterClient}
    Subscription{C}(client, sid, subject, queue, !isnothing(callback), messages,
                    control_handler, pending_msgs_limit, pending_bytes_limit, pending_bytes,
                    received, max_msgs, closed, processor, auto_ack, server_active, processing)
end

mutable struct PongWaiter
    channel::Union{Channel{Bool},Nothing}
end

mutable struct RequestMux{C<:AbstractNatterClient}
    prefix::String
    sub::Subscription{C}
    waiters::Dict{String,Channel{Any}}
end

mutable struct Client{Options<:ConnectOptions,ReadIO,WriteIO} <: AbstractNatterClient
    options::Options
    servers::Vector{Server}
    current_server::Union{Server,Nothing}
    connected_url::Union{String,Nothing}
    status::ConnectionStatus.T
    info::ServerInfo
    socket::Union{Sockets.TCPSocket,Nothing}
    read_io::Union{ReadIO,Nothing}
    reader::Union{ProtocolReader{ReadIO},Nothing}
    write_io::Union{WriteIO,Nothing}
    lock::ReentrantLock
    write_lock::ReentrantLock
    sid::Int
    subscriptions::Dict{Int,Subscription{Client{Options,ReadIO,WriteIO}}}
    request_mux::Union{RequestMux{Client{Options,ReadIO,WriteIO}},Nothing}
    request_mux_lock::ReentrantLock
    pending::IOBuffer
    pending_bytes::Int
    pongs::Vector{PongWaiter}
    reader_task::Union{Task,Nothing}
    ping_task::Union{Task,Nothing}
    reconnect_task::Union{Task,Nothing}
    pings_out::Int
    stats::Stats
    rng::MersenneTwister
    generation::Int
end

function Client(options::Options, servers::Vector{Server}, current_server::Union{Server,Nothing},
                connected_url::Union{String,Nothing}, status::ConnectionStatus.T,
                info::ServerInfo, socket::Union{Sockets.TCPSocket,Nothing}, read_io, write_io,
                lock::ReentrantLock, write_lock::ReentrantLock, sid::Int, subscriptions,
                request_mux::Union{RequestMux,Nothing}, request_mux_lock::ReentrantLock,
                pending::IOBuffer, pending_bytes::Int, pongs::Vector{PongWaiter},
                reader_task::Union{Task,Nothing}, ping_task::Union{Task,Nothing},
                reconnect_task::Union{Task,Nothing}, pings_out::Int, stats::Stats,
                rng::MersenneTwister, generation::Int) where {Options<:ConnectOptions}
    ReadIO = _transport_field_type(read_io)
    WriteIO = _transport_field_type(write_io)
    client_type = Client{Options,ReadIO,WriteIO}
    typed_subscriptions = Dict{Int,Subscription{client_type}}()
    for (sub_sid, sub) in subscriptions
        typed_subscriptions[Int(sub_sid)] = sub
    end
    typed_request_mux = if isnothing(request_mux)
        nothing
    else
        sub = typed_subscriptions[request_mux.sub.sid]
        RequestMux{client_type}(request_mux.prefix, sub, request_mux.waiters)
    end
    reader = isnothing(read_io) ? nothing : ProtocolReader{ReadIO}(read_io)
    Client{Options,ReadIO,WriteIO}(options, servers, current_server, connected_url, status,
                                   info, socket, read_io, reader, write_io, lock, write_lock,
                                   sid, typed_subscriptions, typed_request_mux, request_mux_lock,
                                   pending, pending_bytes, pongs,
                                   reader_task, ping_task, reconnect_task, pings_out, stats,
                                   rng, generation)
end

_read_transport_type(::Client{Options,ReadIO,WriteIO}) where {Options,ReadIO,WriteIO} = ReadIO

status(client::Client) = (@lock client.lock client.status)
stats(client::Client) = @lock client.lock Stats(; in_msgs=client.stats.in_msgs, out_msgs=client.stats.out_msgs,
                                                 in_bytes=client.stats.in_bytes, out_bytes=client.stats.out_bytes,
                                                 reconnects=client.stats.reconnects, errors=client.stats.errors,
                                                 dropped_msgs=client.stats.dropped_msgs)
connected_url(client::Client) = (@lock client.lock client.connected_url)
