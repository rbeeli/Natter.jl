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

const Headers = Dict{String,Vector{String}}

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
    client::Any
    sid::Int
    acked::Bool
end

Msg(subject::String, reply::Union{String,Nothing}, data::Vector{UInt8}; headers=Headers(), client=nothing, sid=0) =
    Msg(subject, reply, data, _headers_copy(headers), client, sid, false)

Base.String(msg::Msg) = String(msg.data)

Base.@kwdef mutable struct ConnectOptions
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
    sub_pending_msgs_limit::Int = 1024
    sub_pending_bytes_limit::Int = 128 * 1024 * 1024
    drain_timeout::Float64 = 30.0
    inbox_prefix::String = DEFAULT_INBOX_PREFIX
    error_cb::Function = _default_error_cb
    disconnected_cb::Function = () -> nothing
    reconnected_cb::Function = () -> nothing
    closed_cb::Function = () -> nothing
    discovered_server_cb::Function = () -> nothing
end

mutable struct Server
    url::String
    reconnects::Int
    last_attempt::Float64
    discovered::Bool
end
Server(url::String; discovered=false) = Server(url, 0, 0.0, discovered)

mutable struct Subscription
    client::Any
    sid::Int
    subject::String
    queue::Union{String,Nothing}
    callback::Union{Function,Nothing}
    messages::Channel{Msg}
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

mutable struct Client
    options::ConnectOptions
    servers::Vector{Server}
    current_server::Union{Server,Nothing}
    connected_url::Union{String,Nothing}
    status::ConnectionStatus.T
    info::Dict{String,Any}
    socket::Any
    read_io::Any
    write_io::Any
    lock::ReentrantLock
    write_lock::ReentrantLock
    sid::Int
    subscriptions::Dict{Int,Subscription}
    pending::IOBuffer
    pending_bytes::Int
    pongs::Vector{Channel{Bool}}
    reader_task::Union{Task,Nothing}
    ping_task::Union{Task,Nothing}
    reconnect_task::Union{Task,Nothing}
    pings_out::Int
    stats::Stats
    rng::MersenneTwister
    generation::Int
end

status(client::Client) = (@lock client.lock client.status)
stats(client::Client) = @lock client.lock Stats(; in_msgs=client.stats.in_msgs, out_msgs=client.stats.out_msgs,
                                                 in_bytes=client.stats.in_bytes, out_bytes=client.stats.out_bytes,
                                                 reconnects=client.stats.reconnects, errors=client.stats.errors,
                                                 dropped_msgs=client.stats.dropped_msgs)
connected_url(client::Client) = (@lock client.lock client.connected_url)
