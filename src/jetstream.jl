Base.@kwdef struct JetStreamContext
    client::Client
    prefix::String = "\$JS.API"
    timeout::Float64 = 5.0
end

struct PubAck
    stream::String
    seq::Int
    duplicate::Bool
    domain::Union{String,Nothing}
end

struct StreamInfo
    name::String
    config::StreamConfig
    state::Dict{String,Any}
    raw::Dict{String,Any}
end

struct ConsumerInfo
    stream_name::String
    name::String
    config::ConsumerConfig
    raw::Dict{String,Any}
end

jetstream(client::Client; prefix::String="\$JS.API", timeout::Real=5.0) = JetStreamContext(client, prefix, Float64(timeout))

function _js_decode(msg::Msg)
    obj = isempty(msg.data) ? Dict{String,Any}() : _json_dict(String(msg.data))
    if haskey(obj, "error")
        err = Dict{String,Any}(String(k) => v for (k, v) in pairs(obj["error"]))
        throw(JetStreamError(Int(get(err, "code", 0)), get(err, "err_code", nothing), String(get(err, "description", ""))))
    end
    obj
end

function _api_request(js::JetStreamContext, subject::String, payload=nothing; timeout::Real=js.timeout)
    msg = request(js.client, subject, payload; timeout)
    _js_decode(msg)
end

function _validate_api_name(kind::AbstractString, name)
    n = String(name)
    isempty(n) && throw(ArgumentError("$kind name cannot be empty"))
    for c in n
        if isspace(c) || c in ('.', '*', '>', '/', '\\') || !isprint(c)
            throw(ArgumentError("$kind name contains an invalid character: $n"))
        end
    end
    n
end

function _puback(obj)
    PubAck(String(obj["stream"]), Int(obj["seq"]), Bool(get(obj, "duplicate", false)), haskey(obj, "domain") ? String(obj["domain"]) : nothing)
end

function js_publish(js::JetStreamContext, subject::AbstractString, data=nothing; timeout::Real=js.timeout, stream::Union{String,Nothing}=nothing,
                    headers::Union{Headers,Nothing}=nothing)
    hdrs = isnothing(headers) ? Headers() : _headers_copy(headers)
    isnothing(stream) || push!(get!(hdrs, "Nats-Expected-Stream", String[]), stream)
    msg = request(js.client, subject, data; timeout, headers=hdrs)
    _puback(_js_decode(msg))
end

function publish_async(js::JetStreamContext, subject::AbstractString, data=nothing; kwargs...)
    @async js_publish(js, subject, data; kwargs...)
end

function _stream_config_name(config::StreamConfig)::String
    isnothing(config.name) && throw(ArgumentError("stream config name is required"))
    _validate_api_name("stream", config.name)
end

function _stream_config_name(config::AbstractDict)::String
    haskey(config, "name") || throw(ArgumentError("stream config name is required"))
    _validate_api_name("stream", config["name"])
end

function stream_create(js::JetStreamContext, config::StreamConfig; timeout::Real=js.timeout)
    name = _stream_config_name(config)
    payload = _js_config_payload(config)
    _stream_info(_api_request(js, "$(js.prefix).STREAM.CREATE.$name", JSON3.write(payload); timeout))
end

function stream_create(js::JetStreamContext, config::AbstractDict{String,<:Any}; timeout::Real=js.timeout)
    name = _stream_config_name(config)
    payload = _js_config_payload(config)
    _stream_info(_api_request(js, "$(js.prefix).STREAM.CREATE.$name", JSON3.write(payload); timeout))
end

function stream_update(js::JetStreamContext, config::StreamConfig; timeout::Real=js.timeout)
    name = _stream_config_name(config)
    payload = _js_config_payload(config)
    _stream_info(_api_request(js, "$(js.prefix).STREAM.UPDATE.$name", JSON3.write(payload); timeout))
end

function stream_update(js::JetStreamContext, config::AbstractDict{String,<:Any}; timeout::Real=js.timeout)
    name = _stream_config_name(config)
    payload = _js_config_payload(config)
    _stream_info(_api_request(js, "$(js.prefix).STREAM.UPDATE.$name", JSON3.write(payload); timeout))
end

stream_info(js::JetStreamContext, name::AbstractString; timeout::Real=js.timeout) =
    _stream_info(_api_request(js, "$(js.prefix).STREAM.INFO.$(_validate_api_name("stream", name))", ""; timeout))

function stream_list(js::JetStreamContext; offset::Int=0, timeout::Real=js.timeout)
    streams = StreamInfo[]
    next_offset = offset
    while true
        obj = _api_request(js, "$(js.prefix).STREAM.LIST", JSON3.write(Dict("offset" => next_offset)); timeout)
        items = get(obj, "streams", Any[])
        append!(streams, [_stream_info(Dict{String,Any}(String(k) => v for (k, v) in pairs(item))) for item in items])
        total = Int(get(obj, "total", offset + length(streams)))
        page_offset = Int(get(obj, "offset", next_offset))
        next_offset = page_offset + length(items)
        (isempty(items) || next_offset >= total) && break
    end
    streams
end

function stream_names(js::JetStreamContext; subject::Union{String,Nothing}=nothing, timeout::Real=js.timeout)
    names = String[]
    next_offset = 0
    while true
        req = isnothing(subject) ? Dict{String,Any}("offset" => next_offset) : Dict{String,Any}("subject" => subject, "offset" => next_offset)
        obj = _api_request(js, "$(js.prefix).STREAM.NAMES", JSON3.write(req); timeout)
        items = String.(get(obj, "streams", String[]))
        append!(names, items)
        total = Int(get(obj, "total", length(names)))
        page_offset = Int(get(obj, "offset", next_offset))
        next_offset = page_offset + length(items)
        (isempty(items) || next_offset >= total) && break
    end
    names
end

stream_delete(js::JetStreamContext, name::AbstractString; timeout::Real=js.timeout) =
    Bool(_api_request(js, "$(js.prefix).STREAM.DELETE.$(_validate_api_name("stream", name))", ""; timeout)["success"])

stream_purge(js::JetStreamContext, name::AbstractString; filter_subject::Union{String,Nothing}=nothing, timeout::Real=js.timeout) = begin
    req = isnothing(filter_subject) ? Dict{String,Any}() : Dict{String,Any}("filter" => filter_subject)
    Bool(_api_request(js, "$(js.prefix).STREAM.PURGE.$(_validate_api_name("stream", name))", JSON3.write(req); timeout)["success"])
end

function _stream_info(obj::Dict{String,Any})
    cfg = _stream_config_from_payload(obj["config"])
    st = Dict{String,Any}(String(k) => v for (k, v) in pairs(obj["state"]))
    name = isnothing(cfg.name) ? String(get(_string_key_dict(obj["config"]), "name", "")) : cfg.name
    StreamInfo(name, cfg, st, obj)
end

function _validate_stream_sequence(seq::Int)::Int
    seq > 0 || throw(ArgumentError("stream sequence must be positive"))
    seq
end

function _stream_message_get_request(seq::Union{Int,Nothing}, subject::Union{String,Nothing}, next_by_subject::Bool)::Dict{String,Any}
    if next_by_subject
        isnothing(seq) && throw(ArgumentError("seq is required when next_by_subject=true"))
        isnothing(subject) && throw(ArgumentError("subject is required when next_by_subject=true"))
        return Dict{String,Any}("seq" => _validate_stream_sequence(seq), "next_by_subj" => _validate_publish_subject(subject))
    end
    if isnothing(seq) == isnothing(subject)
        throw(ArgumentError("provide exactly one of seq or subject"))
    end
    if isnothing(subject)
        return Dict{String,Any}("seq" => _validate_stream_sequence(seq))
    end
    Dict{String,Any}("last_by_subj" => _validate_publish_subject(subject))
end

const _DIRECT_GET_METADATA_HEADERS = Set(["Nats-Stream", "Nats-Subject", "Nats-Sequence", "Nats-Time-Stamp"])

function _direct_message_response(js::JetStreamContext, request_subject::String, response::Msg)::Msg
    code = _status_header(response)
    if code == 503
        throw(NoRespondersError(request_subject))
    elseif !isnothing(code) && code >= 400
        description = _status_description(response)
        isempty(description) && (description = "direct get failed")
        throw(JetStreamError(code, nothing, description))
    end

    subject = header(response, "Nats-Subject")
    isnothing(subject) && throw(ProtocolError("direct get response missing Nats-Subject header"))
    headers = _headers_copy(response.headers)
    for name in _DIRECT_GET_METADATA_HEADERS
        delete!(headers, name)
    end
    Msg(subject, nothing, copy(response.data); headers, client=js.client)
end

function _stream_message_get_direct(js::JetStreamContext, stream::String, req::Dict{String,Any}; timeout::Real)
    request_subject =
        if haskey(req, "last_by_subj") && !haskey(req, "seq")
            "$(js.prefix).DIRECT.GET.$stream.$(req["last_by_subj"])"
        else
            "$(js.prefix).DIRECT.GET.$stream"
        end
    payload = haskey(req, "last_by_subj") && !haskey(req, "seq") ? "" : JSON3.write(req)
    response = _request_raw(js.client, request_subject, payload; timeout)
    _direct_message_response(js, request_subject, response)
end

function stream_message_get(js::JetStreamContext, stream::AbstractString; seq::Union{Int,Nothing}=nothing, subject::Union{String,Nothing}=nothing,
                            direct::Bool=false, next_by_subject::Bool=false, timeout::Real=js.timeout)
    stream = _validate_api_name("stream", stream)
    req = _stream_message_get_request(seq, subject, next_by_subject)
    direct && return _stream_message_get_direct(js, stream, req; timeout)
    obj = _api_request(js, "$(js.prefix).STREAM.MSG.GET.$stream", JSON3.write(req); timeout)
    msg = obj["message"]
    data = haskey(msg, "data") ? base64decode(String(msg["data"])) : UInt8[]
    hdrs = haskey(msg, "hdrs") ? _parse_headers(base64decode(String(msg["hdrs"]))) : Headers()
    Msg(String(msg["subject"]), nothing, data; headers=hdrs, client=js.client)
end

stream_message_delete(js::JetStreamContext, stream::AbstractString, seq::Int; timeout::Real=js.timeout) =
    Bool(_api_request(js, "$(js.prefix).STREAM.MSG.DELETE.$(_validate_api_name("stream", stream))", JSON3.write(Dict("seq" => seq)); timeout)["success"])

function _server_supports_consumer_name(client::Client)
    version = @lock client.lock get(client.info, "version", nothing)
    isnothing(version) && return true
    m = match(r"^(\d+)\.(\d+)", String(version))
    isnothing(m) && return true
    major = parse(Int, m.captures[1])
    minor = parse(Int, m.captures[2])
    major > 2 || (major == 2 && minor >= 9)
end

function _consumer_create_subject(js::JetStreamContext, stream::AbstractString, config)
    stream = _validate_api_name("stream", stream)
    name = get(config, "name", nothing)
    durable = get(config, "durable_name", nothing)
    filter = get(config, "filter_subject", nothing)
    isnothing(name) || (name = _validate_api_name("consumer", name))
    isnothing(durable) || (durable = _validate_api_name("consumer", durable))
    if _server_supports_consumer_name(js.client) && !isnothing(name)
        base = "$(js.prefix).CONSUMER.CREATE.$stream.$name"
        filter_subject = isnothing(filter) ? nothing : String(filter)
        !isnothing(filter_subject) && !occursin(r"[*>\s]", filter_subject) ? "$base.$filter_subject" : base
    elseif !isnothing(durable)
        "$(js.prefix).CONSUMER.DURABLE.CREATE.$stream.$durable"
    else
        "$(js.prefix).CONSUMER.CREATE.$stream"
    end
end

function consumer_create(js::JetStreamContext, stream::AbstractString, config::ConsumerConfig; timeout::Real=js.timeout)
    stream = _validate_api_name("stream", stream)
    payload = _js_config_payload(config)
    subject = _consumer_create_subject(js, stream, payload)
    _consumer_info(_api_request(js, subject, JSON3.write(Dict("stream_name" => stream, "config" => payload)); timeout))
end

function consumer_create(js::JetStreamContext, stream::AbstractString, config::AbstractDict{String,<:Any}; timeout::Real=js.timeout)
    stream = _validate_api_name("stream", stream)
    payload = _js_config_payload(config)
    subject = _consumer_create_subject(js, stream, payload)
    _consumer_info(_api_request(js, subject, JSON3.write(Dict("stream_name" => stream, "config" => payload)); timeout))
end

function consumer_update(js::JetStreamContext, stream::AbstractString, config::Union{ConsumerConfig,AbstractDict{String,<:Any}}; timeout::Real=js.timeout)
    consumer_create(js, stream, config; timeout)
end

consumer_info(js::JetStreamContext, stream::AbstractString, consumer::AbstractString; timeout::Real=js.timeout) =
    _consumer_info(_api_request(js, "$(js.prefix).CONSUMER.INFO.$(_validate_api_name("stream", stream)).$(_validate_api_name("consumer", consumer))", ""; timeout))

function consumer_list(js::JetStreamContext, stream::AbstractString; offset::Int=0, timeout::Real=js.timeout)
    stream = _validate_api_name("stream", stream)
    consumers = ConsumerInfo[]
    next_offset = offset
    while true
        obj = _api_request(js, "$(js.prefix).CONSUMER.LIST.$stream", JSON3.write(Dict("offset" => next_offset)); timeout)
        items = get(obj, "consumers", Any[])
        append!(consumers, [_consumer_info(Dict{String,Any}(String(k) => v for (k, v) in pairs(item))) for item in items])
        total = Int(get(obj, "total", offset + length(consumers)))
        page_offset = Int(get(obj, "offset", next_offset))
        next_offset = page_offset + length(items)
        (isempty(items) || next_offset >= total) && break
    end
    consumers
end

consumer_delete(js::JetStreamContext, stream::AbstractString, consumer::AbstractString; timeout::Real=js.timeout) =
    Bool(_api_request(js, "$(js.prefix).CONSUMER.DELETE.$(_validate_api_name("stream", stream)).$(_validate_api_name("consumer", consumer))", ""; timeout)["success"])

function _consumer_info(obj::Dict{String,Any})
    cfg = _consumer_config_from_payload(obj["config"])
    ConsumerInfo(String(obj["stream_name"]), String(obj["name"]), cfg, obj)
end

mutable struct PullSubscription
    js::JetStreamContext
    sub::Subscription
    stream::String
    consumer::String
    deliver::String
    fetch_lock::ReentrantLock
    delete_on_close::Bool
end

mutable struct PushSubscription
    js::JetStreamContext
    sub::Subscription
    stream::String
    consumer::String
    delete_on_close::Bool
end

function _stream_by_subject(js::JetStreamContext, subject::AbstractString)
    names = stream_names(js; subject=String(subject))
    isempty(names) && throw(JetStreamError(404, nothing, "no stream found for subject $subject"))
    first(names)
end

function pull_subscribe(js::JetStreamContext, subject::AbstractString; stream::Union{String,Nothing}=nothing, durable::Union{String,Nothing}=nothing,
                        config::Union{ConsumerConfig,AbstractDict{String,<:Any}}=ConsumerConfig())
    stream = isnothing(stream) ? _stream_by_subject(js, subject) : stream
    cfg = _js_config_payload(config)
    delete_on_close = isnothing(durable) && !haskey(cfg, "durable_name")
    if !haskey(cfg, "filter_subject") && !haskey(cfg, "filter_subjects")
        cfg["filter_subject"] = String(subject)
    end
    if !isnothing(durable)
        cfg["name"] = get(cfg, "name", durable)
        cfg["durable_name"] = get(cfg, "durable_name", durable)
    else
        cfg["name"] = get(cfg, "name", @lock js.client.lock randstring(js.client.rng, 16))
    end
    info = consumer_create(js, stream, cfg)
    try
        deliver = new_inbox(js.client)
        sub = subscribe(js.client, deliver)
        PullSubscription(js, sub, stream, info.name, deliver, ReentrantLock(), delete_on_close)
    catch err
        if delete_on_close
            try
                consumer_delete(js, stream, info.name)
            catch cleanup_err
                throw(Base.CompositeException([err, CleanupError("delete pull consumer $(info.name)", cleanup_err)]))
            end
        end
        rethrow()
    end
end

function fetch(psub::PullSubscription, batch::Int=1; timeout::Real=psub.js.timeout, expires::Real=timeout)
    timeout >= expires || throw(ArgumentError("fetch timeout must be greater than or equal to expires"))
    @lock psub.fetch_lock begin
        req = Dict{String,Any}("batch" => batch, "expires" => round(Int, expires * 1_000_000_000))
        publish(psub.js.client, "$(psub.js.prefix).CONSUMER.MSG.NEXT.$(psub.stream).$(psub.consumer)", JSON3.write(req); reply=psub.deliver)
        msgs = Msg[]
        deadline = time() + timeout
        while length(msgs) < batch && time() < deadline
            remaining = max(0.001, deadline - time())
            try
                msg = next(psub.sub; timeout=remaining)
                code = _status_header(msg)
                description = _status_description(msg)
                if code == 100
                    continue
                elseif code in (404, 408)
                    break
                elseif code == 409 && occursin("exceeded", lowercase(description))
                    throw(JetStreamError(code, nothing, description))
                elseif code == 409
                    break
                elseif !isnothing(code) && code >= 400
                    throw(JetStreamError(code, nothing, description))
                else
                    push!(msgs, msg)
                end
            catch err
                err isa TimeoutError ? break : rethrow()
            end
        end
        msgs
    end
end

function push_subscribe(js::JetStreamContext, subject::AbstractString; stream::Union{String,Nothing}=nothing, durable::Union{String,Nothing}=nothing,
                        queue::Union{String,Nothing}=nothing, callback::Union{Function,Nothing}=nothing, manual_ack::Bool=false,
                        config::Union{ConsumerConfig,AbstractDict{String,<:Any}}=ConsumerConfig())
    stream = isnothing(stream) ? _stream_by_subject(js, subject) : stream
    deliver = new_inbox(js.client)
    cfg = _js_config_payload(config)
    delete_on_close = isnothing(durable) && !haskey(cfg, "durable_name")
    if !haskey(cfg, "filter_subject") && !haskey(cfg, "filter_subjects")
        cfg["filter_subject"] = String(subject)
    end
    cfg["deliver_subject"] = get(cfg, "deliver_subject", deliver)
    isnothing(queue) || (cfg["deliver_group"] = queue)
    if !isnothing(durable)
        cfg["name"] = get(cfg, "name", durable)
        cfg["durable_name"] = get(cfg, "durable_name", durable)
    else
        cfg["name"] = get(cfg, "name", @lock js.client.lock randstring(js.client.rng, 16))
    end
    info = consumer_create(js, stream, cfg)
    try
        sub = subscribe(js.client, String(cfg["deliver_subject"]); queue, callback, auto_ack=!manual_ack && !isnothing(callback))
        PushSubscription(js, sub, stream, info.name, delete_on_close)
    catch err
        if delete_on_close
            try
                consumer_delete(js, stream, info.name)
            catch cleanup_err
                throw(Base.CompositeException([err, CleanupError("delete push consumer $(info.name)", cleanup_err)]))
            end
        end
        rethrow()
    end
end

function close(psub::PullSubscription)
    errors = Any[]
    try
        close(psub.sub)
    catch err
        push!(errors, err)
    end
    if psub.delete_on_close
        try
            consumer_delete(psub.js, psub.stream, psub.consumer)
        catch err
            push!(errors, CleanupError("delete pull consumer $(psub.consumer)", err))
        end
    end
    _throw_errors(errors)
    nothing
end

function close(psub::PushSubscription)
    errors = Any[]
    try
        close(psub.sub)
    catch err
        push!(errors, err)
    end
    if psub.delete_on_close
        try
            consumer_delete(psub.js, psub.stream, psub.consumer)
        catch err
            push!(errors, CleanupError("delete push consumer $(psub.consumer)", err))
        end
    end
    _throw_errors(errors)
    nothing
end

function metadata(msg::Msg)
    reply = msg.reply
    isnothing(reply) && throw(JetStreamError(400, nothing, "message is not a JetStream message"))
    parts = split(reply, ".")
    if length(parts) == 9 && parts[1] == "\$JS" && parts[2] == "ACK"
        return MsgMetadata(parts[3], parts[4], parse(Int, parts[5]), parse(Int, parts[6]), parse(Int, parts[7]), parse(Int, parts[8]), parse(Int, parts[9]), nothing)
    elseif length(parts) >= 11 && parts[1] == "\$JS" && parts[2] == "ACK"
        domain = parts[3] == "_" ? "" : parts[3]
        return MsgMetadata(parts[5], parts[6], parse(Int, parts[7]), parse(Int, parts[8]), parse(Int, parts[9]), parse(Int, parts[10]), parse(Int, parts[11]), domain)
    end
    throw(JetStreamError(400, nothing, "message is not a JetStream message"))
end

function _ack_payload(kind::Symbol; delay::Union{Nothing,Real}=nothing)
    if kind == :ack
        UInt8[]
    elseif kind == :nak
        if isnothing(delay)
            Vector{UInt8}(codeunits("-NAK"))
        else
            args = JSON3.write(Dict("delay" => round(Int, delay * 1_000_000_000)))
            Vector{UInt8}(codeunits("-NAK $args"))
        end
    elseif kind == :progress
        Vector{UInt8}(codeunits("+WPI"))
    elseif kind == :term
        Vector{UInt8}(codeunits("+TERM"))
    elseif kind == :next
        Vector{UInt8}(codeunits("+NXT"))
    else
        throw(ArgumentError("unknown ack kind $kind"))
    end
end

function _ack(msg::Msg, kind::Symbol; delay=nothing, sync::Bool=false, timeout::Real=1.0)
    isnothing(msg.reply) && throw(JetStreamError(400, nothing, "message has no ack reply subject"))
    kind != :progress && msg.acked && throw(JetStreamError(400, nothing, "message already acknowledged"))
    payload = _ack_payload(kind; delay)
    if sync
        response = request(msg.client, msg.reply, payload; timeout)
        msg.acked = true
        response
    else
        publish(msg.client, msg.reply, payload)
        kind == :progress || (msg.acked = true)
        nothing
    end
end

ack(msg::Msg) = _ack(msg, :ack)
ack_sync(msg::Msg; timeout::Real=1.0) = _ack(msg, :ack; sync=true, timeout)
nak(msg::Msg; delay=nothing) = _ack(msg, :nak; delay)
in_progress(msg::Msg) = _ack(msg, :progress)
term(msg::Msg) = _ack(msg, :term)
