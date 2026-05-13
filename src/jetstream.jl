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

const _JS_STATUS_CONTROL = 100
const _JS_STATUS_BAD_REQUEST = 400
const _JS_STATUS_NO_MESSAGES = 404
const _JS_STATUS_TIMEOUT = 408
const _JS_STATUS_CONFLICT = 409
const _JS_STATUS_PIN_ID_MISMATCH = 423
const _JS_STATUS_NO_RESPONDERS = 503

const _JS_DESC_IDLE_HEARTBEAT = "idle heartbeat"
const _JS_DESC_FLOW_CONTROL = "flowcontrol request"
const _JS_DESC_CONSUMER_DELETED = "consumer deleted"
const _JS_DESC_LEADERSHIP_CHANGE = "leadership change"
const _JS_DESC_MAX_BYTES_EXCEEDED = "message size exceeds maxbytes"
const _JS_DESC_BATCH_COMPLETED = "batch completed"
const _JS_DESC_SERVER_SHUTDOWN = "server shutdown"

function _jetstream_control_status(msg::Msg)
    isempty(msg.data) || return nothing
    code = _status_header(msg)
    isnothing(code) && return nothing
    code, _status_description(msg)
end

function _jetstream_default_status_description(code::Int)::String
    code == _JS_STATUS_CONTROL && return "control message"
    code == _JS_STATUS_BAD_REQUEST && return "bad request"
    code == _JS_STATUS_NO_MESSAGES && return "no messages"
    code == _JS_STATUS_TIMEOUT && return "timeout"
    code == _JS_STATUS_CONFLICT && return "conflict"
    code == _JS_STATUS_PIN_ID_MISMATCH && return "pin id mismatch"
    code == _JS_STATUS_NO_RESPONDERS && return "no responders"
    "JetStream status $code"
end

function _jetstream_status_action(msg::Msg; request_subject::Union{String,Nothing}=nothing)
    status = _jetstream_control_status(msg)
    isnothing(status) && return :message, nothing
    code, description = status
    desc = isempty(description) ? _jetstream_default_status_description(code) : description
    lower = lowercase(strip(desc))
    subject = isnothing(request_subject) ? msg.subject : request_subject

    if code == _JS_STATUS_CONTROL
        lower == _JS_DESC_IDLE_HEARTBEAT && return :idle_heartbeat, nothing
        lower == _JS_DESC_FLOW_CONTROL && return :flow_control, nothing
        return :control, nothing
    elseif code == _JS_STATUS_NO_MESSAGES
        return :no_messages, nothing
    elseif code == _JS_STATUS_TIMEOUT
        return :timeout, nothing
    elseif code == _JS_STATUS_NO_RESPONDERS
        return :no_responders, NoRespondersError(subject)
    elseif code == _JS_STATUS_PIN_ID_MISMATCH
        return :pin_id_mismatch, JetStreamError(code, nothing, desc)
    elseif code == _JS_STATUS_CONFLICT
        if occursin(_JS_DESC_MAX_BYTES_EXCEEDED, lower) || occursin("maxbytes", lower) || occursin("exceeded", lower)
            return :max_bytes_exceeded, JetStreamError(code, nothing, desc)
        elseif occursin(_JS_DESC_BATCH_COMPLETED, lower)
            return :batch_completed, nothing
        elseif occursin(_JS_DESC_CONSUMER_DELETED, lower)
            return :consumer_deleted, JetStreamError(code, nothing, desc)
        elseif occursin(_JS_DESC_LEADERSHIP_CHANGE, lower)
            return :leadership_change, JetStreamError(code, nothing, desc)
        elseif occursin(_JS_DESC_SERVER_SHUTDOWN, lower)
            return :server_shutdown, JetStreamError(code, nothing, desc)
        else
            return :error, JetStreamError(code, nothing, desc)
        end
    elseif code >= 400
        return :error, JetStreamError(code, nothing, desc)
    end

    :control, nothing
end

_jetstream_heartbeat_error() =
    JetStreamError(_JS_STATUS_TIMEOUT, nothing, "JetStream idle heartbeat timed out")

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
                    headers=nothing)
    hdrs = isnothing(headers) ? Headers() : _headers_copy(headers)
    isnothing(stream) || push!(get!(hdrs, "Nats-Expected-Stream", String[]), stream)
    msg = request(js.client, subject, data; timeout, headers=hdrs)
    _puback(_js_decode(msg))
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

function _parse_rfc3339_datetime(value::AbstractString)::DateTime
    m = match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?Z$", String(value))
    isnothing(m) && throw(ProtocolError("timestamp is not UTC RFC3339: $value"))
    fraction = isnothing(m.captures[2]) ? "000" : rpad(first(m.captures[2], min(3, ncodeunits(m.captures[2]))), 3, '0')
    DateTime("$(m.captures[1]).$fraction", dateformat"yyyy-mm-ddTHH:MM:SS.sss")
end

function _direct_metadata_header(response::Msg, name::AbstractString)::String
    value = header(response, name)
    isnothing(value) && throw(ProtocolError("direct get response missing $name header"))
    value
end

function _parse_direct_sequence(response::Msg)::Int
    value = _direct_metadata_header(response, "Nats-Sequence")
    try
        return parse(Int, value)
    catch err
        err isa ArgumentError || rethrow()
        throw(ProtocolError("direct get response has invalid Nats-Sequence header: $value"))
    end
end

function _direct_message_response_info(js::JetStreamContext, request_subject::String, response::Msg)
    code = _status_header(response)
    if code == 503
        throw(NoRespondersError(request_subject))
    elseif !isnothing(code) && code >= 400
        description = _status_description(response)
        isempty(description) && (description = "direct get failed")
        throw(JetStreamError(code, nothing, description))
    end

    subject = _direct_metadata_header(response, "Nats-Subject")
    sequence = _parse_direct_sequence(response)
    created = _parse_rfc3339_datetime(_direct_metadata_header(response, "Nats-Time-Stamp"))
    headers = _headers_copy(response.headers)
    for name in _DIRECT_GET_METADATA_HEADERS
        _delete_header!(headers, name)
    end
    Msg(subject, nothing, copy(response.data); headers, client=js.client), sequence, created
end

function _direct_message_response(js::JetStreamContext, request_subject::String, response::Msg)::Msg
    msg, _sequence, _created = _direct_message_response_info(js, request_subject, response)
    msg
end

function _stream_message_get_direct_info(js::JetStreamContext, stream::String, req::Dict{String,Any}; timeout::Real)
    request_subject =
        if haskey(req, "last_by_subj") && !haskey(req, "seq")
            "$(js.prefix).DIRECT.GET.$stream.$(req["last_by_subj"])"
        else
            "$(js.prefix).DIRECT.GET.$stream"
        end
    payload = haskey(req, "last_by_subj") && !haskey(req, "seq") ? "" : JSON3.write(req)
    response = _request_raw(js.client, request_subject, payload; timeout)
    _direct_message_response_info(js, request_subject, response)
end

function _stream_message_get_direct(js::JetStreamContext, stream::String, req::Dict{String,Any}; timeout::Real)
    msg, _sequence, _created = _stream_message_get_direct_info(js, stream, req; timeout)
    msg
end

function _stream_message_from_api_payload(js::JetStreamContext, raw_msg)
    msg = _string_key_dict(raw_msg)
    data = haskey(msg, "data") ? base64decode(String(msg["data"])) : UInt8[]
    hdrs = haskey(msg, "hdrs") ? _parse_headers(base64decode(String(msg["hdrs"]))) : Headers()
    created = haskey(msg, "time") ? _parse_rfc3339_datetime(String(msg["time"])) : nothing
    Msg(String(msg["subject"]), nothing, data; headers=hdrs, client=js.client), Int(msg["seq"]), created
end

function _stream_message_get_api(js::JetStreamContext, stream::AbstractString, req::Dict{String,Any}; timeout::Real)
    obj = _api_request(js, "$(js.prefix).STREAM.MSG.GET.$stream", JSON3.write(req); timeout)
    _stream_message_from_api_payload(js, obj["message"])
end

function _stream_message_get_info(js::JetStreamContext, stream::AbstractString, req::Dict{String,Any}; direct::Bool, timeout::Real)
    direct && return _stream_message_get_direct_info(js, stream, req; timeout)
    _stream_message_get_api(js, stream, req; timeout)
end

function stream_message_get(js::JetStreamContext, stream::AbstractString; seq::Union{Int,Nothing}=nothing, subject::Union{String,Nothing}=nothing,
                            direct::Bool=false, next_by_subject::Bool=false, timeout::Real=js.timeout)
    stream = _validate_api_name("stream", stream)
    req = _stream_message_get_request(seq, subject, next_by_subject)
    direct && return _stream_message_get_direct(js, stream, req; timeout)
    msg, _seq, _created = _stream_message_get_api(js, stream, req; timeout)
    msg
end

stream_message_delete(js::JetStreamContext, stream::AbstractString, seq::Int; timeout::Real=js.timeout) =
    Bool(_api_request(js, "$(js.prefix).STREAM.MSG.DELETE.$(_validate_api_name("stream", stream))", JSON3.write(Dict("seq" => seq)); timeout)["success"])

function _server_version_at_least(client::Client, major::Int, minor::Int)
    version = @lock client.lock client.info.version
    isnothing(version) && return true
    m = match(r"^(\d+)\.(\d+)", version)
    isnothing(m) && return true
    server_major = parse(Int, m.captures[1])
    server_minor = parse(Int, m.captures[2])
    server_major > major || (server_major == major && server_minor >= minor)
end

_server_supports_consumer_name(client::Client) = _server_version_at_least(client, 2, 9)
_server_supports_consumer_action(client::Client) = _server_version_at_least(client, 2, 10)

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

function _consumer_request_payload(js::JetStreamContext, stream::AbstractString, config::Dict{String,Any}, action::Union{String,Nothing}=nothing)
    request = Dict{String,Any}("stream_name" => stream, "config" => config)
    if !isnothing(action)
        action in ("create", "update") || throw(ArgumentError("invalid consumer action: $action"))
        _server_supports_consumer_action(js.client) || throw(UnsupportedFeatureError("strict consumer $action requires nats-server 2.10+"))
        request["action"] = action
    end
    request
end

function _consumer_create_request(js::JetStreamContext, stream::AbstractString, config; timeout::Real=js.timeout,
                                  action::Union{String,Nothing}=nothing)
    stream = _validate_api_name("stream", stream)
    payload = _js_config_payload(config)
    _consumer_create_payload_request(js, stream, payload; timeout, action)
end

function _consumer_create_payload_request(js::JetStreamContext, stream::AbstractString, payload::Dict{String,Any};
                                          timeout::Real=js.timeout, action::Union{String,Nothing}=nothing)
    stream = _validate_api_name("stream", stream)
    subject = _consumer_create_subject(js, stream, payload)
    _consumer_info(_api_request(js, subject, JSON3.write(_consumer_request_payload(js, stream, payload, action)); timeout))
end

consumer_create(js::JetStreamContext, stream::AbstractString, config::ConsumerConfig; timeout::Real=js.timeout) =
    _consumer_create_request(js, stream, config; timeout, action="create")

consumer_create(js::JetStreamContext, stream::AbstractString, config::AbstractDict{String,<:Any}; timeout::Real=js.timeout) =
    _consumer_create_request(js, stream, config; timeout, action="create")

consumer_create_or_update(js::JetStreamContext, stream::AbstractString, config::ConsumerConfig; timeout::Real=js.timeout) =
    _consumer_create_request(js, stream, config; timeout)

consumer_create_or_update(js::JetStreamContext, stream::AbstractString, config::AbstractDict{String,<:Any}; timeout::Real=js.timeout) =
    _consumer_create_request(js, stream, config; timeout)

consumer_update(js::JetStreamContext, stream::AbstractString, config::ConsumerConfig; timeout::Real=js.timeout) =
    _consumer_create_request(js, stream, config; timeout, action="update")

consumer_update(js::JetStreamContext, stream::AbstractString, config::AbstractDict{String,<:Any}; timeout::Real=js.timeout) =
    _consumer_create_request(js, stream, config; timeout, action="update")

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

_consumer_missing(err) = err isa JetStreamError && err.code == 404

function _consumer_info_or_nothing(js::JetStreamContext, stream::AbstractString, consumer::AbstractString; timeout::Real=js.timeout)
    try
        consumer_info(js, stream, consumer; timeout)
    catch err
        _consumer_missing(err) && return nothing
        rethrow()
    end
end

function _consumer_normalized_config_value(value)
    if isnothing(value)
        return nothing
    elseif value isa AbstractString
        return String(value)
    elseif value isa AbstractDict
        return Dict{String,Any}(String(k) => _consumer_normalized_config_value(v) for (k, v) in pairs(value))
    elseif value isa JSON3.Object
        return Dict{String,Any}(String(k) => _consumer_normalized_config_value(v) for (k, v) in pairs(value))
    elseif value isa AbstractVector
        return Any[_consumer_normalized_config_value(v) for v in value]
    elseif value isa JSON3.Array
        return Any[_consumer_normalized_config_value(v) for v in value]
    else
        return value
    end
end

function _consumer_config_field(info::ConsumerInfo, config::Dict{String,Any}, field::String)
    if haskey(config, field)
        return config[field]
    elseif field == "name"
        return info.name
    elseif field == "durable_name" && info.config.durable_name == info.name
        return info.name
    else
        return nothing
    end
end

function _validate_bound_consumer_config(info::ConsumerInfo, expected::Dict{String,Any}, fields)
    current = _string_key_dict(info.raw["config"])
    for field in fields
        expected_value = _consumer_normalized_config_value(get(expected, field, nothing))
        actual_value = _consumer_normalized_config_value(_consumer_config_field(info, current, field))
        expected_value == actual_value && continue
        throw(ArgumentError("existing consumer $(info.name) config field $field does not match requested value"))
    end
    info
end

function _set_config_default!(config::Dict{String,Any}, field::String, value)
    if !haskey(config, field) || isnothing(config[field])
        config[field] = value
    end
    config[field]
end

function _validate_push_consumer_control_config!(config::Dict{String,Any})
    get(config, "flow_control", false) == true || return config
    idle_heartbeat = get(config, "idle_heartbeat", nothing)
    if !(idle_heartbeat isa Real) || idle_heartbeat isa Bool || idle_heartbeat <= 0
        throw(ArgumentError("flow_control=true requires idle_heartbeat to be set to a positive value"))
    end
    config
end

function _push_config_deliver_group!(config::Dict{String,Any})
    haskey(config, "deliver_group") || return nothing
    value = config["deliver_group"]
    isnothing(value) && return nothing
    value isa AbstractString || throw(ArgumentError("deliver_group must be a string"))
    group = _validate_queue(String(value))
    config["deliver_group"] = group
    group
end

function _resolve_push_queue!(config::Dict{String,Any}, queue::Union{String,Nothing})
    local_queue = _validate_queue(queue)
    config_queue = _push_config_deliver_group!(config)
    if isnothing(local_queue)
        return config_queue
    elseif isnothing(config_queue)
        config["deliver_group"] = local_queue
        return local_queue
    elseif local_queue == config_queue
        return local_queue
    else
        throw(ArgumentError("queue $local_queue does not match deliver_group $config_queue"))
    end
end

_push_config_has_idle_heartbeat(config::Dict{String,Any}) =
    haskey(config, "idle_heartbeat") && !isnothing(config["idle_heartbeat"])

_push_config_has_flow_control(config::Dict{String,Any}) =
    get(config, "flow_control", false) == true

function _validate_push_queue_control_config!(config::Dict{String,Any}, queue::Union{String,Nothing})
    isnothing(queue) && return config
    _push_config_has_flow_control(config) &&
        throw(ArgumentError("queue push subscriptions do not support flow_control"))
    _push_config_has_idle_heartbeat(config) &&
        throw(ArgumentError("queue push subscriptions do not support idle_heartbeat"))
    config
end

function _validate_existing_push_queue_control(info::ConsumerInfo, queue::Union{String,Nothing})
    isnothing(queue) && return info
    info.config.flow_control == true &&
        throw(ArgumentError("existing queue push consumer $(info.name) uses flow_control"))
    !isnothing(info.config.idle_heartbeat) &&
        throw(ArgumentError("existing queue push consumer $(info.name) uses idle_heartbeat"))
    info
end

_consumer_has_filter(config::Dict{String,Any}) =
    (haskey(config, "filter_subject") && !isnothing(config["filter_subject"])) ||
    (haskey(config, "filter_subjects") && !isnothing(config["filter_subjects"]))

function _consumer_bind_name(config::Dict{String,Any})
    if haskey(config, "name") && !isnothing(config["name"])
        return _validate_api_name("consumer", config["name"])
    elseif haskey(config, "durable_name") && !isnothing(config["durable_name"])
        return _validate_api_name("consumer", config["durable_name"])
    else
        return nothing
    end
end

function _default_push_queue_consumer!(config::Dict{String,Any}, bind_fields::Set{String}, queue::Union{String,Nothing})
    isnothing(queue) && return config
    !isnothing(_consumer_bind_name(config)) && return config

    consumer = _validate_api_name("consumer", queue)
    _set_config_default!(config, "name", consumer)
    _set_config_default!(config, "durable_name", consumer)
    push!(bind_fields, "durable_name")
    config
end

function _bind_or_create_consumer(js::JetStreamContext, stream::AbstractString, name::AbstractString,
                                  config::Dict{String,Any}, bind_fields; timeout::Real=js.timeout)
    existing = _consumer_info_or_nothing(js, stream, name; timeout)
    if !isnothing(existing)
        return _validate_bound_consumer_config(existing, config, bind_fields), false
    end
    _consumer_create_payload_request(js, stream, config; timeout, action="create"), true
end

mutable struct PullSubscription
    js::JetStreamContext
    sub::Subscription
    stream::String
    consumer::String
    deliver::String
    fetch_lock::ReentrantLock
    close_lock::ReentrantLock
    delete_on_close::Bool
    closed::Bool
    server_deleted::Bool
    pin_id::Union{String,Nothing}
end

PullSubscription(js::JetStreamContext, sub::Subscription, stream::AbstractString, consumer::AbstractString,
                 deliver::AbstractString, fetch_lock::ReentrantLock, close_lock::ReentrantLock,
                 delete_on_close::Bool, closed::Bool) =
    PullSubscription(js, sub, String(stream), String(consumer), String(deliver), fetch_lock, close_lock,
                     delete_on_close, closed, false, nothing)

mutable struct PushSubscription
    js::JetStreamContext
    sub::Subscription
    stream::String
    consumer::String
    close_lock::ReentrantLock
    delete_on_close::Bool
    closed::Bool
    heartbeat_task::Union{Task,Nothing}
    control_handler::Union{_JetStreamPushControlHandler,Nothing}
end

PushSubscription(js::JetStreamContext, sub::Subscription, stream::AbstractString, consumer::AbstractString,
                 close_lock::ReentrantLock, delete_on_close::Bool, closed::Bool) =
    PushSubscription(js, sub, String(stream), String(consumer), close_lock, delete_on_close, closed, nothing, nothing)

function _touch_push_control_handler!(handler::_JetStreamPushControlHandler)
    handler.idle_heartbeat > 0 || return nothing
    @lock handler.lock handler.last_seen = time()
    nothing
end

function _close_subscription_from_control!(sub::Subscription)
    already_closed = @lock sub.client.lock begin
        was_closed = sub.closed
        if !was_closed
            delete!(sub.client.subscriptions, sub.sid)
            sub.closed = true
        end
        was_closed
    end
    already_closed && return nothing
    errors = Any[]
    _close_subscription_channel!(errors, sub)
    _report_cleanup_errors(sub.client, errors)
    nothing
end

function _record_subscription_data_received!(handler::_JetStreamPushControlHandler)
    @lock handler.lock handler.flow_incoming += one(UInt64)
    nothing
end

function _update_flow_delivered_locked!(handler::_JetStreamPushControlHandler, queued::UInt64)
    delivered = handler.flow_incoming > queued ? handler.flow_incoming - queued : UInt64(0)
    delivered > handler.flow_delivered && (handler.flow_delivered = delivered)
    handler.flow_delivered
end

function _maybe_reply_to_subscription_flow_control!(sub::Subscription, handler::_JetStreamPushControlHandler)
    queued = @lock sub.client.lock UInt64(Base.n_avail(sub.messages))
    reply = @lock handler.lock begin
        _update_flow_delivered_locked!(handler, queued)
        if !isnothing(handler.flow_reply) && handler.flow_delivered >= handler.flow_target
            reply = handler.flow_reply
            handler.flow_reply = nothing
            handler.flow_target = UInt64(0)
            reply
        else
            nothing
        end
    end
    isnothing(reply) || _publish_flow_control_reply(sub, reply)
    nothing
end

function _handle_subscription_control(handler::_JetStreamPushControlHandler, sub::Subscription, msg::Msg)::Bool
    _touch_push_control_handler!(handler)
    action, err = _jetstream_status_action(msg)
    action == :message && return false
    if action == :flow_control
        _schedule_or_reply_to_flow_control(sub, handler, msg)
    elseif !isnothing(err)
        _report_error(sub.client, err)
    end
    if action == :consumer_deleted
        @lock handler.lock handler.consumer_deleted = true
        _close_subscription_from_control!(sub)
    end
    true
end

function _push_heartbeat_monitor(psub::PushSubscription, handler::_JetStreamPushControlHandler)
    interval = handler.idle_heartbeat
    interval > 0 || return nothing
    poll = min(0.25, max(0.01, interval / 2))
    while true
        sleep(poll)
        closed = (@lock psub.close_lock psub.closed) ||
                 (@lock psub.sub.client.lock psub.sub.closed || psub.sub.client.status in (ConnectionStatus.CLOSED, ConnectionStatus.DISCONNECTED))
        closed && return nothing
        st = status(psub.js.client)
        if st in (ConnectionStatus.CONNECTING, ConnectionStatus.RECONNECTING)
            _touch_push_control_handler!(handler)
            continue
        end
        _maybe_reply_to_subscription_flow_control!(psub.sub, handler)
        missed = @lock handler.lock time() - handler.last_seen > 2 * interval
        if missed
            _report_error(psub.js.client, _jetstream_heartbeat_error())
            _touch_push_control_handler!(handler)
        end
    end
end

function _start_push_heartbeat_monitor(psub::PushSubscription, handler::_JetStreamPushControlHandler)
    handler.idle_heartbeat > 0 || return nothing
    @async _push_heartbeat_monitor(psub, handler)
end

function _push_idle_heartbeat_seconds(info::ConsumerInfo)::Float64
    heartbeat = info.config.idle_heartbeat
    isnothing(heartbeat) ? 0.0 : Float64(heartbeat)
end

function _publish_flow_control_reply(sub::Subscription, reply::String)
    try
        publish(sub.client, reply, EMPTY_BYTES)
    catch err
        _report_error(sub.client, err)
    end
    nothing
end

function _schedule_or_reply_to_flow_control(sub::Subscription, handler::_JetStreamPushControlHandler, msg::Msg)
    if isnothing(msg.reply)
        _report_error(sub.client, JetStreamError(_JS_STATUS_CONTROL, nothing, "flow control request missing reply subject"))
        return nothing
    end

    reply = msg.reply
    queued = @lock sub.client.lock UInt64(Base.n_avail(sub.messages))
    send_now = @lock handler.lock begin
        _update_flow_delivered_locked!(handler, queued)
        if handler.flow_delivered >= handler.flow_incoming
            true
        else
            handler.flow_reply = reply
            handler.flow_target = handler.flow_incoming
            false
        end
    end
    send_now && _publish_flow_control_reply(sub, reply)
    nothing
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
    bind_fields = Set{String}(keys(cfg))
    if !_consumer_has_filter(cfg)
        cfg["filter_subject"] = String(subject)
        push!(bind_fields, "filter_subject")
    end
    if !isnothing(durable)
        _set_config_default!(cfg, "name", durable)
        _set_config_default!(cfg, "durable_name", durable)
        push!(bind_fields, "durable_name")
    end
    bind_name = _consumer_bind_name(cfg)
    delete_on_close = isnothing(bind_name)
    info =
        if isnothing(bind_name)
            _set_config_default!(cfg, "name", @lock js.client.lock randstring(js.client.rng, 16))
            _consumer_create_payload_request(js, stream, cfg; action="create")
        else
            consumer, _created = _bind_or_create_consumer(js, stream, bind_name, cfg, bind_fields)
            isnothing(consumer.config.deliver_subject) ||
                throw(ArgumentError("existing consumer $(consumer.name) is configured for push delivery"))
            consumer
        end
    if !haskey(cfg, "name") || isnothing(cfg["name"])
        cfg["name"] = info.name
    end
    try
        deliver = new_inbox(js.client)
        sub = subscribe(js.client, deliver)
        PullSubscription(js, sub, stream, info.name, deliver, ReentrantLock(), ReentrantLock(), delete_on_close, false)
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

function _pull_fetch_heartbeat(expires::Real, heartbeat::Union{Nothing,Real})::Float64
    hb =
        if isnothing(heartbeat)
            expires >= 10 ? 5.0 : 0.0
        else
            heartbeat isa Bool && throw(ArgumentError("fetch heartbeat must be a non-negative number of seconds"))
            Float64(heartbeat)
        end
    hb >= 0 || throw(ArgumentError("fetch heartbeat must be a non-negative number of seconds"))
    hb == 0 && return 0.0
    expires >= 2 * hb || throw(ArgumentError("fetch expires must be at least twice the heartbeat"))
    hb
end

function _validate_pull_fetch(psub::PullSubscription, batch::Int, timeout::Real, expires::Real,
                              heartbeat::Union{Nothing,Real})
    batch > 0 || throw(ArgumentError("fetch batch must be greater than zero"))
    timeout > 0 || throw(ArgumentError("fetch timeout must be greater than zero"))
    expires > 0 || throw(ArgumentError("fetch expires must be greater than zero"))
    timeout >= expires || throw(ArgumentError("fetch timeout must be greater than or equal to expires"))
    (@lock psub.close_lock psub.closed) && throw(ConnectionClosedError("pull subscription is closed"))
    (@lock psub.sub.client.lock psub.sub.closed) && throw(ConnectionClosedError("subscription is closed"))
    _pull_fetch_heartbeat(expires, heartbeat)
end

function _throw_pull_fetch_wait_interrupted(closed::Bool, st::ConnectionStatus.T)
    st in (ConnectionStatus.RECONNECTING, ConnectionStatus.DISCONNECTED, ConnectionStatus.CONNECTING) &&
        throw(FetchDisconnectedError())
    st == ConnectionStatus.DRAINING && throw(ConnectionDrainingError())
    st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
    closed && throw(ConnectionClosedError("subscription is closed"))
    throw(TimeoutError("next message timed out"))
end

function _next_pull_fetch_msg(psub::PullSubscription, timeout::Real)
    sub = psub.sub
    client = sub.client
    closed, st = @lock client.lock (sub.closed, client.status)
    closed && !isready(sub.messages) && _throw_pull_fetch_wait_interrupted(closed, st)
    result = timedwait(timeout; pollint=0.001) do
        isready(sub.messages) || (@lock client.lock sub.closed || client.status != ConnectionStatus.CONNECTED)
    end
    result == :timed_out && throw(TimeoutError("next message timed out"))
    isready(sub.messages) && return _take_subscription_msg!(sub)
    closed, st = @lock client.lock (sub.closed, client.status)
    _throw_pull_fetch_wait_interrupted(closed, st)
end

function _publish_pull_fetch_request(psub::PullSubscription, request_subject::String, payload::AbstractString)
    try
        _publish(psub.js.client, request_subject, payload; reply=psub.deliver, buffer_on_reconnect=false)
    catch err
        if err isa ConnectionReconnectingError ||
           (err isa ConnectionClosedError && status(psub.js.client) == ConnectionStatus.DISCONNECTED)
            throw(FetchDisconnectedError())
        end
        rethrow()
    end
    nothing
end

function fetch(psub::PullSubscription, batch::Int=1; timeout::Real=psub.js.timeout, expires::Real=timeout,
               heartbeat::Union{Nothing,Real}=nothing)
    heartbeat_seconds = _validate_pull_fetch(psub, batch, timeout, expires, heartbeat)
    @lock psub.fetch_lock begin
        req = Dict{String,Any}("batch" => batch, "expires" => round(Int, expires * 1_000_000_000))
        heartbeat_seconds > 0 && (req["idle_heartbeat"] = round(Int, heartbeat_seconds * 1_000_000_000))
        !isnothing(psub.pin_id) && (req["pin_id"] = psub.pin_id)
        request_subject = "$(psub.js.prefix).CONSUMER.MSG.NEXT.$(psub.stream).$(psub.consumer)"
        _publish_pull_fetch_request(psub, request_subject, JSON3.write(req))
        msgs = Msg[]
        deadline = time() + timeout
        heartbeat_deadline = heartbeat_seconds > 0 ? time() + 2 * heartbeat_seconds : Inf
        while length(msgs) < batch && time() < deadline
            wait_deadline = min(deadline, heartbeat_deadline)
            remaining = max(0.001, wait_deadline - time())
            try
                msg = _next_pull_fetch_msg(psub, remaining)
                heartbeat_seconds > 0 && (heartbeat_deadline = time() + 2 * heartbeat_seconds)
                pin_id = header(msg, "Nats-Pin-Id")
                !isnothing(pin_id) && !isempty(pin_id) && (psub.pin_id = pin_id)
                action, err = _jetstream_status_action(msg; request_subject)
                if action in (:idle_heartbeat, :flow_control, :control)
                    continue
                elseif action in (:no_messages, :timeout, :batch_completed)
                    break
                elseif action == :message
                    push!(msgs, msg)
                else
                    action == :consumer_deleted && (@lock psub.close_lock psub.server_deleted = true)
                    action == :pin_id_mismatch && (psub.pin_id = nothing)
                    throw(err)
                end
            catch err
                if err isa TimeoutError
                    if heartbeat_seconds > 0 && time() < deadline && time() >= heartbeat_deadline
                        throw(_jetstream_heartbeat_error())
                    end
                    break
                elseif err isa FetchDisconnectedError
                    isempty(msgs) || break
                end
                rethrow()
            end
        end
        msgs
    end
end

function push_subscribe(js::JetStreamContext, subject::AbstractString; stream::Union{String,Nothing}=nothing, durable::Union{String,Nothing}=nothing,
                        queue::Union{String,Nothing}=nothing, callback::Union{Function,Nothing}=nothing, manual_ack::Bool=false,
                        config::Union{ConsumerConfig,AbstractDict{String,<:Any}}=ConsumerConfig())
    cfg = _js_config_payload(config)
    bind_fields = Set{String}(keys(cfg))
    local_queue = _resolve_push_queue!(cfg, queue)
    !isnothing(queue) && push!(bind_fields, "deliver_group")
    _validate_push_queue_control_config!(cfg, local_queue)
    _validate_push_consumer_control_config!(cfg)

    stream = isnothing(stream) ? _stream_by_subject(js, subject) : stream
    deliver = new_inbox(js.client)
    if !_consumer_has_filter(cfg)
        cfg["filter_subject"] = String(subject)
        push!(bind_fields, "filter_subject")
    end
    if !isnothing(durable)
        _set_config_default!(cfg, "name", durable)
        _set_config_default!(cfg, "durable_name", durable)
        push!(bind_fields, "durable_name")
    end
    _default_push_queue_consumer!(cfg, bind_fields, local_queue)
    bind_name = _consumer_bind_name(cfg)
    delete_on_close = isnothing(bind_name)
    info =
        if isnothing(bind_name)
            _set_config_default!(cfg, "name", @lock js.client.lock randstring(js.client.rng, 16))
            _set_config_default!(cfg, "deliver_subject", deliver)
            _consumer_create_payload_request(js, stream, cfg; action="create")
        else
            existing = _consumer_info_or_nothing(js, stream, bind_name)
            if isnothing(existing)
                _set_config_default!(cfg, "deliver_subject", deliver)
                _consumer_create_payload_request(js, stream, cfg; action="create")
            else
                _validate_bound_consumer_config(existing, cfg, bind_fields)
                isnothing(existing.config.deliver_subject) &&
                    throw(ArgumentError("existing consumer $(existing.name) is configured for pull delivery"))
                cfg["deliver_subject"] = existing.config.deliver_subject
                if isnothing(local_queue) && !isnothing(existing.config.deliver_group)
                    local_queue = _validate_queue(existing.config.deliver_group)
                end
                _validate_existing_push_queue_control(existing, local_queue)
                existing
            end
        end
    if !haskey(cfg, "name") || isnothing(cfg["name"])
        cfg["name"] = info.name
    end
    try
        control_handler = _JetStreamPushControlHandler(_push_idle_heartbeat_seconds(info))
        sub = subscribe(js.client, String(cfg["deliver_subject"]); queue=local_queue, callback,
                        auto_ack=!manual_ack && !isnothing(callback),
                        _control_handler=control_handler)
        psub = PushSubscription(js, sub, String(stream), String(info.name), ReentrantLock(), delete_on_close, false,
                                nothing, control_handler)
        psub.heartbeat_task = _start_push_heartbeat_monitor(psub, control_handler)
        psub
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
    already_closed = @lock psub.close_lock begin
        was_closed = psub.closed
        psub.closed = true
        was_closed
    end
    already_closed && return nothing
    errors = Any[]
    try
        close(psub.sub)
    catch err
        push!(errors, err)
    end
    server_deleted = @lock psub.close_lock psub.server_deleted
    if psub.delete_on_close && !server_deleted
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
    already_closed = @lock psub.close_lock begin
        was_closed = psub.closed
        psub.closed = true
        was_closed
    end
    already_closed && return nothing
    errors = Any[]
    try
        close(psub.sub)
    catch err
        push!(errors, err)
    end
    _wait_task!(errors, "stop push heartbeat monitor $(psub.consumer)", psub.heartbeat_task)
    handler = psub.control_handler
    server_deleted = !isnothing(handler) && (@lock handler.lock handler.consumer_deleted)
    if psub.delete_on_close && !server_deleted
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
