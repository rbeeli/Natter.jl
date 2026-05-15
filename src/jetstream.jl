Base.@kwdef struct JetStreamContext{C<:Client}
    client::C
    prefix::String = "\$JS.API"
    timeout::Float64 = 5.0
end

const _JS_ACK_OPEN = UInt8(0)
const _JS_ACK_BUSY = UInt8(1)
const _JS_ACK_DONE = UInt8(2)

mutable struct JetStreamMsg{C<:Client} <: AbstractMsg
    subject::String
    reply::Union{String,Nothing}
    data::Vector{UInt8}
    headers::HeaderStorage
    sid::Int
    header_bytes::Int
    _client::C
    @atomic _ack_state::UInt8
end

JetStreamMsg{C}(subject::String, reply::Union{String,Nothing}, data::Vector{UInt8},
                headers::HeaderStorage, sid::Int, header_bytes::Int, client::C,
                acked::Bool) where {C<:Client} =
    JetStreamMsg{C}(subject, reply, data, headers, sid, header_bytes, client,
                    acked ? _JS_ACK_DONE : _JS_ACK_OPEN)

JetStreamMsg(msg::Msg, client::C) where {C<:Client} =
    JetStreamMsg{C}(msg.subject, msg.reply, msg.data, msg.headers, msg.sid, msg.header_bytes, client, false)

Base.String(msg::JetStreamMsg) = _bytes_to_string(msg.data)

struct PubAck
    stream::String
    seq::Int
    duplicate::Bool
    domain::Union{String,Nothing}
end

const _JS_MSG_ID_HEADER = "Nats-Msg-Id"
const _JS_EXPECTED_STREAM_HEADER = "Nats-Expected-Stream"
const _JS_EXPECTED_LAST_SEQUENCE_HEADER = "Nats-Expected-Last-Sequence"
const _JS_EXPECTED_LAST_SUBJECT_SEQUENCE_HEADER = "Nats-Expected-Last-Subject-Sequence"
const _JS_EXPECTED_LAST_SUBJECT_SEQUENCE_SUBJECT_HEADER = "Nats-Expected-Last-Subject-Sequence-Subject"
const _JS_EXPECTED_LAST_MSG_ID_HEADER = "Nats-Expected-Last-Msg-Id"
const _JS_MSG_TTL_HEADER = "Nats-TTL"
const _JS_SCHEDULE_HEADER = "Nats-Schedule"
const _JS_SCHEDULE_TARGET_HEADER = "Nats-Schedule-Target"
const _JS_SCHEDULE_SOURCE_HEADER = "Nats-Schedule-Source"
const _JS_SCHEDULE_TTL_HEADER = "Nats-Schedule-TTL"
const _JS_SCHEDULE_TIMEZONE_HEADER = "Nats-Schedule-Time-Zone"

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
    push_bound::Bool
    raw::Dict{String,Any}
end

function _consumer_info_push_bound(raw::Dict{String,Any})::Bool
    value = get(raw, "push_bound", false)
    isnothing(value) && return false
    Bool(value)
end

ConsumerInfo(stream_name::AbstractString, name::AbstractString, config::ConsumerConfig, raw::Dict{String,Any}) =
    ConsumerInfo(String(stream_name), String(name), config, _consumer_info_push_bound(raw), raw)

jetstream(client::Client; prefix::AbstractString="\$JS.API", timeout::Real=5.0) =
    JetStreamContext(client, String(prefix), _positive_timeout_seconds("timeout", timeout))

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
const _JS_ERR_CONSUMER_NAME_EXISTS = 10013
const _JS_ERR_CONSUMER_ALREADY_EXISTS = 10105

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

_json_get(obj, key::Symbol, default) = haskey(obj, key) ? obj[key] : default

function _js_error_from_object(err)
    code = Int(_json_get(err, :code, 0))
    err_code_value = _json_get(err, :err_code, nothing)
    err_code = isnothing(err_code_value) ? nothing : Int(err_code_value)
    description = String(_json_get(err, :description, ""))
    JetStreamError(code, err_code, description)
end

function _js_read_response(msg::Msg)
    isempty(msg.data) && return nothing
    obj = JSON3.read(msg.data)
    haskey(obj, :error) && throw(_js_error_from_object(obj[:error]))
    obj
end

function _js_decode(msg::Msg)
    obj = _js_read_response(msg)
    isnothing(obj) ? Dict{String,Any}() : _string_key_dict(obj)
end

function _api_request(js::JetStreamContext, subject::String, payload=nothing; timeout::Real=js.timeout)
    msg = request(js.client, subject, payload; timeout)
    _js_decode(msg)
end

function _puback(obj)
    PubAck(String(obj[:stream]), Int(obj[:seq]), Bool(_json_get(obj, :duplicate, false)),
           haskey(obj, :domain) ? String(obj[:domain]) : nothing)
end

function _js_header_nonempty(name::AbstractString, value)::String
    s = String(value)
    isempty(s) && throw(ArgumentError("$name cannot be empty"))
    s
end

function _js_header_sequence(name::AbstractString, value)::String
    value isa Integer && !(value isa Bool) ||
        throw(ArgumentError("$name must be a non-negative integer"))
    value >= 0 || throw(ArgumentError("$name must be a non-negative integer"))
    string(Int(value))
end

function _js_duration_header(name::AbstractString, value; allow_never::Bool=false, min_seconds::Real=0.0)::String
    if allow_never && value isa AbstractString && lowercase(String(value)) == "never"
        return "never"
    elseif allow_never && value === :never
        return "never"
    end
    value isa Real && !(value isa Bool) ||
        throw(ArgumentError("$name must be a positive number of seconds"))
    seconds = Float64(value)
    isfinite(seconds) && seconds > 0 ||
        throw(ArgumentError("$name must be a positive number of seconds"))
    seconds >= Float64(min_seconds) ||
        throw(ArgumentError("$name must be at least $(Float64(min_seconds)) seconds"))
    isinteger(seconds) ? "$(Int(seconds))s" : "$(seconds)s"
end

function _js_publish_schedule(schedule, schedule_at, schedule_every)
    provided = count(!isnothing, (schedule, schedule_at, schedule_every))
    provided <= 1 || throw(ArgumentError("provide at most one of schedule, schedule_at, or schedule_every"))
    if !isnothing(schedule)
        return _js_header_nonempty("schedule", schedule)
    elseif !isnothing(schedule_at)
        return "@at $(_timestamp_to_rfc3339(schedule_at))"
    elseif !isnothing(schedule_every)
        seconds = Float64(schedule_every)
        isfinite(seconds) && seconds >= 1 ||
            throw(ArgumentError("schedule_every must be at least 1 second"))
        return "@every $(_js_duration_header("schedule_every", seconds))"
    end
    nothing
end

function _push_header!(headers::Headers, name::String, value::String)
    push!(get!(headers, name, String[]), value)
    headers
end

function _js_publish_headers(headers; stream::Union{AbstractString,Nothing}=nothing,
                             expected_stream::Union{AbstractString,Nothing}=nothing,
                             msg_id=nothing, expected_last_sequence=nothing,
                             expected_last_subject_sequence=nothing,
                             expected_last_subject=nothing,
                             expected_last_msg_id=nothing, ttl=nothing,
                             schedule=nothing, schedule_at=nothing, schedule_every=nothing,
                             schedule_target=nothing, schedule_source=nothing,
                             schedule_ttl=nothing, schedule_timezone=nothing)::Headers
    hdrs = isnothing(headers) ? Headers() : _headers_copy(headers)
    if !isnothing(stream) && !isnothing(expected_stream) && String(stream) != String(expected_stream)
        throw(ArgumentError("stream and expected_stream must match when both are provided"))
    end
    expected = isnothing(expected_stream) ? stream : expected_stream

    isnothing(msg_id) || _push_header!(hdrs, _JS_MSG_ID_HEADER, _js_header_nonempty("msg_id", msg_id))
    isnothing(expected) ||
        _push_header!(hdrs, _JS_EXPECTED_STREAM_HEADER, _validate_api_name("stream", expected))
    isnothing(expected_last_sequence) ||
        _push_header!(hdrs, _JS_EXPECTED_LAST_SEQUENCE_HEADER,
                      _js_header_sequence("expected_last_sequence", expected_last_sequence))
    isnothing(expected_last_subject_sequence) ||
        _push_header!(hdrs, _JS_EXPECTED_LAST_SUBJECT_SEQUENCE_HEADER,
                      _js_header_sequence("expected_last_subject_sequence", expected_last_subject_sequence))
    if !isnothing(expected_last_subject)
        isnothing(expected_last_subject_sequence) &&
            throw(ArgumentError("expected_last_subject requires expected_last_subject_sequence"))
        _push_header!(hdrs, _JS_EXPECTED_LAST_SUBJECT_SEQUENCE_SUBJECT_HEADER,
                      _validate_publish_subject(expected_last_subject))
    end
    isnothing(expected_last_msg_id) ||
        _push_header!(hdrs, _JS_EXPECTED_LAST_MSG_ID_HEADER,
                      _js_header_nonempty("expected_last_msg_id", expected_last_msg_id))
    isnothing(ttl) || _push_header!(hdrs, _JS_MSG_TTL_HEADER, _js_duration_header("ttl", ttl; min_seconds=1.0))

    publish_schedule = _js_publish_schedule(schedule, schedule_at, schedule_every)
    isnothing(publish_schedule) || _push_header!(hdrs, _JS_SCHEDULE_HEADER, publish_schedule)
    isnothing(schedule_target) ||
        _push_header!(hdrs, _JS_SCHEDULE_TARGET_HEADER, _validate_publish_subject(schedule_target))
    isnothing(schedule_source) ||
        _push_header!(hdrs, _JS_SCHEDULE_SOURCE_HEADER, _validate_publish_subject(schedule_source))
    isnothing(schedule_ttl) ||
        _push_header!(hdrs, _JS_SCHEDULE_TTL_HEADER,
                      _js_duration_header("schedule_ttl", schedule_ttl; allow_never=true))
    isnothing(schedule_timezone) ||
        _push_header!(hdrs, _JS_SCHEDULE_TIMEZONE_HEADER,
                      _js_header_nonempty("schedule_timezone", schedule_timezone))
    hdrs
end

function _js_publish_retry_attempts(value)::Int
    value isa Integer && !(value isa Bool) ||
        throw(ArgumentError("retry_attempts must be a non-negative integer"))
    value >= 0 || throw(ArgumentError("retry_attempts must be a non-negative integer"))
    Int(value)
end

function _js_publish_retry_wait(value)::Float64
    value isa Real && !(value isa Bool) ||
        throw(ArgumentError("retry_wait must be a positive number of seconds"))
    wait = Float64(value)
    isfinite(wait) && wait > 0 ||
        throw(ArgumentError("retry_wait must be a positive number of seconds"))
    wait
end

function js_publish(js::JetStreamContext, subject::AbstractString, data=nothing; timeout::Real=js.timeout,
                    stream::Union{AbstractString,Nothing}=nothing, headers=nothing,
                    expected_stream::Union{AbstractString,Nothing}=nothing, msg_id=nothing,
                    expected_last_sequence=nothing, expected_last_subject_sequence=nothing,
                    expected_last_subject=nothing, expected_last_msg_id=nothing, ttl=nothing,
                    schedule=nothing, schedule_at=nothing, schedule_every=nothing,
                    schedule_target=nothing, schedule_source=nothing, schedule_ttl=nothing,
                    schedule_timezone=nothing, retry_attempts::Integer=0, retry_wait::Real=0.25)
    timeout = _positive_timeout_seconds("timeout", timeout)
    hdrs = _js_publish_headers(headers; stream, expected_stream, msg_id, expected_last_sequence,
                               expected_last_subject_sequence, expected_last_subject,
                               expected_last_msg_id, ttl, schedule, schedule_at, schedule_every,
                               schedule_target, schedule_source, schedule_ttl, schedule_timezone)
    attempts = _js_publish_retry_attempts(retry_attempts)
    wait_seconds = _js_publish_retry_wait(retry_wait)
    deadline = time() + timeout
    attempt = 0
    while true
        remaining = deadline - time()
        remaining > 0 || throw(TimeoutError("request timed out"))
        try
            msg = request(js.client, subject, data; timeout=remaining, headers=hdrs)
            obj = _js_read_response(msg)
            isnothing(obj) && throw(ProtocolError("JetStream publish response is empty"))
            return _puback(obj)
        catch err
            err isa NoRespondersError && attempt < attempts || rethrow()
            attempt += 1
            sleep(min(wait_seconds, max(0.0, deadline - time())))
        end
    end
end

function _stream_config_name(config::StreamConfig)::String
    isnothing(config.name) && throw(ArgumentError("stream config name is required"))
    _validate_api_name("stream", config.name)
end

function _stream_config_name(config::AbstractDict)::String
    haskey(config, "name") || throw(ArgumentError("stream config name is required"))
    _validate_api_name("stream", config["name"])
end

_stream_config_payload(config::StreamConfig) = _js_config_payload(config)

function _stream_config_payload(config::AbstractDict{String,<:Any})::Dict{String,Any}
    payload = _js_config_payload(config)
    _validate_stream_config_payload!(payload)
end

function stream_create(js::JetStreamContext, config::StreamConfig; timeout::Real=js.timeout)
    name = _stream_config_name(config)
    payload = _stream_config_payload(config)
    _stream_info(_api_request(js, "$(js.prefix).STREAM.CREATE.$name", JSON3.write(payload); timeout))
end

function stream_create(js::JetStreamContext, config::AbstractDict{String,<:Any}; timeout::Real=js.timeout)
    name = _stream_config_name(config)
    payload = _stream_config_payload(config)
    _stream_info(_api_request(js, "$(js.prefix).STREAM.CREATE.$name", JSON3.write(payload); timeout))
end

function stream_update(js::JetStreamContext, config::StreamConfig; timeout::Real=js.timeout)
    name = _stream_config_name(config)
    payload = _stream_config_payload(config)
    _stream_info(_api_request(js, "$(js.prefix).STREAM.UPDATE.$name", JSON3.write(payload); timeout))
end

function stream_update(js::JetStreamContext, config::AbstractDict{String,<:Any}; timeout::Real=js.timeout)
    name = _stream_config_name(config)
    payload = _stream_config_payload(config)
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

function stream_names(js::JetStreamContext; subject::Union{AbstractString,Nothing}=nothing, timeout::Real=js.timeout)
    subject = isnothing(subject) ? nothing : _validate_subject(subject)
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

function _validate_stream_purge_keep(keep::Union{Integer,Nothing})
    isnothing(keep) && return nothing
    keep isa Bool && throw(ArgumentError("keep must be non-negative"))
    keep >= 0 || throw(ArgumentError("keep must be non-negative"))
    Int(keep)
end

stream_purge(js::JetStreamContext, name::AbstractString; filter_subject::Union{AbstractString,Nothing}=nothing,
             keep::Union{Integer,Nothing}=nothing, timeout::Real=js.timeout) = begin
    filter = isnothing(filter_subject) ? nothing : _validate_subject(filter_subject)
    req = Dict{String,Any}()
    isnothing(filter) || (req["filter"] = filter)
    keep = _validate_stream_purge_keep(keep)
    isnothing(keep) || (req["keep"] = keep)
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

function _stream_message_get_request(seq::Union{Int,Nothing}, subject::Union{AbstractString,Nothing}, next_by_subject::Bool)::Dict{String,Any}
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
    Msg(subject, nothing, copy(response.data); headers), sequence, created
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
    Msg(String(msg["subject"]), nothing, data; headers=hdrs), Int(msg["seq"]), created
end

function _stream_message_get_api(js::JetStreamContext, stream::AbstractString, req::Dict{String,Any}; timeout::Real)
    obj = _api_request(js, "$(js.prefix).STREAM.MSG.GET.$stream", JSON3.write(req); timeout)
    _stream_message_from_api_payload(js, obj["message"])
end

function _stream_message_get_info(js::JetStreamContext, stream::AbstractString, req::Dict{String,Any}; direct::Bool, timeout::Real)
    direct && return _stream_message_get_direct_info(js, stream, req; timeout)
    _stream_message_get_api(js, stream, req; timeout)
end

function stream_message_get(js::JetStreamContext, stream::AbstractString; seq::Union{Int,Nothing}=nothing, subject::Union{AbstractString,Nothing}=nothing,
                            direct::Bool=false, next_by_subject::Bool=false, timeout::Real=js.timeout)
    stream = _validate_api_name("stream", stream)
    req = _stream_message_get_request(seq, subject, next_by_subject)
    direct && return _stream_message_get_direct(js, stream, req; timeout)
    msg, _seq, _created = _stream_message_get_api(js, stream, req; timeout)
    msg
end

function stream_message_delete(js::JetStreamContext, stream::AbstractString, seq::Int; timeout::Real=js.timeout)
    stream = _validate_api_name("stream", stream)
    seq = _validate_stream_sequence(seq)
    Bool(_api_request(js, "$(js.prefix).STREAM.MSG.DELETE.$stream", JSON3.write(Dict("seq" => seq)); timeout)["success"])
end

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
    _validate_consumer_config_payload!(payload)
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
_consumer_create_conflict(err) =
    err isa JetStreamError && err.err_code in (_JS_ERR_CONSUMER_NAME_EXISTS, _JS_ERR_CONSUMER_ALREADY_EXISTS)

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

function _validate_pull_consumer_config!(config::Dict{String,Any})
    if haskey(config, "deliver_subject") && !isnothing(config["deliver_subject"])
        throw(ArgumentError("pull subscriptions do not support deliver_subject"))
    elseif haskey(config, "deliver_group") && !isnothing(config["deliver_group"])
        throw(ArgumentError("pull subscriptions do not support deliver_group"))
    end
    config
end

function _validate_existing_pull_consumer(info::ConsumerInfo)
    if !isnothing(info.config.deliver_subject) || !isnothing(info.config.deliver_group)
        throw(ArgumentError("existing consumer $(info.name) is configured for push delivery"))
    end
    info
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

function _resolve_push_queue!(config::Dict{String,Any}, queue::Union{AbstractString,Nothing})
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

function _validate_existing_push_bind(info::ConsumerInfo, queue::Union{String,Nothing}, queue_explicit::Bool)
    deliver_group = info.config.deliver_group
    if !isnothing(deliver_group)
        queue_explicit ||
            throw(ArgumentError("existing queue push consumer $(info.name) requires explicit queue $deliver_group"))
        queue == deliver_group ||
            throw(ArgumentError("queue $queue does not match existing push consumer $(info.name) deliver_group $deliver_group"))
        return info
    end
    isnothing(queue) ||
        throw(ArgumentError("existing non-queue push consumer $(info.name) cannot be joined with queue $queue"))
    info.push_bound &&
        throw(ArgumentError("existing non-queue push consumer $(info.name) is already bound to a subscription"))
    info
end

function _bind_existing_push_consumer!(info::ConsumerInfo, config::Dict{String,Any}, bind_fields,
                                       queue::Union{String,Nothing}, queue_explicit::Bool)
    _validate_bound_consumer_config(info, config, bind_fields)
    isnothing(info.config.deliver_subject) &&
        throw(ArgumentError("existing consumer $(info.name) is configured for pull delivery"))
    config["deliver_subject"] = info.config.deliver_subject
    _validate_existing_push_bind(info, queue, queue_explicit)
    _validate_existing_push_queue_control(info, queue)
    info
end

_consumer_has_filter(config::Dict{String,Any}) =
    (haskey(config, "filter_subject") && !isnothing(config["filter_subject"])) ||
    (haskey(config, "filter_subjects") && !isnothing(config["filter_subjects"]) &&
     (!(config["filter_subjects"] isa AbstractVector) || !isempty(config["filter_subjects"])))

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
    _validate_consumer_config_payload!(config)
    existing = _consumer_info_or_nothing(js, stream, name; timeout)
    if !isnothing(existing)
        return _validate_bound_consumer_config(existing, config, bind_fields), false
    end
    try
        _consumer_create_payload_request(js, stream, config; timeout, action="create"), true
    catch err
        _consumer_create_conflict(err) || rethrow()
        _validate_bound_consumer_config(consumer_info(js, stream, name; timeout), config, bind_fields), false
    end
end

_pull_fetch_next_subject(js::JetStreamContext, stream::AbstractString, consumer::AbstractString)::String =
    string(js.prefix, ".CONSUMER.MSG.NEXT.", stream, ".", consumer)

function _pull_fetch_request_payload(batch::Int, expires_ns::Int, heartbeat_ns::Int,
                                     pin_id::Union{String,Nothing})::String
    if heartbeat_ns > 0
        if isnothing(pin_id)
            return JSON3.write((batch=batch, expires=expires_ns, idle_heartbeat=heartbeat_ns))
        else
            return JSON3.write((batch=batch, expires=expires_ns,
                                idle_heartbeat=heartbeat_ns, pin_id=pin_id))
        end
    elseif isnothing(pin_id)
        return JSON3.write((batch=batch, expires=expires_ns))
    else
        return JSON3.write((batch=batch, expires=expires_ns, pin_id=pin_id))
    end
end

mutable struct PullSubscription{C<:Client,J<:JetStreamContext{C},S<:Subscription{C}}
    js::J
    sub::S
    stream::String
    consumer::String
    next_subject::String
    deliver::String
    fetch_lock::ReentrantLock
    close_lock::ReentrantLock
    delete_on_close::Bool
    closed::Bool
    server_deleted::Bool
    pin_id::Union{String,Nothing}
end

function PullSubscription(js::J, sub::S, stream::AbstractString, consumer::AbstractString,
                          deliver::AbstractString, fetch_lock::ReentrantLock, close_lock::ReentrantLock,
                          delete_on_close::Bool, closed::Bool) where {C<:Client,J<:JetStreamContext{C},S<:Subscription{C}}
    stream = String(stream)
    consumer = String(consumer)
    PullSubscription{C,J,S}(js, sub, stream, consumer, _pull_fetch_next_subject(js, stream, consumer),
                            String(deliver), fetch_lock, close_lock,
                            delete_on_close, closed, false, nothing)
end

mutable struct PushSubscription{C<:Client,J<:JetStreamContext{C},S<:Subscription{C}}
    js::J
    sub::S
    stream::String
    consumer::String
    close_lock::ReentrantLock
    delete_on_close::Bool
    closed::Bool
    heartbeat_task::Union{Task,Nothing}
    control_handler::Union{_JetStreamPushControlHandler,Nothing}
    info::Union{ConsumerInfo,Nothing}
end

function PushSubscription(js::J, sub::S, stream::AbstractString, consumer::AbstractString,
                          close_lock::ReentrantLock, delete_on_close::Bool, closed::Bool,
                          heartbeat_task::Union{Task,Nothing},
                          control_handler::Union{_JetStreamPushControlHandler,Nothing}) where {C<:Client,J<:JetStreamContext{C},S<:Subscription{C}}
    PushSubscription{C,J,S}(js, sub, String(stream), String(consumer), close_lock, delete_on_close, closed,
                            heartbeat_task, control_handler, nothing)
end

PushSubscription(js::JetStreamContext, sub::Subscription, stream::AbstractString, consumer::AbstractString,
                 close_lock::ReentrantLock, delete_on_close::Bool, closed::Bool) =
    PushSubscription(js, sub, String(stream), String(consumer), close_lock, delete_on_close, closed, nothing, nothing)

function _touch_push_control_handler!(handler::_JetStreamPushControlHandler)
    handler.idle_heartbeat[] > 0 || return nothing
    handler.last_seen[] = time()
    nothing
end

function _close_subscription_from_control!(sub::Subscription)
    already_closed = @lock sub.lock begin
        was_closed = sub.closed
        if !was_closed
            sub.closed = true
            sub.server_active = false
            _notify_subscription_waiters_locked(sub; all=true)
        end
        was_closed
    end
    already_closed && return nothing
    @lock sub.client.lock begin
        _delete_subscription_locked!(sub.client, sub.sid, sub)
    end
    errors = Any[]
    _close_subscription_channel!(errors, sub)
    _report_cleanup_errors(sub.client, errors)
    nothing
end

function _record_subscription_data_received!(handler::_JetStreamPushControlHandler)
    handler.flow_control[] || return nothing
    Threads.atomic_add!(handler.flow_incoming, one(UInt64))
    nothing
end

function _update_flow_delivered_locked!(handler::_JetStreamPushControlHandler, queued::UInt64)
    incoming = handler.flow_incoming[]
    delivered = incoming > queued ? incoming - queued : UInt64(0)
    delivered > handler.flow_delivered && (handler.flow_delivered = delivered)
    handler.flow_delivered
end

function _maybe_reply_to_subscription_flow_control!(sub::Subscription, handler::_JetStreamPushControlHandler)
    handler.flow_control[] || return nothing
    queued = @lock sub.lock UInt64(Base.n_avail(sub.messages))
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
    isempty(msg.data) || return false
    action, err = _jetstream_status_action(msg)
    action == :message && return false
    if action == :flow_control
        _schedule_or_reply_to_flow_control(sub, handler, msg)
    elseif !isnothing(err)
        _report_error(sub.client, err)
    end
    if action == :consumer_deleted
        handler.consumer_deleted[] = true
        _close_subscription_from_control!(sub)
    end
    true
end

function _push_heartbeat_monitor(psub::PushSubscription, handler::_JetStreamPushControlHandler)
    interval = handler.idle_heartbeat[]
    interval > 0 || return nothing
    poll = min(0.25, max(0.01, interval / 2))
    while true
        sleep(poll)
        closed = (@lock psub.close_lock psub.closed) ||
                 (@lock psub.sub.lock psub.sub.closed) ||
                 status(psub.sub.client) in (ConnectionStatus.CLOSED, ConnectionStatus.DISCONNECTED)
        closed && return nothing
        st = status(psub.js.client)
        if st in (ConnectionStatus.CONNECTING, ConnectionStatus.RECONNECTING)
            _touch_push_control_handler!(handler)
            continue
        end
        _maybe_reply_to_subscription_flow_control!(psub.sub, handler)
        missed = time() - handler.last_seen[] > 2 * interval
        if missed
            _report_error(psub.js.client, _jetstream_heartbeat_error())
            _touch_push_control_handler!(handler)
        end
    end
end

function _start_push_heartbeat_monitor(psub::PushSubscription, handler::_JetStreamPushControlHandler)
    handler.idle_heartbeat[] > 0 || return nothing
    @async _push_heartbeat_monitor(psub, handler)
end

function next(psub::PushSubscription{C}; timeout::Real=1.0)::JetStreamMsg{C} where {C<:Client}
    JetStreamMsg(next(psub.sub; timeout), psub.js.client)
end

function _push_idle_heartbeat_seconds(info::ConsumerInfo)::Float64
    heartbeat = info.config.idle_heartbeat
    isnothing(heartbeat) ? 0.0 : Float64(heartbeat)
end

function _push_idle_heartbeat_seconds(config::Dict{String,Any})::Float64
    heartbeat = get(config, "idle_heartbeat", nothing)
    heartbeat isa Real && !(heartbeat isa Bool) ? Float64(heartbeat) / 1_000_000_000 : 0.0
end

_push_flow_control_enabled(info::ConsumerInfo)::Bool = info.config.flow_control == true
_push_flow_control_enabled(config::Dict{String,Any})::Bool = get(config, "flow_control", false) == true

function _push_callback_auto_ack(manual_ack::Bool, callback, info::ConsumerInfo)::Bool
    !manual_ack && !isnothing(callback) && info.config.ack_policy != AckPolicy.NONE
end

function _push_callback_auto_ack(manual_ack::Bool, callback, config::Dict{String,Any})::Bool
    !manual_ack && !isnothing(callback) && get(config, "ack_policy", nothing) != "none"
end

function _jetstream_push_callback(js::JetStreamContext{C}, callback,
                                  auto_ack::Bool) where {C<:Client}
    isnothing(callback) && return nothing
    msg -> begin
        jsmsg = JetStreamMsg(msg, js.client)
        callback(jsmsg)
        auto_ack && ack(jsmsg)
        nothing
    end
end

function _publish_flow_control_reply(sub::Subscription, reply::String)
    try
        _publish(sub.client, reply, EMPTY_BYTES; force_flush=true)
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
    if !handler.flow_control[]
        _publish_flow_control_reply(sub, reply)
        return nothing
    end
    queued = @lock sub.lock UInt64(Base.n_avail(sub.messages))
    send_now = @lock handler.lock begin
        _update_flow_delivered_locked!(handler, queued)
        if handler.flow_delivered >= handler.flow_incoming[]
            true
        else
            handler.flow_reply = reply
            handler.flow_target = handler.flow_incoming[]
            false
        end
    end
    send_now && _publish_flow_control_reply(sub, reply)
    nothing
end

function _stream_by_subject(js::JetStreamContext, subject::AbstractString)
    subject = _validate_subject(subject)
    names = stream_names(js; subject)
    isempty(names) && throw(JetStreamError(404, nothing, "no stream found for subject $subject"))
    first(names)
end

function pull_subscribe(js::JetStreamContext, subject::AbstractString; stream::Union{AbstractString,Nothing}=nothing, durable::Union{AbstractString,Nothing}=nothing,
                        config::Union{ConsumerConfig,AbstractDict{String,<:Any}}=ConsumerConfig())
    subject = _validate_subject(subject)
    cfg = _js_config_payload(config)
    _validate_pull_consumer_config!(cfg)
    _validate_consumer_config_payload!(cfg)

    stream = isnothing(stream) ? _stream_by_subject(js, subject) : _validate_api_name("stream", stream)
    durable = isnothing(durable) ? nothing : _validate_api_name("consumer", durable)
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
            _validate_existing_pull_consumer(consumer)
        end
    if !haskey(cfg, "name") || isnothing(cfg["name"])
        cfg["name"] = info.name
    end
    try
        deliver = "$(new_inbox(js.client)).*"
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

function _pull_fetch_default_expires(timeout::Real)::Float64
    ttl = Float64(timeout)
    ttl - min(ttl * 0.1, 5.0)
end

function _validate_pull_fetch(psub::PullSubscription, batch::Int, timeout::Real, expires::Real,
                              heartbeat::Union{Nothing,Real})
    batch > 0 || throw(ArgumentError("fetch batch must be greater than zero"))
    timeout = _positive_timeout_seconds("fetch timeout", timeout)
    expires = _positive_timeout_seconds("fetch expires", expires)
    timeout > expires || throw(ArgumentError("fetch timeout must be greater than expires"))
    (@lock psub.close_lock psub.closed) && throw(ConnectionClosedError("pull subscription is closed"))
    (@lock psub.sub.lock psub.sub.closed) && throw(ConnectionClosedError("subscription is closed"))
    timeout, expires, _pull_fetch_heartbeat(expires, heartbeat)
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
    deadline = time() + Float64(timeout)
    while true
        msg = _take_subscription_msg_if_ready!(sub)
        !isnothing(msg) && return msg

        closed, empty = @lock sub.lock (sub.closed, !isready(sub.messages))
        st = status(client)
        closed && empty && _throw_pull_fetch_wait_interrupted(closed, st)
        ready = @lock sub.lock begin
            _wait_until_condition_locked(sub.condition, _remaining_timeout(deadline)) do
                isready(sub.messages) || sub.closed || status(client) != ConnectionStatus.CONNECTED
            end
        end
        ready || throw(TimeoutError("next message timed out"))
        msg = _take_subscription_msg_if_ready!(sub)
        !isnothing(msg) && return msg
        closed = @lock sub.lock sub.closed
        st = status(client)
        (closed || st != ConnectionStatus.CONNECTED) && _throw_pull_fetch_wait_interrupted(closed, st)
    end
end

function _publish_pull_fetch_request(psub::PullSubscription, request_subject::String, payload::AbstractString,
                                     reply::String)
    try
        _publish(psub.js.client, request_subject, payload; reply,
                 buffer_on_reconnect=false, force_flush=true)
    catch err
        if err isa ConnectionReconnectingError ||
           (err isa ConnectionClosedError && status(psub.js.client) == ConnectionStatus.DISCONNECTED)
            throw(FetchDisconnectedError())
        end
        rethrow()
    end
    nothing
end

function _pull_fetch_reply(psub::PullSubscription)::Tuple{String,Union{String,Nothing}}
    endswith(psub.deliver, ".*") || return psub.deliver, nothing
    token = @lock psub.js.client.lock randstring(psub.js.client.rng, NUID_ALPHABET, 22)
    string(chop(psub.deliver; tail=1), token), token
end

function _pull_fetch_status_matches_request(subject::AbstractString, token::Union{String,Nothing})::Bool
    isnothing(token) && return true
    subject = String(subject)
    subject_len = ncodeunits(subject)
    token_len = ncodeunits(token)
    subject_len > token_len || return false
    codeunit(subject, subject_len - token_len) == UInt8('.') || return false
    @inbounds for i in 1:token_len
        codeunit(subject, subject_len - token_len + i) == codeunit(token, i) || return false
    end
    true
end

function fetch(psub::PullSubscription{C}, batch::Int=1; timeout::Real=psub.js.timeout,
               expires::Real=_pull_fetch_default_expires(timeout),
               heartbeat::Union{Nothing,Real}=nothing) where {C}
    timeout_seconds, expires_seconds, heartbeat_seconds =
        _validate_pull_fetch(psub, batch, timeout, expires, heartbeat)
    @lock psub.fetch_lock begin
        request_subject = psub.next_subject
        heartbeat_ns = heartbeat_seconds > 0 ? _seconds_to_nanoseconds(heartbeat_seconds) : 0
        payload = _pull_fetch_request_payload(batch, _seconds_to_nanoseconds(expires_seconds),
                                              heartbeat_ns, psub.pin_id)
        reply, reply_token = _pull_fetch_reply(psub)
        _publish_pull_fetch_request(psub, request_subject, payload, reply)
        msgs = JetStreamMsg{C}[]
        sizehint!(msgs, batch)
        deadline = time() + timeout_seconds
        heartbeat_deadline = heartbeat_seconds > 0 ? time() + 2 * heartbeat_seconds : Inf
        while length(msgs) < batch && time() < deadline
            wait_deadline = min(deadline, heartbeat_deadline)
            remaining = max(0.001, wait_deadline - time())
            try
                msg = _next_pull_fetch_msg(psub, remaining)
                action, err = _jetstream_status_action(msg; request_subject)
                action != :message && !_pull_fetch_status_matches_request(msg.subject, reply_token) && continue
                heartbeat_seconds > 0 && (heartbeat_deadline = time() + 2 * heartbeat_seconds)
                pin_id = header(msg, "Nats-Pin-Id")
                !isnothing(pin_id) && !isempty(pin_id) && (psub.pin_id = pin_id)
                if action in (:idle_heartbeat, :flow_control, :control)
                    continue
                elseif action in (:no_messages, :timeout, :batch_completed)
                    break
                elseif action == :message
                    push!(msgs, JetStreamMsg(msg, psub.js.client))
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

function push_subscribe(js::JetStreamContext, subject::AbstractString; stream::Union{AbstractString,Nothing}=nothing, durable::Union{AbstractString,Nothing}=nothing,
                        queue::Union{AbstractString,Nothing}=nothing, callback=nothing, manual_ack::Bool=false,
                        config::Union{ConsumerConfig,AbstractDict{String,<:Any}}=ConsumerConfig())
    subject = _validate_subject(subject)
    cfg = _js_config_payload(config)
    queue_explicit = !isnothing(queue)
    local_queue = _resolve_push_queue!(cfg, queue)
    _validate_push_queue_control_config!(cfg, local_queue)
    _validate_push_consumer_control_config!(cfg)
    _validate_consumer_config_payload!(cfg)
    bind_fields = Set{String}(keys(cfg))
    !isnothing(queue) && push!(bind_fields, "deliver_group")

    stream = isnothing(stream) ? _stream_by_subject(js, subject) : _validate_api_name("stream", stream)
    durable = isnothing(durable) ? nothing : _validate_api_name("consumer", durable)
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
    _validate_consumer_config_payload!(cfg)
    bind_name = _consumer_bind_name(cfg)
    delete_on_close = isnothing(bind_name)
    info::Union{ConsumerInfo,Nothing} = nothing
    create_consumer = false
    if isnothing(bind_name)
        _set_config_default!(cfg, "name", @lock js.client.lock randstring(js.client.rng, 16))
        _set_config_default!(cfg, "deliver_subject", deliver)
        create_consumer = true
    else
        existing = _consumer_info_or_nothing(js, stream, bind_name)
        if isnothing(existing)
            _set_config_default!(cfg, "deliver_subject", deliver)
            create_consumer = true
        else
            info = _bind_existing_push_consumer!(existing, cfg, bind_fields, local_queue, queue_explicit)
        end
    end
    control_handler = _JetStreamPushControlHandler(
        isnothing(info) ? _push_idle_heartbeat_seconds(cfg) : _push_idle_heartbeat_seconds(info);
        flow_control=isnothing(info) ? _push_flow_control_enabled(cfg) : _push_flow_control_enabled(info),
    )
    deliver_subject = String(cfg["deliver_subject"])
    auto_ack = isnothing(info) ? _push_callback_auto_ack(manual_ack, callback, cfg) :
               _push_callback_auto_ack(manual_ack, callback, info)
    wrapped_callback = _jetstream_push_callback(js, callback, auto_ack)
    sub = subscribe(js.client, deliver_subject; queue=local_queue, callback=wrapped_callback,
                    _control_handler=control_handler)
    consumer_created = false
    try
        if create_consumer
            try
                info = _consumer_create_payload_request(js, stream, cfg; action="create")
                consumer_created = true
            catch err
                (!isnothing(bind_name) && _consumer_create_conflict(err)) || rethrow()
                try
                    close(sub)
                catch cleanup_err
                    throw(Base.CompositeException([err, CleanupError("close provisional push subscription $deliver_subject", cleanup_err)]))
                end
                existing = consumer_info(js, stream, bind_name)
                info = _bind_existing_push_consumer!(existing, cfg, bind_fields, local_queue, queue_explicit)
                deliver_subject = String(cfg["deliver_subject"])
                retry_auto_ack = _push_callback_auto_ack(manual_ack, callback, info)
                wrapped_callback = _jetstream_push_callback(js, callback, retry_auto_ack)
                sub = subscribe(js.client, deliver_subject; queue=local_queue, callback=wrapped_callback,
                                _control_handler=control_handler)
            end
        end
        info = info::ConsumerInfo
        if !haskey(cfg, "name") || isnothing(cfg["name"])
            cfg["name"] = info.name
        end
        control_handler.idle_heartbeat[] = _push_idle_heartbeat_seconds(info)
        control_handler.flow_control[] = _push_flow_control_enabled(info)
        control_handler.last_seen[] = time()
        psub = PushSubscription(js, sub, String(stream), String(info.name), ReentrantLock(), delete_on_close, false,
                                nothing, control_handler, info)
        psub.heartbeat_task = _start_push_heartbeat_monitor(psub, control_handler)
        psub
    catch err
        cleanup_errors = Any[]
        try
            close(sub)
        catch cleanup_err
            push!(cleanup_errors, CleanupError("close push subscription $deliver_subject", cleanup_err))
        end
        if delete_on_close && consumer_created
            try
                consumer_delete(js, stream, (info::ConsumerInfo).name)
            catch cleanup_err
                push!(cleanup_errors, CleanupError("delete push consumer $((info::ConsumerInfo).name)", cleanup_err))
            end
        end
        isempty(cleanup_errors) ? rethrow() : throw(Base.CompositeException(vcat(Any[err], cleanup_errors)))
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
    _wait_task!(errors, "stop push heartbeat monitor $(psub.consumer)", psub.heartbeat_task;
                interrupt=true)
    handler = psub.control_handler
    server_deleted = !isnothing(handler) && handler.consumer_deleted[]
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

const _JS_ACK_PREFIX = "\$JS.ACK."
const _JS_ACK_NOT_MESSAGE = "message is not a JetStream message"
const _ACK_PAYLOAD_NAK = Vector{UInt8}(codeunits("-NAK"))
const _ACK_PAYLOAD_PROGRESS = Vector{UInt8}(codeunits("+WPI"))
const _ACK_PAYLOAD_TERM = Vector{UInt8}(codeunits("+TERM"))
const _ACK_PAYLOAD_NEXT = Vector{UInt8}(codeunits("+NXT"))

struct _ParsedMsgMetadata
    stream_first::Int
    stream_last::Int
    consumer_first::Int
    consumer_last::Int
    delivered::Int
    stream_sequence::Int
    consumer_sequence::Int
    timestamp_ns::Int
    pending::Int
    domain_first::Int
    domain_last::Int
    has_domain::Bool
end

_ack_metadata_error() = throw(JetStreamError(400, nothing, _JS_ACK_NOT_MESSAGE))

function _ack_token_count(reply::String)::Int
    tokens = 1
    @inbounds for i in 1:ncodeunits(reply)
        codeunit(reply, i) == UInt8('.') && (tokens += 1)
    end
    tokens
end

function _ack_next_dot(reply::String, first::Int, last::Int)::Int
    @inbounds for i in first:last
        codeunit(reply, i) == UInt8('.') && return i
    end
    0
end

function _ack_required_dot(reply::String, first::Int, last::Int)::Int
    dot = _ack_next_dot(reply, first, last)
    dot == 0 && _ack_metadata_error()
    dot
end

function _ack_parse_int(reply::String, first::Int, last::Int)::Int
    first <= last || throw(ArgumentError("JetStream ack metadata integer field cannot be empty"))

    negative = false
    i = first
    @inbounds sign = codeunit(reply, i)
    if sign == UInt8('-') || sign == UInt8('+')
        negative = sign == UInt8('-')
        i += 1
        i <= last || throw(ArgumentError("JetStream ack metadata integer field has no digits"))
    end

    limit = negative ? UInt64(typemax(Int)) + UInt64(1) : UInt64(typemax(Int))
    value = UInt64(0)
    @inbounds while i <= last
        digit_byte = codeunit(reply, i)
        UInt8('0') <= digit_byte <= UInt8('9') ||
            throw(ArgumentError("JetStream ack metadata integer field contains a non-digit"))
        digit = UInt64(digit_byte - UInt8('0'))
        value <= (limit - digit) ÷ UInt64(10) ||
            throw(OverflowError("JetStream ack metadata integer field overflows Int"))
        value = value * UInt64(10) + digit
        i += 1
    end

    if negative
        value == limit && return typemin(Int)
        return -Int(value)
    end
    Int(value)
end

function _ack_parse_int_token(reply::String, first::Int, last::Int, require_dot::Bool)::Tuple{Int,Int}
    dot = _ack_next_dot(reply, first, last)
    if dot == 0
        require_dot && _ack_metadata_error()
        return _ack_parse_int(reply, first, last), 0
    end
    _ack_parse_int(reply, first, dot - 1), dot + 1
end

function _ack_field_string(reply::String, first::Int, last::Int)::String
    first <= last || return ""
    String(SubString(reply, first, prevind(reply, last + 1)))
end

function _ack_field_equals(reply::String, first::Int, last::Int, value::String)::Bool
    ncodeunits(value) == last - first + 1 || return false
    @inbounds for i in 1:ncodeunits(value)
        codeunit(reply, first + i - 1) == codeunit(value, i) || return false
    end
    true
end

function _parse_ack_metadata_no_domain(reply::String, first::Int, last::Int)::_ParsedMsgMetadata
    dot = _ack_required_dot(reply, first, last)
    stream_first, stream_last = first, dot - 1

    first = dot + 1
    dot = _ack_required_dot(reply, first, last)
    consumer_first, consumer_last = first, dot - 1

    delivered, first = _ack_parse_int_token(reply, dot + 1, last, true)
    stream_sequence, first = _ack_parse_int_token(reply, first, last, true)
    consumer_sequence, first = _ack_parse_int_token(reply, first, last, true)
    timestamp_ns, first = _ack_parse_int_token(reply, first, last, true)
    pending = _ack_parse_int(reply, first, last)

    _ParsedMsgMetadata(stream_first, stream_last, consumer_first, consumer_last,
                       delivered, stream_sequence, consumer_sequence, timestamp_ns,
                       pending, 0, -1, false)
end

function _parse_ack_metadata_with_domain(reply::String, first::Int, last::Int)::_ParsedMsgMetadata
    dot = _ack_required_dot(reply, first, last)
    domain_first, domain_last = first, dot - 1

    first = dot + 1
    dot = _ack_required_dot(reply, first, last) # account hash

    first = dot + 1
    dot = _ack_required_dot(reply, first, last)
    stream_first, stream_last = first, dot - 1

    first = dot + 1
    dot = _ack_required_dot(reply, first, last)
    consumer_first, consumer_last = first, dot - 1

    delivered, first = _ack_parse_int_token(reply, dot + 1, last, true)
    stream_sequence, first = _ack_parse_int_token(reply, first, last, true)
    consumer_sequence, first = _ack_parse_int_token(reply, first, last, true)
    timestamp_ns, first = _ack_parse_int_token(reply, first, last, true)
    pending, _ = _ack_parse_int_token(reply, first, last, false)

    _ParsedMsgMetadata(stream_first, stream_last, consumer_first, consumer_last,
                       delivered, stream_sequence, consumer_sequence, timestamp_ns,
                       pending, domain_first, domain_last, true)
end

function _parse_msg_metadata(reply::String)::_ParsedMsgMetadata
    startswith(reply, _JS_ACK_PREFIX) || _ack_metadata_error()
    token_count = _ack_token_count(reply)
    first = ncodeunits(_JS_ACK_PREFIX) + 1
    last = ncodeunits(reply)
    if token_count == 9
        return _parse_ack_metadata_no_domain(reply, first, last)
    elseif token_count >= 11
        return _parse_ack_metadata_with_domain(reply, first, last)
    end
    _ack_metadata_error()
end

function _parse_msg_metadata(msg::Msg)::_ParsedMsgMetadata
    reply = getfield(msg, :reply)
    reply === nothing && _ack_metadata_error()
    _parse_msg_metadata(reply)
end

function _parse_msg_metadata(msg::JetStreamMsg)::_ParsedMsgMetadata
    reply = getfield(msg, :reply)
    reply === nothing && _ack_metadata_error()
    _parse_msg_metadata(reply)
end

function _parse_msg_metadata(msg::AbstractMsg)::_ParsedMsgMetadata
    reply = msg.reply
    reply === nothing && _ack_metadata_error()
    _parse_msg_metadata(reply)
end

function metadata(msg::AbstractMsg)
    reply = msg.reply
    isnothing(reply) && _ack_metadata_error()
    parsed = _parse_msg_metadata(reply::String)
    domain = if parsed.has_domain
        _ack_field_equals(reply, parsed.domain_first, parsed.domain_last, "_") ?
            "" : _ack_field_string(reply, parsed.domain_first, parsed.domain_last)
    else
        nothing
    end
    MsgMetadata(_ack_field_string(reply, parsed.stream_first, parsed.stream_last),
                _ack_field_string(reply, parsed.consumer_first, parsed.consumer_last),
                parsed.delivered, parsed.stream_sequence, parsed.consumer_sequence,
                parsed.timestamp_ns, parsed.pending, domain)
end

function _ack_delay_ns(delay)::Int
    delay isa Real && !(delay isa Bool) ||
        throw(ArgumentError("delay must be a finite non-negative number"))
    seconds = Float64(delay)
    isfinite(seconds) && seconds >= 0 ||
        throw(ArgumentError("delay must be a finite non-negative number"))
    ns = seconds * 1_000_000_000
    isfinite(ns) && ns <= typemax(Int) ||
        throw(ArgumentError("delay is too large"))
    round(Int, ns)
end

function _ack_payload(kind::Symbol; delay=nothing)
    if kind == :ack
        EMPTY_BYTES
    elseif kind == :nak
        if isnothing(delay)
            _ACK_PAYLOAD_NAK
        else
            args = JSON3.write(Dict("delay" => _ack_delay_ns(delay)))
            Vector{UInt8}(codeunits("-NAK $args"))
        end
    elseif kind == :progress
        _ACK_PAYLOAD_PROGRESS
    elseif kind == :term
        _ACK_PAYLOAD_TERM
    elseif kind == :next
        _ACK_PAYLOAD_NEXT
    else
        throw(ArgumentError("unknown ack kind $kind"))
    end
end

function _ack_reply_subject(msg::JetStreamMsg)::String
    isnothing(msg.reply) && throw(JetStreamError(400, nothing, "message has no ack reply subject"))
    msg.reply
end

_ack_terminal(kind::Symbol)::Bool = kind != :progress

function _ack_already_acknowledged()
    throw(JetStreamError(400, nothing, "message already acknowledged"))
end

function _begin_ack!(msg::JetStreamMsg)
    while true
        state = @atomic msg._ack_state
        state == _JS_ACK_DONE && _ack_already_acknowledged()
        if state == _JS_ACK_OPEN
            replaced = @atomicreplace msg._ack_state _JS_ACK_OPEN => _JS_ACK_BUSY
            replaced.success && return nothing
        end
        yield()
    end
end

function _finish_ack!(msg::JetStreamMsg, terminal::Bool, succeeded::Bool)
    next_state = terminal && succeeded ? _JS_ACK_DONE : _JS_ACK_OPEN
    @atomic msg._ack_state = next_state
    nothing
end

_acknowledged(msg::JetStreamMsg)::Bool = (@atomic msg._ack_state) == _JS_ACK_DONE

function _ack_publish(msg::JetStreamMsg, kind::Symbol; delay=nothing)::Nothing
    reply = _ack_reply_subject(msg)
    payload = _ack_payload(kind; delay)
    terminal = _ack_terminal(kind)
    _begin_ack!(msg)
    succeeded = false
    try
        _publish_unchecked(msg._client, reply, payload;
                           buffer_on_reconnect=!terminal,
                           force_flush=terminal)
        succeeded = true
    finally
        _finish_ack!(msg, terminal, succeeded)
    end
    nothing
end

function _ack_request(msg::JetStreamMsg, kind::Symbol; delay=nothing, timeout::Real=1.0)::Msg
    timeout = _positive_timeout_seconds("timeout", timeout)
    reply = _ack_reply_subject(msg)
    payload = _ack_payload(kind; delay)
    terminal = _ack_terminal(kind)
    _begin_ack!(msg)
    succeeded = false
    try
        mux = _ensure_request_mux(msg._client)
        token, waiter = _register_request_waiter!(msg._client, mux, timeout)
        response_subject = "$(mux.prefix).$token"
        response = try
            frame = PublishFrame(reply, response_subject, payload, EMPTY_BYTES)
            _publish_frame_unchecked(msg._client, frame; buffer_on_reconnect=false, force_flush=true)
            _wait_request_reply(mux, waiter, timeout)
        finally
            _remove_request_waiter!(msg._client, mux, token, waiter)
        end
        code = _status_header(response)
        if code == 503
            throw(NoRespondersError(reply))
        elseif !isnothing(code) && code >= 400
            throw(ProtocolError("request failed with status $code $(_status_description(response))"))
        end
        succeeded = true
        response
    finally
        _finish_ack!(msg, terminal, succeeded)
    end
end

ack(msg::JetStreamMsg)::Nothing = _ack_publish(msg, :ack)
ack_sync(msg::JetStreamMsg; timeout::Real=1.0)::Msg = _ack_request(msg, :ack; timeout)
nak(msg::JetStreamMsg; delay=nothing)::Nothing = _ack_publish(msg, :nak; delay)
in_progress(msg::JetStreamMsg)::Nothing = _ack_publish(msg, :progress)
term(msg::JetStreamMsg)::Nothing = _ack_publish(msg, :term)
