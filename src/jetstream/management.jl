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

function _js_plain_config_value(value)
    if isnothing(value)
        return nothing
    elseif value isa AbstractString
        return String(value)
    elseif value isa Union{AbstractDict,JSON3.Object}
        return Dict{String,Any}(String(k) => _js_plain_config_value(v) for (k, v) in pairs(value))
    elseif value isa Union{AbstractVector,JSON3.Array}
        return Any[_js_plain_config_value(v) for v in value]
    else
        return value
    end
end
function _js_response_config_payload(obj)::Dict{String,Any}
    config = _js_plain_config_value(_json_get_required(obj, :config))
    config isa Dict{String,Any} || throw(ProtocolError("JetStream response config is not an object"))
    config
end

struct _JSConfigFieldMissing end
const _JS_CONFIG_FIELD_MISSING = _JSConfigFieldMissing()

_js_config_field(observed::AbstractDict, field::String) =
    haskey(observed, field) ? observed[field] : _JS_CONFIG_FIELD_MISSING

_js_absent_config_field_reflects(::Nothing) = true
_js_absent_config_field_reflects(value::Bool) = value == false
_js_absent_config_field_reflects(value::Real) = value == 0
_js_absent_config_field_reflects(value::AbstractString) = isempty(value)
_js_absent_config_field_reflects(value::AbstractDict) = isempty(value)
_js_absent_config_field_reflects(value::AbstractVector) = isempty(value)
_js_absent_config_field_reflects(_value) = false

# Server responses may omit false, zero, and empty defaults. Non-missing values
# are still compared by _js_requested_config_reflected, so stale truthy values fail.
function _js_config_value_is_empty_default(value)::Bool
    value isa _JSConfigFieldMissing && return true
    value isa Nothing && return true
    value isa Bool && return value == false
    value isa Real && return value == 0
    value isa AbstractString && return isempty(value)
    if value isa AbstractDict
        for nested in values(value)
            _js_config_value_is_empty_default(nested) || return false
        end
        return true
    elseif value isa AbstractVector
        for nested in value
            _js_config_value_is_empty_default(nested) || return false
        end
        return true
    end
    false
end

const _JS_KNOWN_STREAM_CONFIG_FIELDS = Set{String}(string.(fieldnames(StreamConfig)))
const _JS_KNOWN_CONSUMER_CONFIG_FIELDS = Set{String}(string.(fieldnames(ConsumerConfig)))

_js_unknown_config_field(kind::AbstractString, field::AbstractString)::Bool =
    kind == "stream" ? !(field in _JS_KNOWN_STREAM_CONFIG_FIELDS) :
    kind == "consumer" ? !(field in _JS_KNOWN_CONSUMER_CONFIG_FIELDS) :
    false

function _js_requested_config_reflected(expected, observed; strict_observed_extras::Bool=true)::Bool
    observed isa _JSConfigFieldMissing && return _js_absent_config_field_reflects(expected)
    if expected isa AbstractDict
        observed isa AbstractDict || return false
        expected_keys = Set{String}()
        for (raw_key, expected_value) in pairs(expected)
            key = String(raw_key)
            push!(expected_keys, key)
            _js_requested_config_reflected(expected_value, _js_config_field(observed, key);
                                          strict_observed_extras) ||
                return false
        end
        if strict_observed_extras
            for (raw_key, observed_value) in pairs(observed)
                String(raw_key) in expected_keys && continue
                _js_config_value_is_empty_default(observed_value) || return false
            end
        end
        return true
    elseif expected isa AbstractVector
        observed isa AbstractVector || return false
        length(expected) == length(observed) || return false
        for i in eachindex(expected)
            _js_requested_config_reflected(expected[i], observed[i];
                                          strict_observed_extras) || return false
        end
        return true
    else
        return expected == observed
    end
end

function _js_stream_sources_reflected(expected, observed; strict_observed_extras::Bool=true)::Bool
    observed isa _JSConfigFieldMissing && return _js_absent_config_field_reflects(expected)
    expected isa AbstractVector && observed isa AbstractVector || return false
    length(expected) == length(observed) || return false
    used = falses(length(observed))
    for expected_source in expected
        found = false
        for i in eachindex(observed)
            used[i] && continue
            if _js_requested_config_reflected(expected_source, observed[i];
                                             strict_observed_extras)
                used[i] = true
                found = true
                break
            end
        end
        found || return false
    end
    true
end

_js_server_metadata_key(key::AbstractString)::Bool = startswith(String(key), "_nats")

function _js_user_metadata(value::AbstractDict)::Dict{String,Any}
    metadata = Dict{String,Any}()
    for (raw_key, metadata_value) in pairs(value)
        key = String(raw_key)
        _js_server_metadata_key(key) && continue
        metadata[key] = metadata_value
    end
    metadata
end

function _js_metadata_reflected(expected, observed)::Bool
    expected isa AbstractDict || return false
    requested = Dict{String,Any}(String(k) => v for (k, v) in pairs(expected))
    observed isa _JSConfigFieldMissing && return isempty(requested)
    observed isa AbstractDict || return false
    requested == _js_user_metadata(observed)
end

function _assert_js_config_reflected!(kind::AbstractString, requested::Dict{String,Any},
                                      observed::Dict{String,Any};
                                      allow_unknown_field_extras::Bool=false)
    for (field, expected) in requested
        strict_observed_extras =
            !(allow_unknown_field_extras && _js_unknown_config_field(kind, field))
        reflected = field == "metadata" ?
                    _js_metadata_reflected(expected, _js_config_field(observed, field)) :
                    kind == "stream" && field == "sources" ?
                    _js_stream_sources_reflected(expected, _js_config_field(observed, field);
                                                strict_observed_extras) :
                    _js_requested_config_reflected(expected, _js_config_field(observed, field);
                                                  strict_observed_extras)
        reflected ||
            throw(UnsupportedFeatureError("JetStream $kind config field $field was not reflected by server response"))
    end
    nothing
end

function _assert_stream_config_reflected!(requested::Dict{String,Any}, response;
                                          allow_unknown_field_extras::Bool=false)
    _assert_js_config_reflected!("stream", requested, _js_response_config_payload(response);
                                 allow_unknown_field_extras)
    _stream_info(response)
end

function _assert_consumer_config_reflected!(requested::Dict{String,Any}, response;
                                            allow_unknown_field_extras::Bool=false)
    _assert_js_config_reflected!("consumer", requested, _js_response_config_payload(response);
                                 allow_unknown_field_extras)
    _consumer_info(response)
end

function stream_create(js::JetStreamContext, config::StreamConfig; timeout::Real=js.timeout,
                       cancel_token::MaybeCancellationToken=nothing)
    name = _stream_config_name(config)
    payload = _stream_config_payload(config)
    response = _api_request(js, "$(js.prefix).STREAM.CREATE.$name", JSON3.write(payload);
                            timeout, cancel_token)
    _assert_stream_config_reflected!(payload, response)
end

function stream_create(js::JetStreamContext, config::AbstractDict{String,<:Any}; timeout::Real=js.timeout,
                       cancel_token::MaybeCancellationToken=nothing)
    name = _stream_config_name(config)
    payload = _stream_config_payload(config)
    response = _api_request(js, "$(js.prefix).STREAM.CREATE.$name", JSON3.write(payload);
                            timeout, cancel_token)
    _assert_stream_config_reflected!(payload, response; allow_unknown_field_extras=true)
end

function stream_update(js::JetStreamContext, config::StreamConfig; timeout::Real=js.timeout,
                       cancel_token::MaybeCancellationToken=nothing)
    name = _stream_config_name(config)
    payload = _stream_config_payload(config)
    response = _api_request(js, "$(js.prefix).STREAM.UPDATE.$name", JSON3.write(payload);
                            timeout, cancel_token)
    _assert_stream_config_reflected!(payload, response)
end

function stream_update(js::JetStreamContext, config::AbstractDict{String,<:Any}; timeout::Real=js.timeout,
                       cancel_token::MaybeCancellationToken=nothing)
    name = _stream_config_name(config)
    payload = _stream_config_payload(config)
    response = _api_request(js, "$(js.prefix).STREAM.UPDATE.$name", JSON3.write(payload);
                            timeout, cancel_token)
    _assert_stream_config_reflected!(payload, response; allow_unknown_field_extras=true)
end

stream_info(js::JetStreamContext, name::AbstractString; timeout::Real=js.timeout,
            cancel_token::MaybeCancellationToken=nothing) =
    _stream_info(_api_request(js, "$(js.prefix).STREAM.INFO.$(_validate_api_name("stream", name))", "";
                              timeout, cancel_token))

function _stream_list_page(js::JetStreamContext, offset::Int; timeout::Real,
                           cancel_token::MaybeCancellationToken=nothing)::JetStreamPage{StreamInfo}
    obj = _api_request(js, "$(js.prefix).STREAM.LIST", JSON3.write((offset=offset,));
                       timeout, cancel_token)
    items = StreamInfo[_stream_info(item) for item in _json_get(obj, :streams, ())]
    page_offset = _json_int(_json_get(obj, :offset, offset))
    total = _json_int(_json_get(obj, :total, page_offset + length(items)))
    limit = _json_int(_json_get(obj, :limit, length(items)))
    JetStreamPage(items, page_offset, total, limit)
end

function stream_list_page(js::JetStreamContext; offset=0, timeout::Real=js.timeout,
                          cancel_token::MaybeCancellationToken=nothing)
    offset = _nonnegative_integer_option("stream list offset", offset)
    timeout = _positive_timeout_seconds("timeout", timeout)
    _stream_list_page(js, offset; timeout, cancel_token)
end

function stream_list_pages(js::JetStreamContext; offset=0, timeout::Real=js.timeout,
                           cancel_token::MaybeCancellationToken=nothing)
    offset = _nonnegative_integer_option("stream list offset", offset)
    timeout = _positive_timeout_seconds("timeout", timeout)
    JetStreamPageIterator{StreamInfo}(next_offset -> _stream_list_page(
        js, next_offset; timeout, cancel_token), offset)
end

function stream_list_iter(js::JetStreamContext; kwargs...)
    pages = stream_list_pages(js; kwargs...)
    JetStreamItemIterator{StreamInfo,typeof(pages)}(pages)
end

function stream_list(js::JetStreamContext; offset=0, timeout::Real=js.timeout,
                     cancel_token::MaybeCancellationToken=nothing)
    offset = _nonnegative_integer_option("stream list offset", offset)
    timeout = _positive_timeout_seconds("timeout", timeout)
    deadline = time() + timeout
    streams = StreamInfo[]
    next_offset = offset
    while true
        page = _stream_list_page(js, next_offset;
                                 timeout=_remaining_timeout_or_throw(deadline, "stream list";
                                                                     cancel_token),
                                 cancel_token)
        append!(streams, page.items)
        _page_complete(page) && break
        next_offset = _page_next_offset(page)
    end
    streams
end

function _stream_names_page(js::JetStreamContext, offset::Int,
                            subject::Union{String,Nothing}; timeout::Real,
                            cancel_token::MaybeCancellationToken=nothing)::JetStreamPage{String}
    req = isnothing(subject) ? (offset=offset,) : (subject=subject, offset=offset)
    obj = _api_request(js, "$(js.prefix).STREAM.NAMES", JSON3.write(req);
                       timeout, cancel_token)
    items = String[String(item) for item in _json_get(obj, :streams, String[])]
    page_offset = _json_int(_json_get(obj, :offset, offset))
    total = _json_int(_json_get(obj, :total, page_offset + length(items)))
    limit = _json_int(_json_get(obj, :limit, length(items)))
    JetStreamPage(items, page_offset, total, limit)
end

function stream_names_page(js::JetStreamContext; subject::Union{AbstractString,Nothing}=nothing,
                           offset=0, timeout::Real=js.timeout,
                           cancel_token::MaybeCancellationToken=nothing)
    subject = isnothing(subject) ? nothing : _validate_subject(subject)
    offset = _nonnegative_integer_option("stream names offset", offset)
    timeout = _positive_timeout_seconds("timeout", timeout)
    _stream_names_page(js, offset, subject; timeout, cancel_token)
end

function stream_names_pages(js::JetStreamContext; subject::Union{AbstractString,Nothing}=nothing,
                            offset=0, timeout::Real=js.timeout,
                            cancel_token::MaybeCancellationToken=nothing)
    subject = isnothing(subject) ? nothing : _validate_subject(subject)
    offset = _nonnegative_integer_option("stream names offset", offset)
    timeout = _positive_timeout_seconds("timeout", timeout)
    JetStreamPageIterator{String}(next_offset -> _stream_names_page(
        js, next_offset, subject; timeout, cancel_token), offset)
end

function stream_names_iter(js::JetStreamContext; kwargs...)
    pages = stream_names_pages(js; kwargs...)
    JetStreamItemIterator{String,typeof(pages)}(pages)
end

function stream_names(js::JetStreamContext; subject::Union{AbstractString,Nothing}=nothing,
                      offset=0, timeout::Real=js.timeout,
                      cancel_token::MaybeCancellationToken=nothing)
    subject = isnothing(subject) ? nothing : _validate_subject(subject)
    offset = _nonnegative_integer_option("stream names offset", offset)
    timeout = _positive_timeout_seconds("timeout", timeout)
    deadline = time() + timeout
    names = String[]
    next_offset = offset
    while true
        page = _stream_names_page(js, next_offset, subject;
                                  timeout=_remaining_timeout_or_throw(deadline, "stream names";
                                                                      cancel_token),
                                  cancel_token)
        append!(names, page.items)
        _page_complete(page) && break
        next_offset = _page_next_offset(page)
    end
    names
end

stream_delete(js::JetStreamContext, name::AbstractString; timeout::Real=js.timeout,
              cancel_token::MaybeCancellationToken=nothing) =
    _json_bool(_json_get_required(_api_request(js, "$(js.prefix).STREAM.DELETE.$(_validate_api_name("stream", name))", "";
                                                timeout, cancel_token), :success))

function _validate_stream_purge_keep(keep::Union{Integer,Nothing})
    isnothing(keep) && return nothing
    keep isa Bool && throw(ArgumentError("keep must be non-negative"))
    keep >= 0 || throw(ArgumentError("keep must be non-negative"))
    Int(keep)
end

stream_purge(js::JetStreamContext, name::AbstractString; filter_subject::Union{AbstractString,Nothing}=nothing,
             keep::Union{Integer,Nothing}=nothing, timeout::Real=js.timeout,
             cancel_token::MaybeCancellationToken=nothing) = begin
    filter = isnothing(filter_subject) ? nothing : _validate_subject(filter_subject)
    req = Dict{String,Any}()
    isnothing(filter) || (req["filter"] = filter)
    keep = _validate_stream_purge_keep(keep)
    isnothing(keep) || (req["keep"] = keep)
    _json_bool(_json_get_required(_api_request(js, "$(js.prefix).STREAM.PURGE.$(_validate_api_name("stream", name))",
                                               JSON3.write(req); timeout, cancel_token), :success))
end

function _json_int_vector(value)::Vector{Int}
    isnothing(value) && return Int[]
    out = Int[]
    sizehint!(out, length(value))
    for item in value
        push!(out, _json_int(item))
    end
    out
end

function _json_string_int_dict(value)::Dict{String,Int}
    out = Dict{String,Int}()
    isnothing(value) && return out
    for (k, v) in pairs(value)
        out[String(k)] = _json_int(v)
    end
    out
end

function _stream_lost_data_from_payload(value)::Union{StreamLostData,Nothing}
    isnothing(value) && return nothing
    StreamLostData(msgs=_json_int_vector(_json_get(value, :msgs, ())),
                   bytes=_json_int(_json_get(value, :bytes, 0)))
end

function _stream_state_from_payload(value)::StreamState
    StreamState(
        messages=_json_int(_json_get(value, :messages, 0)),
        bytes=_json_int(_json_get(value, :bytes, 0)),
        first_seq=_json_int(_json_get(value, :first_seq, 0)),
        first_ts=_json_datetime(_json_get(value, :first_ts, nothing)),
        last_seq=_json_int(_json_get(value, :last_seq, 0)),
        last_ts=_json_datetime(_json_get(value, :last_ts, nothing)),
        consumer_count=_json_int(_json_get(value, :consumer_count, _json_get(value, :consumers, 0))),
        num_deleted=_json_int(_json_get(value, :num_deleted, 0)),
        deleted=_json_int_vector(_json_get(value, :deleted, ())),
        num_subjects=_json_int(_json_get(value, :num_subjects, 0)),
        subjects=_json_string_int_dict(_json_get(value, :subjects, nothing)),
        lost=_stream_lost_data_from_payload(_json_get(value, :lost, nothing)),
    )
end

function _stream_info(obj)
    config = _json_get_required(obj, :config)
    cfg = _stream_config_from_payload(config)
    state = _stream_state_from_payload(_json_get_required(obj, :state))
    name = isnothing(cfg.name) ? String(_json_get(config, :name, "")) : cfg.name
    StreamInfo(name, cfg, state)
end

function _validate_stream_sequence(seq)::Int
    _positive_integer_option("stream sequence", seq)
end

function _stream_message_get_request(seq::Union{Integer,Nothing}, subject::Union{AbstractString,Nothing}, next_by_subject::Bool)::Dict{String,Any}
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

function _direct_message_response_info(request_subject::String, response::Msg)
    code = _status_header(response)
    if code == 503
        throw(NoRespondersError(request_subject))
    elseif !isnothing(code) && code >= 400
        description = _status_description(response)
        isempty(description) && (description = "direct get failed")
        throw(JetStreamError(code, nothing, description))
    end

    _direct_metadata_header(response, "Nats-Stream")
    subject = _direct_metadata_header(response, "Nats-Subject")
    sequence = _parse_direct_sequence(response)
    created = _parse_rfc3339_datetime(_direct_metadata_header(response, "Nats-Time-Stamp"))
    headers = _headers_copy(response.headers)
    for name in _DIRECT_GET_METADATA_HEADERS
        _delete_header!(headers, name)
    end
    StoredMsg(Msg(subject, nothing, copy(response.data); headers), sequence, created)
end

function _stream_message_get_direct_info(js::JetStreamContext, stream::String, req::Dict{String,Any};
                                         timeout::Real,
                                         cancel_token::MaybeCancellationToken=nothing)
    request_subject =
        if haskey(req, "last_by_subj") && !haskey(req, "seq")
            "$(js.prefix).DIRECT.GET.$stream.$(req["last_by_subj"])"
        else
            "$(js.prefix).DIRECT.GET.$stream"
        end
    payload = haskey(req, "last_by_subj") && !haskey(req, "seq") ? "" : JSON3.write(req)
    response = _request_raw(js.client, request_subject, payload; timeout, cancel_token)
    _direct_message_response_info(request_subject, response)
end

function _stream_message_from_api_payload(raw_msg)
    data = _json_haskey(raw_msg, :data) ? base64decode(String(_json_get(raw_msg, :data, ""))) : UInt8[]
    hdrs = _json_haskey(raw_msg, :hdrs) ? _parse_headers(base64decode(String(_json_get(raw_msg, :hdrs, "")))) : Headers()
    created = _json_datetime(_json_get_required(raw_msg, :time))::DateTime
    msg = Msg(String(_json_get_required(raw_msg, :subject)), nothing, data; headers=hdrs)
    StoredMsg(msg, _json_int(_json_get_required(raw_msg, :seq)), created)
end

function _stream_message_get_api(js::JetStreamContext, stream::AbstractString, req::Dict{String,Any};
                                 timeout::Real, cancel_token::MaybeCancellationToken=nothing)
    obj = _api_request(js, "$(js.prefix).STREAM.MSG.GET.$stream", JSON3.write(req);
                       timeout, cancel_token)
    _stream_message_from_api_payload(_json_get_required(obj, :message))
end

function _stream_message_get_info(js::JetStreamContext, stream::AbstractString, req::Dict{String,Any};
                                  direct::Bool, timeout::Real,
                                  cancel_token::MaybeCancellationToken=nothing)
    direct && return _stream_message_get_direct_info(js, stream, req; timeout, cancel_token)
    _stream_message_get_api(js, stream, req; timeout, cancel_token)
end

function stream_message_get(js::JetStreamContext, stream::AbstractString; seq::Union{Integer,Nothing}=nothing, subject::Union{AbstractString,Nothing}=nothing,
                            direct::Bool=false, next_by_subject::Bool=false, timeout::Real=js.timeout,
                            cancel_token::MaybeCancellationToken=nothing)
    stream = _validate_api_name("stream", stream)
    req = _stream_message_get_request(seq, subject, next_by_subject)
    _stream_message_get_info(js, stream, req; direct, timeout, cancel_token)
end

function stream_message_delete(js::JetStreamContext, stream::AbstractString, seq::Integer;
                               timeout::Real=js.timeout,
                               cancel_token::MaybeCancellationToken=nothing)
    stream = _validate_api_name("stream", stream)
    seq = _validate_stream_sequence(seq)
    _json_bool(_json_get_required(_api_request(js, "$(js.prefix).STREAM.MSG.DELETE.$stream",
                                               JSON3.write((seq=seq,)); timeout, cancel_token), :success))
end

function _server_version_at_least(client::Client, major::Int, minor::Int)
    version = @lock client.lock client.info.version
    isnothing(version) && return false
    m = match(r"^(\d+)\.(\d+)", version)
    isnothing(m) && return false
    server_major = tryparse(Int, m.captures[1])
    isnothing(server_major) && return false
    server_minor = tryparse(Int, m.captures[2])
    isnothing(server_minor) && return false
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
                                  action::Union{String,Nothing}=nothing,
                                  allow_unknown_field_extras::Bool=false,
                                  cancel_token::MaybeCancellationToken=nothing)
    stream = _validate_api_name("stream", stream)
    payload = _js_config_payload(config)
    _consumer_create_payload_request(js, stream, payload; timeout, action,
                                     verify_config=true, allow_unknown_field_extras,
                                     cancel_token)
end

function _consumer_create_payload_request(js::JetStreamContext, stream::AbstractString, payload::Dict{String,Any};
                                          timeout::Real=js.timeout, action::Union{String,Nothing}=nothing,
                                          verify_config::Bool=false,
                                          allow_unknown_field_extras::Bool=false,
                                          cancel_token::MaybeCancellationToken=nothing)
    stream = _validate_api_name("stream", stream)
    _validate_consumer_config_payload!(payload)
    subject = _consumer_create_subject(js, stream, payload)
    response = _api_request(js, subject, JSON3.write(_consumer_request_payload(js, stream, payload, action));
                            timeout, cancel_token)
    if verify_config
        _assert_consumer_config_reflected!(payload, response; allow_unknown_field_extras)
    else
        _consumer_info(response)
    end
end

consumer_create(js::JetStreamContext, stream::AbstractString, config::ConsumerConfig;
                timeout::Real=js.timeout, cancel_token::MaybeCancellationToken=nothing) =
    _consumer_create_request(js, stream, config; timeout, action="create", cancel_token)

consumer_create(js::JetStreamContext, stream::AbstractString, config::AbstractDict{String,<:Any};
                timeout::Real=js.timeout, cancel_token::MaybeCancellationToken=nothing) =
    _consumer_create_request(js, stream, config; timeout, action="create",
                             allow_unknown_field_extras=true, cancel_token)

consumer_create_or_update(js::JetStreamContext, stream::AbstractString, config::ConsumerConfig;
                          timeout::Real=js.timeout, cancel_token::MaybeCancellationToken=nothing) =
    _consumer_create_request(js, stream, config; timeout, cancel_token)

consumer_create_or_update(js::JetStreamContext, stream::AbstractString, config::AbstractDict{String,<:Any};
                          timeout::Real=js.timeout, cancel_token::MaybeCancellationToken=nothing) =
    _consumer_create_request(js, stream, config; timeout, allow_unknown_field_extras=true,
                             cancel_token)

consumer_update(js::JetStreamContext, stream::AbstractString, config::ConsumerConfig;
                timeout::Real=js.timeout, cancel_token::MaybeCancellationToken=nothing) =
    _consumer_create_request(js, stream, config; timeout, action="update", cancel_token)

consumer_update(js::JetStreamContext, stream::AbstractString, config::AbstractDict{String,<:Any};
                timeout::Real=js.timeout, cancel_token::MaybeCancellationToken=nothing) =
    _consumer_create_request(js, stream, config; timeout, action="update",
                             allow_unknown_field_extras=true, cancel_token)

consumer_info(js::JetStreamContext, stream::AbstractString, consumer::AbstractString;
              timeout::Real=js.timeout, cancel_token::MaybeCancellationToken=nothing) =
    _consumer_info(_api_request(js, "$(js.prefix).CONSUMER.INFO.$(_validate_api_name("stream", stream)).$(_validate_api_name("consumer", consumer))", "";
                                timeout, cancel_token))

function _consumer_list_page(js::JetStreamContext, stream::String, offset::Int;
                             timeout::Real,
                             cancel_token::MaybeCancellationToken=nothing)::JetStreamPage{ConsumerInfo}
    obj = _api_request(js, "$(js.prefix).CONSUMER.LIST.$stream", JSON3.write((offset=offset,));
                       timeout, cancel_token)
    items = ConsumerInfo[_consumer_info(item) for item in _json_get(obj, :consumers, ())]
    page_offset = _json_int(_json_get(obj, :offset, offset))
    total = _json_int(_json_get(obj, :total, page_offset + length(items)))
    limit = _json_int(_json_get(obj, :limit, length(items)))
    JetStreamPage(items, page_offset, total, limit)
end

function consumer_list_page(js::JetStreamContext, stream::AbstractString; offset=0,
                            timeout::Real=js.timeout,
                            cancel_token::MaybeCancellationToken=nothing)
    stream = _validate_api_name("stream", stream)
    offset = _nonnegative_integer_option("consumer list offset", offset)
    timeout = _positive_timeout_seconds("timeout", timeout)
    _consumer_list_page(js, stream, offset; timeout, cancel_token)
end

function consumer_list_pages(js::JetStreamContext, stream::AbstractString; offset=0,
                             timeout::Real=js.timeout,
                             cancel_token::MaybeCancellationToken=nothing)
    stream = _validate_api_name("stream", stream)
    offset = _nonnegative_integer_option("consumer list offset", offset)
    timeout = _positive_timeout_seconds("timeout", timeout)
    JetStreamPageIterator{ConsumerInfo}(next_offset -> _consumer_list_page(
        js, stream, next_offset; timeout, cancel_token), offset)
end

function consumer_list_iter(js::JetStreamContext, stream::AbstractString; kwargs...)
    pages = consumer_list_pages(js, stream; kwargs...)
    JetStreamItemIterator{ConsumerInfo,typeof(pages)}(pages)
end

function consumer_list(js::JetStreamContext, stream::AbstractString; offset=0, timeout::Real=js.timeout,
                       cancel_token::MaybeCancellationToken=nothing)
    stream = _validate_api_name("stream", stream)
    offset = _nonnegative_integer_option("consumer list offset", offset)
    timeout = _positive_timeout_seconds("timeout", timeout)
    deadline = time() + timeout
    consumers = ConsumerInfo[]
    next_offset = offset
    while true
        page = _consumer_list_page(js, stream, next_offset;
                                   timeout=_remaining_timeout_or_throw(deadline, "consumer list";
                                                                       cancel_token),
                                   cancel_token)
        append!(consumers, page.items)
        _page_complete(page) && break
        next_offset = _page_next_offset(page)
    end
    consumers
end

consumer_delete(js::JetStreamContext, stream::AbstractString, consumer::AbstractString;
                timeout::Real=js.timeout, cancel_token::MaybeCancellationToken=nothing) =
    _json_bool(_json_get_required(_api_request(js, "$(js.prefix).CONSUMER.DELETE.$(_validate_api_name("stream", stream)).$(_validate_api_name("consumer", consumer))", "";
                                              timeout, cancel_token), :success))

function _consumer_sequence_info_from_payload(value)::ConsumerSequenceInfo
    isnothing(value) && return ConsumerSequenceInfo()
    ConsumerSequenceInfo(
        consumer_seq=_json_int(_json_get(value, :consumer_seq, 0)),
        stream_seq=_json_int(_json_get(value, :stream_seq, 0)),
        last_active=_json_datetime(_json_get(value, :last_active, nothing)),
    )
end

function _consumer_info(obj)
    cfg = _consumer_config_from_payload(_json_get_required(obj, :config))
    ConsumerInfo(
        String(_json_get_required(obj, :stream_name)),
        String(_json_get_required(obj, :name)),
        cfg;
        created=_json_datetime(_json_get(obj, :created, nothing)),
        delivered=_consumer_sequence_info_from_payload(_json_get(obj, :delivered, nothing)),
        ack_floor=_consumer_sequence_info_from_payload(_json_get(obj, :ack_floor, nothing)),
        num_ack_pending=_json_int(_json_get(obj, :num_ack_pending, 0)),
        num_redelivered=_json_int(_json_get(obj, :num_redelivered, 0)),
        num_waiting=_json_int(_json_get(obj, :num_waiting, 0)),
        num_pending=_json_int(_json_get(obj, :num_pending, 0)),
        push_bound=_json_bool(_json_get(obj, :push_bound, false)),
        paused=_json_bool(_json_get(obj, :paused, false)),
        pause_remaining=_json_seconds_from_ns(_json_get(obj, :pause_remaining, nothing)),
    )
end

_consumer_missing(err) = err isa JetStreamError && err.code == 404
_consumer_create_conflict(err) =
    err isa JetStreamError && err.err_code in (_JS_ERR_CONSUMER_NAME_EXISTS, _JS_ERR_CONSUMER_ALREADY_EXISTS)

_priority_policy_none(policy::Union{PriorityPolicy.T,Nothing}) =
    isnothing(policy) || policy == PriorityPolicy.NONE

function _pull_consumer_priority_groups(info::ConsumerInfo)::Vector{String}
    groups = info.config.priority_groups
    isnothing(groups) ? String[] : copy(groups)
end

function _validate_pull_consumer_priority_config(info::ConsumerInfo)
    groups = _pull_consumer_priority_groups(info)
    policy = info.config.priority_policy
    if isempty(groups)
        _priority_policy_none(policy) ||
            throw(ProtocolError("pull consumer $(info.name) has priority_policy without priority_groups"))
    elseif _priority_policy_none(policy)
        throw(ProtocolError("pull consumer $(info.name) has priority_groups without priority_policy"))
    end
    if !isnothing(info.config.priority_timeout) && policy != PriorityPolicy.PINNED_CLIENT
        throw(ProtocolError(
            "pull consumer $(info.name) has priority_timeout without priority_policy=pinned_client"))
    end
    policy, groups
end

function _consumer_info_or_nothing(js::JetStreamContext, stream::AbstractString, consumer::AbstractString;
                                   timeout::Real=js.timeout,
                                   cancel_token::MaybeCancellationToken=nothing)
    try
        consumer_info(js, stream, consumer; timeout, cancel_token)
    catch err
        _consumer_missing(err) && return nothing
        rethrow()
    end
end

function _delete_consumer_for_close!(mark_deleted::Function, js::JetStreamContext,
                                     stream::AbstractString, consumer::AbstractString,
                                     operation::AbstractString; timeout::Real,
                                     cancel_token::MaybeCancellationToken=nothing)
    try
        consumer_delete(js, stream, consumer; timeout, cancel_token) ||
            throw(ErrorException("consumer delete response did not indicate success"))
        mark_deleted()
    catch err
        if _consumer_missing(err)
            mark_deleted()
            return nothing
        end
        throw(CleanupError("$operation $consumer", err))
    end
    nothing
end

_consumer_normalized_config_value(value) = _js_plain_config_value(value)

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
    current = _js_config_payload(info.config)
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

const _ORDERED_CONSUMER_HEARTBEAT_SECONDS = 5.0
const _ORDERED_CONSUMER_ACK_WAIT_SECONDS = 22 * 60 * 60.0

function _prepare_ordered_push_consumer_config!(config::Dict{String,Any},
                                                queue::Union{String,Nothing})
    isnothing(queue) || throw(ArgumentError("ordered push consumers do not support queue groups"))
    for field in ("name", "durable_name", "deliver_subject", "deliver_group")
        if haskey(config, field) && !isnothing(config[field])
            throw(ArgumentError("ordered push consumers do not support $field"))
        end
    end
    if haskey(config, "ack_policy") && !isnothing(config["ack_policy"]) && config["ack_policy"] != "none"
        throw(ArgumentError("ordered push consumers require ack_policy=none"))
    end
    if haskey(config, "max_deliver") && !isnothing(config["max_deliver"]) && config["max_deliver"] != 1
        throw(ArgumentError("ordered push consumers require max_deliver=1"))
    end
    config["ack_policy"] = "none"
    config["flow_control"] = true
    config["max_deliver"] = 1
    _set_config_default!(config, "ack_wait", _seconds_to_nanoseconds(_ORDERED_CONSUMER_ACK_WAIT_SECONDS))
    _set_config_default!(config, "idle_heartbeat", _seconds_to_nanoseconds(_ORDERED_CONSUMER_HEARTBEAT_SECONDS))
    _set_config_default!(config, "num_replicas", 1)
    _set_config_default!(config, "mem_storage", true)
    config
end

function _copy_config_payload(config::Dict{String,Any})::Dict{String,Any}
    Dict{String,Any}(k => _consumer_normalized_config_value(v) for (k, v) in config)
end

function _ordered_push_reset_config(base_config::Dict{String,Any}, name::String, deliver::String,
                                    start_seq::Int)::Dict{String,Any}
    cfg = _copy_config_payload(base_config)
    cfg["name"] = name
    cfg["deliver_subject"] = deliver
    cfg["deliver_policy"] = "by_start_sequence"
    cfg["opt_start_seq"] = max(1, start_seq)
    delete!(cfg, "opt_start_time")
    cfg
end

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
                                  config::Dict{String,Any}, bind_fields; timeout::Real=js.timeout,
                                  verify_config::Bool=false,
                                  cancel_token::MaybeCancellationToken=nothing)
    _validate_consumer_config_payload!(config)
    existing = _consumer_info_or_nothing(js, stream, name; timeout, cancel_token)
    if !isnothing(existing)
        return _validate_bound_consumer_config(existing, config, bind_fields), false
    end
    try
        _consumer_create_payload_request(js, stream, config; timeout, action="create",
                                         verify_config, cancel_token), true
    catch err
        _consumer_create_conflict(err) || rethrow()
        _validate_bound_consumer_config(consumer_info(js, stream, name; timeout, cancel_token),
                                        config, bind_fields), false
    end
end
