const _JS_ACK_OPEN = UInt8(0)
const _JS_ACK_BUSY = UInt8(1)
const _JS_ACK_DONE = UInt8(2)
abstract type AbstractJetStreamMsg{C<:Client} <: AbstractMsg end

mutable struct JetStreamMsg{C<:Client} <: AbstractJetStreamMsg{C}
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

mutable struct BorrowedJetStreamMsg{C<:Client,D<:AbstractVector{UInt8},H} <: AbstractJetStreamMsg{C}
    subject::String
    reply::Union{String,Nothing}
    data::D
    headers::H
    sid::Int
    header_bytes::Int
    _client::C
    @atomic _ack_state::UInt8
end

BorrowedJetStreamMsg(msg::BorrowedMsg, client::C) where {C<:Client} =
    BorrowedJetStreamMsg{C,typeof(msg.data),typeof(msg.headers)}(
        msg.subject, msg.reply, msg.data, msg.headers, msg.sid, msg.header_bytes, client,
        _JS_ACK_OPEN)

Base.String(msg::BorrowedJetStreamMsg) = _bytes_to_string(msg.data)

struct PubAck
    stream::String
    seq::Int
    duplicate::Bool
    domain::Union{String,Nothing}
end

struct _JSErrorJSON
    code::Union{Int,Nothing}
    err_code::Union{Int,Nothing}
    description::Union{String,Nothing}
end

struct _PubAckJSON
    stream::Union{String,Nothing}
    seq::Union{Int,Nothing}
    duplicate::Union{Bool,Nothing}
    domain::Union{String,Nothing}
    error::Union{_JSErrorJSON,Nothing}
end

struct StoredMsg <: AbstractMsg
    subject::String
    reply::Union{String,Nothing}
    data::Vector{UInt8}
    headers::HeaderStorage
    sid::Int
    header_bytes::Int
    seq::Int
    created::DateTime
end

StoredMsg(msg::Msg, seq::Integer, created::DateTime) =
    StoredMsg(msg.subject, msg.reply, msg.data, msg.headers, msg.sid, msg.header_bytes,
              Int(seq), created)

Base.String(msg::StoredMsg) = _bytes_to_string(msg.data)

abstract type AbstractJetStreamAsyncPublishState{C<:Client} end

mutable struct JetStreamPublishFuture{C<:Client,S<:AbstractJetStreamAsyncPublishState{C}}
    state::S
    token::Int
    reply::String
    subject::String
    deadline::Float64
    generation::Int
    retry_attempts::Int
    retry_wait::Float64
    retries::Int
    retry_deadline::Float64
    retry_frame::Union{_AbstractPublishFrame,Nothing}
    ready::Bool
    active::Bool
    value::Union{PubAck,Exception,Nothing}
end

mutable struct JetStreamAsyncPublishState{C<:Client} <: AbstractJetStreamAsyncPublishState{C}
    client::C
    prefix::String
    condition::Base.GenericCondition{ReentrantLock}
    timeout_condition::Base.GenericCondition{ReentrantLock}
    setup_lock::ReentrantLock
    sub::Union{Subscription{C},Nothing}
    futures::Dict{Int,JetStreamPublishFuture{C,JetStreamAsyncPublishState{C}}}
    max_pending::Int
    pending::Int
    next_token::Int
    wait_queue::_ConditionTimeoutQueue
    deadline_queue::_DeadlineQueue{JetStreamPublishFuture{C,JetStreamAsyncPublishState{C}}}
    retry_queue::_DeadlineQueue{JetStreamPublishFuture{C,JetStreamAsyncPublishState{C}}}
    timeout_task::Union{Task,Nothing}
end

function _register_js_async_publish_state!(client::Client, state::JetStreamAsyncPublishState)
    _register_client_lifecycle_watcher!(client, state)
end

function JetStreamAsyncPublishState(client::C, max_pending::Int) where {C<:Client}
    lock = ReentrantLock()
    state = JetStreamAsyncPublishState{C}(
        client,
        new_inbox(client),
        Base.Threads.Condition(lock),
        Base.Threads.Condition(lock),
        ReentrantLock(),
        nothing,
        Dict{Int,JetStreamPublishFuture{C,JetStreamAsyncPublishState{C}}}(),
        max_pending,
        0,
        0,
        _ConditionTimeoutQueue(lock),
        _DeadlineQueue{JetStreamPublishFuture{C,JetStreamAsyncPublishState{C}}}(),
        _DeadlineQueue{JetStreamPublishFuture{C,JetStreamAsyncPublishState{C}}}(),
        nothing,
    )
    _register_js_async_publish_state!(client, state)
    state
end

struct JetStreamContext{C<:Client,S<:JetStreamAsyncPublishState{C}}
    client::C
    prefix::String
    timeout::Float64
    publish_futures::S
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

Base.@kwdef struct StreamLostData
    msgs::Vector{Int} = Int[]
    bytes::Int = 0
end

Base.@kwdef struct StreamState
    messages::Int = 0
    bytes::Int = 0
    first_seq::Int = 0
    first_ts::Union{DateTime,Nothing} = nothing
    last_seq::Int = 0
    last_ts::Union{DateTime,Nothing} = nothing
    consumer_count::Int = 0
    num_deleted::Int = 0
    deleted::Vector{Int} = Int[]
    num_subjects::Int = 0
    subjects::Dict{String,Int} = Dict{String,Int}()
    lost::Union{StreamLostData,Nothing} = nothing
end

struct StreamInfo
    name::String
    config::StreamConfig
    state::StreamState
end

Base.@kwdef struct ConsumerSequenceInfo
    consumer_seq::Int = 0
    stream_seq::Int = 0
    last_active::Union{DateTime,Nothing} = nothing
end

struct ConsumerInfo
    stream_name::String
    name::String
    config::ConsumerConfig
    created::Union{DateTime,Nothing}
    delivered::ConsumerSequenceInfo
    ack_floor::ConsumerSequenceInfo
    num_ack_pending::Int
    num_redelivered::Int
    num_waiting::Int
    num_pending::Int
    push_bound::Bool
    paused::Bool
    pause_remaining::Float64
end

StreamInfo(name::AbstractString, config::StreamConfig, state::StreamState=StreamState()) =
    StreamInfo(String(name), config, state)

function ConsumerInfo(stream_name::AbstractString, name::AbstractString, config::ConsumerConfig;
                      created::Union{DateTime,Nothing}=nothing,
                      delivered::ConsumerSequenceInfo=ConsumerSequenceInfo(),
                      ack_floor::ConsumerSequenceInfo=ConsumerSequenceInfo(),
                      num_ack_pending::Integer=0, num_redelivered::Integer=0,
                      num_waiting::Integer=0, num_pending::Integer=0,
                      push_bound::Bool=false, paused::Bool=false,
                      pause_remaining::Real=0.0)
    ConsumerInfo(String(stream_name), String(name), config, created, delivered, ack_floor,
                 Int(num_ack_pending), Int(num_redelivered), Int(num_waiting), Int(num_pending),
                 push_bound, paused, Float64(pause_remaining))
end

struct JetStreamPage{T}
    items::Vector{T}
    offset::Int
    total::Int
    limit::Int
end

Base.length(page::JetStreamPage) = length(page.items)
Base.isempty(page::JetStreamPage) = isempty(page.items)
Base.iterate(page::JetStreamPage, state...) = iterate(page.items, state...)
Base.IteratorEltype(::Type{<:JetStreamPage{T}}) where {T} = Base.HasEltype()
Base.eltype(::Type{<:JetStreamPage{T}}) where {T} = T

_page_next_offset(page::JetStreamPage)::Int = page.offset + length(page.items)
_page_complete(page::JetStreamPage)::Bool = isempty(page.items) || _page_next_offset(page) >= page.total

struct JetStreamPageIterator{T,F}
    fetch_page::F
    offset::Int
end

JetStreamPageIterator{T}(fetch_page::F, offset::Int) where {T,F} =
    JetStreamPageIterator{T,F}(fetch_page, offset)

Base.IteratorSize(::Type{<:JetStreamPageIterator}) = Base.SizeUnknown()
Base.IteratorEltype(::Type{<:JetStreamPageIterator{T}}) where {T} = Base.HasEltype()
Base.eltype(::Type{<:JetStreamPageIterator{T}}) where {T} = JetStreamPage{T}

function Base.iterate(iter::JetStreamPageIterator)
    _iterate_jetstream_pages(iter, iter.offset, typemax(Int), true)
end

function Base.iterate(iter::JetStreamPageIterator, state)
    next_offset, total = state
    _iterate_jetstream_pages(iter, next_offset, total, false)
end

function _iterate_jetstream_pages(iter::JetStreamPageIterator, next_offset::Int,
                                  total::Int, first_page::Bool)
    (!first_page && next_offset >= total) && return nothing
    page = iter.fetch_page(next_offset)
    isempty(page) && return nothing
    page_next = _page_next_offset(page)
    page, (page_next, page.total)
end

struct JetStreamItemIterator{T,P}
    pages::P
end

Base.IteratorSize(::Type{<:JetStreamItemIterator}) = Base.SizeUnknown()
Base.IteratorEltype(::Type{<:JetStreamItemIterator{T}}) where {T} = Base.HasEltype()
Base.eltype(::Type{<:JetStreamItemIterator{T}}) where {T} = T

function Base.iterate(iter::JetStreamItemIterator)
    _iterate_jetstream_items(iter.pages, iterate(iter.pages))
end

function Base.iterate(iter::JetStreamItemIterator, state)
    page, item_state, page_state = state
    item_result = iterate(page.items, item_state)
    if !isnothing(item_result)
        item, next_item_state = item_result
        return item, (page, next_item_state, page_state)
    end
    _iterate_jetstream_items(iter.pages, iterate(iter.pages, page_state))
end

function _iterate_jetstream_items(pages, page_result)
    while !isnothing(page_result)
        page, page_state = page_result
        item_result = iterate(page.items)
        if !isnothing(item_result)
            item, item_state = item_result
            return item, (page, item_state, page_state)
        end
        page_result = iterate(pages, page_state)
    end
    nothing
end

function jetstream(client::Client; prefix::AbstractString="\$JS.API", timeout::Real=5.0,
                   publish_future_max_pending::Integer=256)
    max_pending = _positive_integer_option("publish_future_max_pending", publish_future_max_pending)
    JetStreamContext(client, String(prefix), _positive_timeout_seconds("timeout", timeout),
                     JetStreamAsyncPublishState(client, max_pending))
end

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
const _JS_HEADER_CONSUMER_STALLED = "Nats-Consumer-Stalled"
const _JS_HEADER_LAST_CONSUMER = "Nats-Last-Consumer"
const DEFAULT_JS_PUBLISH_RETRY_ATTEMPTS = 2
const DEFAULT_JS_PUBLISH_RETRY_WAIT = 0.25

function _jetstream_control_status(msg::AbstractMsg)
    isempty(msg.data) || return nothing
    code = _status_header(msg)
    isnothing(code) && return nothing
    code
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

@inline _js_status_byte(bytes::AbstractVector{UInt8}, pos::Int)::UInt8 = bytes[pos]
@inline _js_status_byte(value::AbstractString, pos::Int)::UInt8 = codeunit(value, pos)

function _js_ascii_trim_hspace(value, first::Int, last::Int)::Tuple{Int,Int}
    @inbounds while first <= last && _is_hspace(_js_status_byte(value, first))
        first += 1
    end
    @inbounds while first <= last && _is_hspace(_js_status_byte(value, last))
        last -= 1
    end
    first, last
end

function _js_ascii_eq_ci_stripped(value, first::Int, last::Int, expected::AbstractString)::Bool
    first, last = _js_ascii_trim_hspace(value, first, last)
    last - first + 1 == ncodeunits(expected) || return false
    @inbounds for i in 1:ncodeunits(expected)
        _ascii_lower(_js_status_byte(value, first + i - 1)) == _ascii_lower(codeunit(expected, i)) ||
            return false
    end
    true
end

function _js_ascii_contains_ci(value, first::Int, last::Int, expected::AbstractString)::Bool
    expected_len = ncodeunits(expected)
    expected_len == 0 && return true
    last - first + 1 >= expected_len || return false
    expected_first = _ascii_lower(codeunit(expected, 1))
    stop = last - expected_len + 1
    @inbounds for pos in first:stop
        _ascii_lower(_js_status_byte(value, pos)) == expected_first || continue
        matched = true
        for i in 2:expected_len
            if _ascii_lower(_js_status_byte(value, pos + i - 1)) != _ascii_lower(codeunit(expected, i))
                matched = false
                break
            end
        end
        matched && return true
    end
    false
end

function _jetstream_status_description_eq(msg::AbstractMsg, expected::AbstractString)::Bool
    hdrs = msg.headers
    if hdrs isa LazyHeaders
        return _jetstream_status_description_eq_lazy(hdrs, expected)
    elseif hdrs isa RawHeaders
        return _jetstream_status_description_eq_raw(hdrs, expected)
    end
    description = _status_description(msg)
    _js_ascii_eq_ci_stripped(description, 1, ncodeunits(description), expected)
end

function _jetstream_status_description_eq_lazy(hdrs::LazyHeaders, expected::AbstractString)::Bool
    first, last = _lazy_status_description_range(hdrs)
    _js_ascii_eq_ci_stripped(hdrs.raw, first, last, expected)
end

function _jetstream_status_description_eq_raw(hdrs::RawHeaders, expected::AbstractString)::Bool
    first, last = _raw_status_description_range(hdrs)
    _js_ascii_eq_ci_stripped(hdrs.raw, first, last, expected)
end

function _jetstream_status_description_contains(msg::AbstractMsg, expected::AbstractString)::Bool
    hdrs = msg.headers
    if hdrs isa LazyHeaders
        return _jetstream_status_description_contains_lazy(hdrs, expected)
    elseif hdrs isa RawHeaders
        return _jetstream_status_description_contains_raw(hdrs, expected)
    end
    description = _status_description(msg)
    _js_ascii_contains_ci(description, 1, ncodeunits(description), expected)
end

function _jetstream_status_description_contains_lazy(hdrs::LazyHeaders, expected::AbstractString)::Bool
    first, last = _lazy_status_description_range(hdrs)
    _js_ascii_contains_ci(hdrs.raw, first, last, expected)
end

function _jetstream_status_description_contains_raw(hdrs::RawHeaders, expected::AbstractString)::Bool
    first, last = _raw_status_description_range(hdrs)
    _js_ascii_contains_ci(hdrs.raw, first, last, expected)
end

function _jetstream_status_description(msg::AbstractMsg, code::Int)::String
    description = _status_description(msg)
    isempty(description) ? _jetstream_default_status_description(code) : description
end

function _jetstream_status_action(msg::AbstractMsg, request_subject::Union{String,Nothing}=nothing)
    code = _jetstream_control_status(msg)
    isnothing(code) && return :message, nothing
    subject = isnothing(request_subject) ? msg.subject : request_subject

    if code == _JS_STATUS_CONTROL
        _jetstream_status_description_eq(msg, _JS_DESC_IDLE_HEARTBEAT) && return :idle_heartbeat, nothing
        _jetstream_status_description_eq(msg, _JS_DESC_FLOW_CONTROL) && return :flow_control, nothing
        return :control, nothing
    elseif code == _JS_STATUS_NO_MESSAGES
        return :no_messages, nothing
    elseif code == _JS_STATUS_TIMEOUT
        return :timeout, nothing
    elseif code == _JS_STATUS_NO_RESPONDERS
        return :no_responders, NoRespondersError(subject)
    elseif code == _JS_STATUS_PIN_ID_MISMATCH
        return :pin_id_mismatch, JetStreamError(code, nothing, _jetstream_status_description(msg, code))
    elseif code == _JS_STATUS_CONFLICT
        if _jetstream_status_description_contains(msg, _JS_DESC_MAX_BYTES_EXCEEDED) ||
           _jetstream_status_description_contains(msg, "maxbytes") ||
           _jetstream_status_description_contains(msg, "exceeded")
            return :max_bytes_exceeded, nothing
        elseif _jetstream_status_description_contains(msg, _JS_DESC_BATCH_COMPLETED)
            return :batch_completed, nothing
        elseif _jetstream_status_description_contains(msg, _JS_DESC_CONSUMER_DELETED)
            return :consumer_deleted, JetStreamError(code, nothing, _jetstream_status_description(msg, code))
        elseif _jetstream_status_description_contains(msg, _JS_DESC_LEADERSHIP_CHANGE)
            return :leadership_change, JetStreamError(code, nothing, _jetstream_status_description(msg, code))
        elseif _jetstream_status_description_contains(msg, _JS_DESC_SERVER_SHUTDOWN)
            return :server_shutdown, JetStreamError(code, nothing, _jetstream_status_description(msg, code))
        else
            return :error, JetStreamError(code, nothing, _jetstream_status_description(msg, code))
        end
    elseif code >= 400
        return :error, JetStreamError(code, nothing, _jetstream_status_description(msg, code))
    end

    :control, nothing
end

_jetstream_heartbeat_error() =
    JetStreamError(_JS_STATUS_TIMEOUT, nothing, "JetStream idle heartbeat timed out")

function _json_haskey(obj, key::Symbol)::Bool
    obj isa AbstractDict && haskey(obj, String(key)) && return true
    try
        return haskey(obj, key)
    catch err
        err isa MethodError || rethrow()
        return false
    end
end

function _json_get(obj, key::Symbol, default)
    if obj isa AbstractDict
        string_key = String(key)
        haskey(obj, string_key) && return obj[string_key]
    end
    try
        haskey(obj, key) && return obj[key]
    catch err
        err isa MethodError || rethrow()
    end
    default
end

function _json_get_required(obj, key::Symbol)
    _json_haskey(obj, key) || throw(ProtocolError("JetStream response missing $(String(key))"))
    _json_get(obj, key, nothing)
end

function _json_bool(value, default::Bool=false)::Bool
    isnothing(value) && return default
    Bool(value)
end

function _json_int(value, default::Int=0)::Int
    isnothing(value) && return default
    value isa Real && !(value isa Bool) || throw(ProtocolError("JetStream response integer field is not numeric"))
    Int(value)
end

function _json_seconds_from_ns(value, default::Float64=0.0)::Float64
    isnothing(value) && return default
    value isa Real && !(value isa Bool) || throw(ProtocolError("JetStream response duration field is not numeric"))
    _nanoseconds_to_seconds(value)
end

_json_datetime(value)::Union{DateTime,Nothing} = isnothing(value) ? nothing : _parse_rfc3339_datetime(String(value))

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
    _json_haskey(obj, :error) && throw(_js_error_from_object(_json_get(obj, :error, nothing)))
    obj
end

function _js_decode(msg::Msg)
    obj = _js_read_response(msg)
    isnothing(obj) ? (;) : obj
end

function _api_request(js::JetStreamContext, subject::String, payload=nothing; timeout::Real=js.timeout,
                      cancel_token::MaybeCancellationToken=nothing)
    msg = request(js.client, subject, payload; timeout, cancel_token)
    _js_decode(msg)
end

function _puback(obj)
    PubAck(String(_json_get_required(obj, :stream)), Int(_json_get_required(obj, :seq)),
           Bool(_json_get(obj, :duplicate, false)),
           _json_haskey(obj, :domain) ? String(_json_get(obj, :domain, "")) : nothing)
end

function _js_error_from_object(err::_JSErrorJSON)
    JetStreamError(something(err.code, 0), err.err_code, something(err.description, ""))
end

function _puback(ack::_PubAckJSON)
    isnothing(ack.error) || throw(_js_error_from_object(ack.error))
    isnothing(ack.stream) && throw(ProtocolError("JetStream publish response is missing stream"))
    isnothing(ack.seq) && throw(ProtocolError("JetStream publish response is missing seq"))
    PubAck(ack.stream, ack.seq, something(ack.duplicate, false), ack.domain)
end

function _js_read_puback(msg::Msg)::PubAck
    isempty(msg.data) && throw(ProtocolError("JetStream publish response is empty"))
    ack = try
        JSON3.read(msg.data, _PubAckJSON)
    catch err
        err isa InterruptException && rethrow()
        obj = _js_read_response(msg)
        isnothing(obj) && throw(ProtocolError("JetStream publish response is empty"))
        return _puback(obj)
    end
    _puback(ack)
end

function _js_header_nonempty(name::AbstractString, value)::String
    s = String(value)
    isempty(s) && throw(ArgumentError("$name cannot be empty"))
    s
end

function _js_header_sequence(name::AbstractString, value)::String
    string(_nonnegative_integer_option(name, value))
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

_ensure_js_publish_headers(hdrs::Headers) = hdrs
_ensure_js_publish_headers(::Nothing) = Headers()

function _js_publish_headers(headers; stream::Union{AbstractString,Nothing}=nothing,
                             expected_stream::Union{AbstractString,Nothing}=nothing,
                             msg_id=nothing, expected_last_sequence=nothing,
                             expected_last_subject_sequence=nothing,
                             expected_last_subject=nothing,
                             expected_last_msg_id=nothing, ttl=nothing,
                             schedule=nothing, schedule_at=nothing, schedule_every=nothing,
                             schedule_target=nothing, schedule_source=nothing,
                             schedule_ttl=nothing, schedule_timezone=nothing)::Union{Headers,Nothing}
    hdrs = isnothing(headers) ? nothing : _headers_copy(headers)
    if !isnothing(stream) && !isnothing(expected_stream) && String(stream) != String(expected_stream)
        throw(ArgumentError("stream and expected_stream must match when both are provided"))
    end
    expected = isnothing(expected_stream) ? stream : expected_stream
    publish_schedule = _js_publish_schedule(schedule, schedule_at, schedule_every)

    if !isnothing(msg_id)
        hdrs = _push_header!(_ensure_js_publish_headers(hdrs), _JS_MSG_ID_HEADER,
                             _js_header_nonempty("msg_id", msg_id))
    end
    if !isnothing(expected)
        hdrs = _push_header!(_ensure_js_publish_headers(hdrs), _JS_EXPECTED_STREAM_HEADER,
                             _validate_api_name("stream", expected))
    end
    if !isnothing(expected_last_sequence)
        hdrs = _push_header!(_ensure_js_publish_headers(hdrs), _JS_EXPECTED_LAST_SEQUENCE_HEADER,
                             _js_header_sequence("expected_last_sequence", expected_last_sequence))
    end
    if !isnothing(expected_last_subject_sequence)
        hdrs = _push_header!(_ensure_js_publish_headers(hdrs),
                             _JS_EXPECTED_LAST_SUBJECT_SEQUENCE_HEADER,
                             _js_header_sequence("expected_last_subject_sequence",
                                                 expected_last_subject_sequence))
    end
    if !isnothing(expected_last_subject)
        isnothing(expected_last_subject_sequence) &&
            throw(ArgumentError("expected_last_subject requires expected_last_subject_sequence"))
        hdrs = _push_header!(_ensure_js_publish_headers(hdrs),
                             _JS_EXPECTED_LAST_SUBJECT_SEQUENCE_SUBJECT_HEADER,
                             _validate_publish_subject(expected_last_subject))
    end
    if !isnothing(expected_last_msg_id)
        hdrs = _push_header!(_ensure_js_publish_headers(hdrs), _JS_EXPECTED_LAST_MSG_ID_HEADER,
                             _js_header_nonempty("expected_last_msg_id", expected_last_msg_id))
    end
    if !isnothing(ttl)
        hdrs = _push_header!(_ensure_js_publish_headers(hdrs), _JS_MSG_TTL_HEADER,
                             _js_duration_header("ttl", ttl; min_seconds=1.0))
    end

    if !isnothing(publish_schedule)
        hdrs = _push_header!(_ensure_js_publish_headers(hdrs), _JS_SCHEDULE_HEADER,
                             publish_schedule)
    end
    if !isnothing(schedule_target)
        hdrs = _push_header!(_ensure_js_publish_headers(hdrs), _JS_SCHEDULE_TARGET_HEADER,
                             _validate_publish_subject(schedule_target))
    end
    if !isnothing(schedule_source)
        hdrs = _push_header!(_ensure_js_publish_headers(hdrs), _JS_SCHEDULE_SOURCE_HEADER,
                             _validate_publish_subject(schedule_source))
    end
    if !isnothing(schedule_ttl)
        hdrs = _push_header!(_ensure_js_publish_headers(hdrs), _JS_SCHEDULE_TTL_HEADER,
                             _js_duration_header("schedule_ttl", schedule_ttl; allow_never=true))
    end
    if !isnothing(schedule_timezone)
        hdrs = _push_header!(_ensure_js_publish_headers(hdrs), _JS_SCHEDULE_TIMEZONE_HEADER,
                             _js_header_nonempty("schedule_timezone", schedule_timezone))
    end
    hdrs
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
                    schedule_timezone=nothing,
                    retry_attempts::Integer=DEFAULT_JS_PUBLISH_RETRY_ATTEMPTS,
                    retry_wait::Real=DEFAULT_JS_PUBLISH_RETRY_WAIT,
                    cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    timeout = _positive_timeout_seconds("timeout", timeout)
    hdrs = _js_publish_headers(headers; stream, expected_stream, msg_id, expected_last_sequence,
                               expected_last_subject_sequence, expected_last_subject,
                               expected_last_msg_id, ttl, schedule, schedule_at, schedule_every,
                               schedule_target, schedule_source, schedule_ttl, schedule_timezone)
    attempts = _nonnegative_integer_option("retry_attempts", retry_attempts)
    wait_seconds = _js_publish_retry_wait(retry_wait)
    deadline = time() + timeout
    attempt = 0
    while true
        remaining = deadline - time()
        _throw_if_cancelled(cancel_token)
        remaining > 0 || throw(TimeoutError("request timed out"))
        try
            msg = request(js.client, subject, data; timeout=remaining, headers=hdrs, cancel_token)
            return _js_read_puback(msg)
        catch err
            err isa NoRespondersError && attempt < attempts || rethrow()
            attempt += 1
            _sleep_or_cancel(min(wait_seconds, max(0.0, deadline - time())), cancel_token)
        end
    end
end
