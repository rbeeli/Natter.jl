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
    deadline_queue::_DeadlineQueue{JetStreamPublishFuture{C,JetStreamAsyncPublishState{C}}}
    timeout_task::Union{Task,Nothing}
end

function _register_js_async_publish_state!(client::Client, state::JetStreamAsyncPublishState)
    ref = WeakRef(state)
    @lock client.lock begin
        states = client.jetstream_async_publish_states
        live = 1
        for i in eachindex(states)
            if !isnothing(states[i].value)
                states[live] = states[i]
                live += 1
            end
        end
        resize!(states, live - 1)
        push!(states, ref)
    end
    nothing
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

function _js_async_publish_subscription_active(sub::Union{Subscription,Nothing})::Bool
    isnothing(sub) && return false
    @lock sub.lock !sub.closed && sub.server_active
end

function _js_async_publish_connection_error(client::Client, generation::Int)
    st = status(client)
    st == ConnectionStatus.CLOSED && return ConnectionClosedError()
    st == ConnectionStatus.DISCONNECTED && return ConnectionClosedError("connection is disconnected")
    st in (ConnectionStatus.CONNECTING, ConnectionStatus.RECONNECTING) && return ConnectionReconnectingError()
    _load_generation(client) == generation || return ConnectionReconnectingError()
    nothing
end

function _js_async_publish_result(future::JetStreamPublishFuture, msg::Msg)::Union{PubAck,Exception}
    try
        code = _status_header(msg)
        if code == _JS_STATUS_NO_RESPONDERS
            return NoRespondersError(future.subject)
        elseif !isnothing(code) && code >= 400
            return ProtocolError("request failed with status $code $(_status_description(msg))")
        end
        return _js_read_puback(msg)
    catch err
        return err
    end
end

function _resolve_js_publish_future_locked!(future::JetStreamPublishFuture{C},
                                            value::Union{PubAck,Exception})::Bool where {C<:Client}
    future.active || return false
    state = future.state::JetStreamAsyncPublishState{C}
    future.value = value
    future.ready = true
    future.active = false
    if get(state.futures, future.token, nothing) === future
        delete!(state.futures, future.token)
        state.pending = max(0, state.pending - 1)
        if isempty(state.futures)
            empty!(state.deadline_queue)
            _notify_js_async_publish_timeout_task_locked(state)
        else
            _compact_js_async_publish_deadline_queue_locked!(state)
        end
    end
    notify(state.condition; all=true)
    true
end

function _resolve_js_publish_future!(future::JetStreamPublishFuture{C},
                                     value::Union{PubAck,Exception}) where {C<:Client}
    state = future.state::JetStreamAsyncPublishState{C}
    lock(state.condition)
    try
        _resolve_js_publish_future_locked!(future, value)
    finally
        unlock(state.condition)
    end
    nothing
end

function _clear_js_async_publish_pending!(state::JetStreamAsyncPublishState{C},
                                          err::Exception) where {C<:Client}
    lock(state.condition)
    try
        if isempty(state.futures)
            empty!(state.deadline_queue)
            _notify_js_async_publish_timeout_task_locked(state)
            return nothing
        end
        state.pending = 0
        for future in values(state.futures)
            if future.active
                future.value = err
                future.ready = true
                future.active = false
            end
        end
        empty!(state.futures)
        empty!(state.deadline_queue)
        notify(state.condition; all=true)
        _notify_js_async_publish_timeout_task_locked(state)
    finally
        unlock(state.condition)
    end
    nothing
end

function _clear_js_async_publish_pending!(client::Client, err::Exception)
    states = Any[]
    @lock client.lock begin
        refs = client.jetstream_async_publish_states
        live = 1
        for i in eachindex(refs)
            state = refs[i].value
            if !isnothing(state)
                refs[live] = refs[i]
                live += 1
                push!(states, state)
            end
        end
        resize!(refs, live - 1)
    end
    for state in states
        _clear_js_async_publish_pending!(state, err)
    end
    nothing
end

function _handle_js_async_publish_ack(state::JetStreamAsyncPublishState,
                                      token::Union{Int,Nothing}, msg::Msg)
    if isnothing(token)
        _record_drop!(state.client)
        return nothing
    end
    future = lock(state.condition) do
        get(state.futures, token, nothing)
    end
    if isnothing(future)
        _record_drop!(state.client)
        return nothing
    end
    result = _js_async_publish_result(future, msg)
    result isa NoRespondersError && _retry_js_async_publish!(future) && return nothing
    _resolve_js_publish_future!(future, result)
    nothing
end

function _handle_js_async_publish_ack(state::JetStreamAsyncPublishState, msg::Msg)
    token = _request_mux_token(state.prefix, msg.subject)
    _handle_js_async_publish_ack(state, token, msg)
end

function _handle_js_async_publish_ack_parts(state::JetStreamAsyncPublishState,
                                            token::Union{Int,Nothing}, reply,
                                            payload::Vector{UInt8}, headers::HeaderStorage,
                                            sid::Int, header_bytes::Int)
    if isnothing(token)
        _record_drop!(state.client)
        return nothing
    end
    future = lock(state.condition) do
        get(state.futures, token, nothing)
    end
    if isnothing(future)
        _record_drop!(state.client)
        return nothing
    end
    msg = Msg(future.reply, reply, payload, headers, sid, header_bytes)
    result = _js_async_publish_result(future, msg)
    result isa NoRespondersError && _retry_js_async_publish!(future) && return nothing
    _resolve_js_publish_future!(future, result)
    nothing
end

function _try_handle_subscription_msg_control(handler::_JetStreamAsyncPublishControlHandler,
                                              client::Client, sub::Subscription,
                                              reader::ProtocolReader, subject_start::Int,
                                              subject_end::Int, reply_start::Int,
                                              reply_end::Int, size::Int)::Bool
    state = handler.state
    token = _request_mux_token(state.prefix, reader.buffer, subject_start, subject_end)
    reply, payload = _fast_control_msg_parts(reader, reply_start, reply_end, size)
    _handle_js_async_publish_ack_parts(state, token, reply, payload, nothing, sub.sid, 0)
    _record_in!(client, size)
    true
end

function _try_handle_subscription_hmsg_control(handler::_JetStreamAsyncPublishControlHandler,
                                               client::Client, sub::Subscription,
                                               reader::ProtocolReader, subject_start::Int,
                                               subject_end::Int, reply_start::Int,
                                               reply_end::Int, hsize::Int, total::Int)::Bool
    state = handler.state
    token = _request_mux_token(state.prefix, reader.buffer, subject_start, subject_end)
    hdrs, reply, payload = _fast_control_hmsg_parts(reader, reply_start, reply_end, hsize, total)
    _handle_js_async_publish_ack_parts(state, token, reply, payload, hdrs, sub.sid, hsize)
    _record_in!(client, total)
    true
end

function _handle_subscription_control(handler::_JetStreamAsyncPublishControlHandler,
                                      _sub::Subscription, msg::Msg)::Bool
    _handle_js_async_publish_ack(handler.state, msg)
    true
end

function _retry_js_async_publish!(future::JetStreamPublishFuture{C})::Bool where {C<:Client}
    state = future.state::JetStreamAsyncPublishState{C}
    frame = nothing
    retry_wait = 0.0
    lock(state.condition)
    try
        future.active || return false
        future.retries < future.retry_attempts || return false
        remaining = future.deadline - time()
        remaining > 0 || return false
        retry_frame = future.retry_frame
        isnothing(retry_frame) && return false
        future.retries += 1
        frame = retry_frame
        retry_wait = min(future.retry_wait, remaining)
    finally
        unlock(state.condition)
    end

    @async begin
        sleep(retry_wait)
        lock(state.condition)
        try
            future.active && future.deadline > time() || return
            isnothing(_js_async_publish_connection_error(state.client, future.generation)) || return
        finally
            unlock(state.condition)
        end
        try
            _publish_frame_unchecked(state.client, frame; buffer_on_reconnect=false)
        catch err
            _resolve_js_publish_future!(future, err)
        end
    end
    true
end

function _ensure_js_async_publish_timeout_task_locked!(state::JetStreamAsyncPublishState)
    task = state.timeout_task
    if isnothing(task) || istaskdone(task)
        state.timeout_task = @async _js_async_publish_timeout_loop(state)
    end
    nothing
end

function _js_async_publish_deadline_entry_valid_locked(state::JetStreamAsyncPublishState,
                                                       entry::_DeadlineEntry)::Bool
    future = get(state.futures, entry.token, nothing)
    future === entry.value && future.active && future.deadline == entry.deadline
end

function _next_js_async_publish_deadline_entry_locked!(state::JetStreamAsyncPublishState)
    while true
        entry = _deadline_queue_peek(state.deadline_queue)
        isnothing(entry) && return nothing
        _js_async_publish_deadline_entry_valid_locked(state, entry) && return entry
        _deadline_queue_pop!(state.deadline_queue)
    end
end

function _rebuild_js_async_publish_deadline_queue_locked!(state::JetStreamAsyncPublishState{C}) where {C<:Client}
    queue = _DeadlineQueue{JetStreamPublishFuture{C,JetStreamAsyncPublishState{C}}}()
    for (token, future) in state.futures
        future.active && _deadline_queue_push!(queue, token, future.deadline, future)
    end
    state.deadline_queue = queue
    nothing
end

function _compact_js_async_publish_deadline_queue_locked!(state::JetStreamAsyncPublishState)
    _deadline_queue_compaction_due(state.deadline_queue, length(state.futures)) ||
        return nothing
    _rebuild_js_async_publish_deadline_queue_locked!(state)
end

function _notify_js_async_publish_timeout_task_locked(state::JetStreamAsyncPublishState)
    notify(state.timeout_condition; all=true)
    nothing
end

function _wait_js_async_publish_timeout_locked!(state::JetStreamAsyncPublishState, delay::Float64)
    timer = Timer(min(delay, _MAX_TIMER_DELAY_SECONDS)) do _
        lock(state.timeout_condition)
        try
            _notify_js_async_publish_timeout_task_locked(state)
        finally
            unlock(state.timeout_condition)
        end
    end
    try
        wait(state.timeout_condition)
    finally
        close(timer)
    end
    nothing
end

function _js_async_publish_timeout_loop(state::JetStreamAsyncPublishState{C}) where {C<:Client}
    lock(state.condition)
    try
        while true
            if isempty(state.futures)
                empty!(state.deadline_queue)
                state.timeout_task = nothing
                return nothing
            end

            entry = _next_js_async_publish_deadline_entry_locked!(state)
            if isnothing(entry)
                _rebuild_js_async_publish_deadline_queue_locked!(state)
                entry = _next_js_async_publish_deadline_entry_locked!(state)
                if isnothing(entry)
                    empty!(state.futures)
                    empty!(state.deadline_queue)
                    state.pending = 0
                    state.timeout_task = nothing
                    return nothing
                end
            end

            err = _js_async_publish_connection_error(state.client, entry.value.generation)
            if !isnothing(err)
                _deadline_queue_pop!(state.deadline_queue)
                future = get(state.futures, entry.token, nothing)
                if future === entry.value && future.active && future.deadline == entry.deadline
                    _resolve_js_publish_future_locked!(future, err)
                end
                continue
            end

            delay = entry.deadline - time()
            if !isfinite(delay)
                wait(state.timeout_condition)
            elseif delay <= 0
                _deadline_queue_pop!(state.deadline_queue)
                future = get(state.futures, entry.token, nothing)
                if future === entry.value && future.active && future.deadline == entry.deadline
                    _resolve_js_publish_future_locked!(future, TimeoutError("JetStream async publish ack timed out"))
                end
            else
                _wait_js_async_publish_timeout_locked!(state, delay)
            end
        end
    finally
        unlock(state.condition)
    end
end

function _ensure_js_async_publish_subscription!(state::JetStreamAsyncPublishState{C};
                                                cancel_token::MaybeCancellationToken=nothing) where {C<:Client}
    _throw_if_cancelled(cancel_token)
    sub = lock(state.condition) do
        state.sub
    end
    _js_async_publish_subscription_active(sub) && return sub::Subscription{C}

    @lock state.setup_lock begin
        _throw_if_cancelled(cancel_token)
        sub = lock(state.condition) do
            state.sub
        end
        _js_async_publish_subscription_active(sub) && return sub::Subscription{C}
        _ensure_connected_for_request(state.client)

        if !isnothing(sub)
            closed = @lock sub.lock sub.closed
            if !closed
                _send_subscription_now!(sub; cancel_token) || throw(ConnectionReconnectingError())
                _js_async_publish_subscription_active(sub) && return sub::Subscription{C}
                throw(ConnectionReconnectingError())
            end
        end

        sub = _subscribe(state.client, "$(state.prefix).*";
                         _control_handler=_JetStreamAsyncPublishControlHandler(state),
                         pending_msgs_limit=state.max_pending,
                         pending_bytes_limit=state.client.options.sub_pending_bytes_limit,
                         require_connected=true,
                         cancel_token)
        lock(state.condition)
        try
            state.sub = sub
            notify(state.condition; all=true)
        finally
            unlock(state.condition)
        end
        sub
    end
end

function _next_js_async_publish_token!(state::JetStreamAsyncPublishState)::Int
    start = state.next_token
    while true
        token = state.next_token == typemax(Int) ? 1 : state.next_token + 1
        state.next_token = token
        !haskey(state.futures, token) && return token
        token == start && throw(OverflowError("JetStream async publish token space exhausted"))
    end
end

function _reserve_js_async_publish_future!(state::JetStreamAsyncPublishState{C},
                                           subject::AbstractString, deadline::Float64,
                                           generation::Int,
                                           retry_attempts::Int, retry_wait::Float64,
                                           retry_frame::Union{_AbstractPublishFrame,Nothing},
                                           cancel_token::MaybeCancellationToken) where {C<:Client}
    lock(state.condition)
    try
        while state.pending >= state.max_pending
            err = _js_async_publish_connection_error(state.client, generation)
            isnothing(err) || throw(err)
            remaining = deadline - time()
            remaining > 0 || throw(TimeoutError("JetStream async publish backpressure timed out"))
            ready = _wait_until_condition_locked(state.condition, remaining; cancel_token) do
                state.pending < state.max_pending ||
                    !isnothing(_js_async_publish_connection_error(state.client, generation))
            end
            ready || throw(TimeoutError("JetStream async publish backpressure timed out"))
        end

        token = _next_js_async_publish_token!(state)
        reply = string(state.prefix, '.', token)
        future = JetStreamPublishFuture{C,typeof(state)}(
            state, token, reply, String(subject), deadline, generation, retry_attempts,
            retry_wait, 0, retry_frame, false, true, nothing)
        state.futures[token] = future
        state.pending += 1
        wake_timeout = _deadline_queue_push!(state.deadline_queue, token, deadline, future)
        _ensure_js_async_publish_timeout_task_locked!(state)
        wake_timeout && _notify_js_async_publish_timeout_task_locked(state)
        future
    finally
        unlock(state.condition)
    end
end

function Base.wait(future::JetStreamPublishFuture)
    state = future.state
    lock(state.condition)
    value = try
        while !future.ready
            remaining = future.deadline - time()
            if remaining <= 0
                _resolve_js_publish_future_locked!(future, TimeoutError("JetStream async publish ack timed out"))
                break
            end
            _wait_until_notified_locked(state.condition) do
                future.ready
            end
        end
        future.value
    finally
        unlock(state.condition)
    end
    value isa Exception && throw(value)
    future
end

function Base.fetch(future::JetStreamPublishFuture)
    wait(future)
    future.value::PubAck
end

function Base.isready(future::JetStreamPublishFuture)
    state = future.state
    lock(state.condition)
    try
        future.ready
    finally
        unlock(state.condition)
    end
end

Base.show(io::IO, future::JetStreamPublishFuture) =
    print(io, "JetStreamPublishFuture(", future.subject, ", ",
          isready(future) ? "ready" : "pending", ")")

function js_publish_future(js::JetStreamContext, subject::AbstractString, data=nothing; timeout::Real=js.timeout,
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
    attempts = _nonnegative_integer_option("retry_attempts", retry_attempts)
    wait_seconds = _js_publish_retry_wait(retry_wait)
    hdrs = _js_publish_headers(headers; stream, expected_stream, msg_id, expected_last_sequence,
                               expected_last_subject_sequence, expected_last_subject,
                               expected_last_msg_id, ttl, schedule, schedule_at, schedule_every,
                               schedule_target, schedule_source, schedule_ttl, schedule_timezone)
    frame = _publish_frame(subject, nothing, data, hdrs)
    _validate_publish_frame_for_client(js.client, frame)
    _ensure_js_async_publish_subscription!(js.publish_futures; cancel_token)

    deadline = time() + timeout
    generation = _load_generation(js.client)
    future = _reserve_js_async_publish_future!(js.publish_futures, frame.subject, deadline, generation,
                                               attempts, wait_seconds, nothing, cancel_token)
    publish_frame = _PublishFrame(frame.subject, future.reply, frame.payload, frame.headers)
    if attempts > 0
        future.retry_frame = publish_frame
    end
    try
        _publish_frame_unchecked(js.client, publish_frame; buffer_on_reconnect=false, cancel_token)
    catch err
        _resolve_js_publish_future!(future, err)
        rethrow()
    end
    future
end

function js_publish_future_pending(js::JetStreamContext)::Int
    lock(js.publish_futures.condition)
    try
        js.publish_futures.pending
    finally
        unlock(js.publish_futures.condition)
    end
end

function js_publish_future_complete(js::JetStreamContext; timeout::Real=js.timeout,
                                     cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    timeout = _positive_timeout_seconds("timeout", timeout)
    deadline = time() + timeout
    state = js.publish_futures
    lock(state.condition)
    try
        while state.pending > 0
            remaining = deadline - time()
            remaining > 0 || throw(TimeoutError("JetStream async publish completion timed out"))
            ready = _wait_until_condition_locked(state.condition, remaining; cancel_token) do
                state.pending == 0
            end
            ready || throw(TimeoutError("JetStream async publish completion timed out"))
        end
    finally
        unlock(state.condition)
    end
    nothing
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

_pull_fetch_next_subject(js::JetStreamContext, stream::AbstractString, consumer::AbstractString)::String =
    string(js.prefix, ".CONSUMER.MSG.NEXT.", stream, ".", consumer)

mutable struct _PullStreamRequest
    remaining_messages::Int
    remaining_bytes::Int
end

struct _PullStreamReservation
    request::_PullStreamRequest
    batch::Int
    max_bytes::Union{Int,Nothing}
end

struct _PullStreamConfig
    batch::Int
    max_bytes::Union{Int,Nothing}
    expires::Float64
    heartbeat::Float64
    threshold_messages::Int
    threshold_bytes::Union{Int,Nothing}
    min_pending::Union{Int,Nothing}
    min_ack_pending::Union{Int,Nothing}
    priority_group::Union{String,Nothing}
    priority::Union{Int,Nothing}
    stop_after::Union{Int,Nothing}
    channel_size::Int
end

function _append_ascii!(buffer::Vector{UInt8}, value::AbstractString)
    append!(buffer, codeunits(value))
    buffer
end

function _append_json_int!(buffer::Vector{UInt8}, value::Int)
    value >= 0 || throw(ArgumentError("JSON integer must be non-negative"))
    digits = _decimal_digits(value)
    old = length(buffer)
    resize!(buffer, old + digits)
    pos = old + digits
    n = value
    @inbounds while pos > old
        buffer[pos] = UInt8('0') + UInt8(n % 10)
        n = div(n, 10)
        pos -= 1
    end
    buffer
end

function _append_json_hex_byte!(buffer::Vector{UInt8}, byte::UInt8)
    hi = byte >>> 4
    lo = byte & 0x0f
    push!(buffer, hi < 0x0a ? UInt8('0') + hi : UInt8('a') + (hi - 0x0a))
    push!(buffer, lo < 0x0a ? UInt8('0') + lo : UInt8('a') + (lo - 0x0a))
    buffer
end

function _append_json_string!(buffer::Vector{UInt8}, value::String)
    push!(buffer, UInt8('"'))
    for byte in codeunits(value)
        if byte == UInt8('"')
            _append_ascii!(buffer, "\\\"")
        elseif byte == UInt8('\\')
            _append_ascii!(buffer, "\\\\")
        elseif byte == UInt8('\b')
            _append_ascii!(buffer, "\\b")
        elseif byte == UInt8('\t')
            _append_ascii!(buffer, "\\t")
        elseif byte == UInt8('\n')
            _append_ascii!(buffer, "\\n")
        elseif byte == UInt8('\f')
            _append_ascii!(buffer, "\\f")
        elseif byte == UInt8('\r')
            _append_ascii!(buffer, "\\r")
        elseif byte < 0x20
            _append_ascii!(buffer, "\\u00")
            _append_json_hex_byte!(buffer, byte)
        else
            push!(buffer, byte)
        end
    end
    push!(buffer, UInt8('"'))
    buffer
end

function _pull_fetch_request_payload!(buffer::Vector{UInt8}, batch::Int, expires_ns::Int,
                                      heartbeat_ns::Int, max_bytes::Union{Int,Nothing},
                                      no_wait::Bool, pin_id::Union{String,Nothing},
                                      min_pending::Union{Int,Nothing},
                                      min_ack_pending::Union{Int,Nothing},
                                      priority_group::Union{String,Nothing},
                                      priority::Union{Int,Nothing})::Vector{UInt8}
    empty!(buffer)
    _append_ascii!(buffer, "{\"batch\":")
    _append_json_int!(buffer, batch)
    if !isnothing(max_bytes)
        _append_ascii!(buffer, ",\"max_bytes\":")
        _append_json_int!(buffer, max_bytes)
    end
    if expires_ns > 0
        _append_ascii!(buffer, ",\"expires\":")
        _append_json_int!(buffer, expires_ns)
    end
    if heartbeat_ns > 0
        _append_ascii!(buffer, ",\"idle_heartbeat\":")
        _append_json_int!(buffer, heartbeat_ns)
    end
    no_wait && _append_ascii!(buffer, ",\"no_wait\":true")
    if !isnothing(min_pending)
        _append_ascii!(buffer, ",\"min_pending\":")
        _append_json_int!(buffer, min_pending)
    end
    if !isnothing(min_ack_pending)
        _append_ascii!(buffer, ",\"min_ack_pending\":")
        _append_json_int!(buffer, min_ack_pending)
    end
    if !isnothing(priority_group)
        _append_ascii!(buffer, ",\"group\":")
        _append_json_string!(buffer, priority_group)
    end
    if !isnothing(priority)
        _append_ascii!(buffer, ",\"priority\":")
        _append_json_int!(buffer, priority)
    end
    if !isnothing(pin_id)
        _append_ascii!(buffer, ",\"id\":")
        _append_json_string!(buffer, pin_id)
    end
    push!(buffer, UInt8('}'))
    buffer
end

mutable struct _PullMessageStreamState
    lock::ReentrantLock
    closed::Bool
    error::Union{Exception,Nothing}
    requests::Vector{_PullStreamRequest}
    requested_messages::Int
    requested_bytes::Int
    delivered::Int
    buffered_messages::Int
    buffered_bytes::Int
end

_PullMessageStreamState() = _PullMessageStreamState(ReentrantLock(), false, nothing,
                                                    _PullStreamRequest[], 0, 0, 0, 0, 0)

mutable struct PullSubscription{C<:Client,J<:JetStreamContext{C}}
    js::J
    stream::String
    consumer::String
    next_subject::String
    fetch_lock::ReentrantLock
    close_lock::ReentrantLock
    delete_on_close::Bool
    closed::Bool
    server_deleted::Bool
    pin_id::Union{String,Nothing}
    active_fetches::Int
    active_stream::Bool
    active_deliveries::Vector{Subscription{C}}
    fetch_delivery::Union{Subscription{C},Nothing}
    fetch_delivery_in_use::Bool
    priority_policy::Union{PriorityPolicy.T,Nothing}
    priority_groups::Vector{String}
end

mutable struct PullMessageStream{C<:Client,P<:PullSubscription{C}}
    subscription::P
    delivery::Subscription{C}
    messages::MsgQueue{Msg}
    message_lock::ReentrantLock
    message_condition::Base.GenericCondition{ReentrantLock}
    config::_PullStreamConfig
    payload_buffer::Vector{UInt8}
    task::Task
    callback_task::Union{Task,Nothing}
    state::_PullMessageStreamState
end

function PullSubscription(js::J, stream::AbstractString, consumer::AbstractString,
                          fetch_lock::ReentrantLock, close_lock::ReentrantLock,
                          delete_on_close::Bool, closed::Bool,
                          priority_policy::Union{PriorityPolicy.T,Nothing}=nothing,
                          priority_groups::Vector{String}=String[]) where {C<:Client,J<:JetStreamContext{C}}
    stream = String(stream)
    consumer = String(consumer)
    PullSubscription{C,J}(js, stream, consumer, _pull_fetch_next_subject(js, stream, consumer),
                          fetch_lock, close_lock, delete_on_close, closed, false, nothing,
                          0, false, Subscription{C}[], nothing, false,
                          priority_policy, copy(priority_groups))
end

mutable struct PushSubscription{C<:Client,J<:JetStreamContext{C},S<:Subscription{C}}
    js::J
    sub::S
    stream::String
    consumer::String
    close_lock::ReentrantLock
    delete_on_close::Bool
    closed::Bool
    server_deleted::Bool
    heartbeat_task::Union{Task,Nothing}
    ordered_reset_task::Union{Task,Nothing}
    control_handler::Union{_JetStreamPushControlHandler,Nothing}
    info::Union{ConsumerInfo,Nothing}
end

function PushSubscription(js::J, sub::S, stream::AbstractString, consumer::AbstractString,
                          close_lock::ReentrantLock, delete_on_close::Bool, closed::Bool,
                          heartbeat_task::Union{Task,Nothing},
                          control_handler::Union{_JetStreamPushControlHandler,Nothing}) where {C<:Client,J<:JetStreamContext{C},S<:Subscription{C}}
    PushSubscription{C,J,S}(js, sub, String(stream), String(consumer), close_lock, delete_on_close, closed,
                            false, heartbeat_task, nothing, control_handler, nothing)
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

function _push_msg_metadata(msg::M) where {M<:AbstractMsg}
    reply = msg.reply
    isnothing(reply) && return nothing
    startswith(reply, _JS_ACK_PREFIX) || return nothing
    try
        _parse_msg_metadata(reply)
    catch err
        err isa JetStreamError || err isa ProtocolError || err isa ArgumentError ||
            err isa OverflowError || rethrow()
        nothing
    end
end

function _record_subscription_data_received!(handler::_JetStreamPushControlHandler,
                                             msg::M) where {M<:AbstractMsg}
    handler.flow_control[] && Threads.atomic_add!(handler.flow_incoming, one(UInt64))
    handler.ordered && return nothing
    handler.idle_heartbeat[] > 0 || return nothing
    @lock handler.lock begin
        if handler.sequence_state_anchored
            if handler.last_stream_seq > 0
                handler.next_consumer_seq += 1
                handler.last_stream_seq += 1
            end
        else
            handler.sequence_state_anchored = true
            parsed = _push_msg_metadata(msg)
            if !isnothing(parsed)
                handler.next_consumer_seq = parsed.consumer_sequence + 1
                handler.last_stream_seq = parsed.stream_sequence
            end
        end
    end
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

function _handle_subscription_control(handler::_JetStreamPushControlHandler, sub::Subscription,
                                      msg::M)::Bool where {M<:AbstractMsg}
    _touch_push_control_handler!(handler)
    isempty(msg.data) || return false
    action, err = _jetstream_status_action(msg)
    action == :message && return false
    if action == :flow_control
        _schedule_or_reply_to_flow_control(sub, handler, msg)
    elseif action == :idle_heartbeat
        _reply_to_consumer_stalled!(sub, msg)
        _handle_push_sequence_heartbeat!(handler, sub, msg)
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
            if handler.ordered
                reset_seq = @lock handler.lock handler.last_stream_seq + 1
                try
                    _request_ordered_push_reset!(handler, reset_seq)
                catch err
                    _report_error(psub.js.client, CleanupError("reset ordered push consumer after missed heartbeat", err))
                end
            else
                _report_error(psub.js.client, _jetstream_heartbeat_error())
            end
            _touch_push_control_handler!(handler)
        end
    end
end

function _start_push_heartbeat_monitor(psub::PushSubscription, handler::_JetStreamPushControlHandler)
    handler.idle_heartbeat[] > 0 || return nothing
    @async _push_heartbeat_monitor(psub, handler)
end

function next(psub::PushSubscription{C}; timeout::Real=1.0,
              cancel_token::MaybeCancellationToken=nothing)::JetStreamMsg{C} where {C<:Client}
    JetStreamMsg(next(psub.sub; timeout, cancel_token), psub.js.client)
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
                                  auto_ack::Bool; borrowed::Bool=false) where {C<:Client}
    isnothing(callback) && return nothing
    if borrowed
        return msg -> begin
            jsmsg = BorrowedJetStreamMsg(msg, js.client)
            callback(jsmsg)
            auto_ack && ack(jsmsg)
            nothing
        end
    end
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

function _reply_to_consumer_stalled!(sub::Subscription, msg::AbstractMsg)
    reply = header(msg, _JS_HEADER_CONSUMER_STALLED)
    isnothing(reply) && return nothing
    !isempty(reply) && _publish_flow_control_reply(sub, reply)
    nothing
end

function _schedule_or_reply_to_flow_control(sub::Subscription, handler::_JetStreamPushControlHandler, msg::AbstractMsg)
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

_header_int(msg::AbstractMsg, key::AbstractString)::Union{Int,Nothing} = begin
    value = header(msg, key)
    isnothing(value) && return nothing
    tryparse(Int, value)
end

function _request_ordered_push_reset!(handler::_JetStreamPushControlHandler, start_seq::Int)
    callback = @lock handler.lock begin
        handler.ordered || return nothing
        handler.ordered_resetting && return nothing
        handler.ordered_resetting = true
        handler.next_consumer_seq = 1
        handler.last_stream_seq = max(0, start_seq - 1)
        handler.sequence_state_anchored = false
        handler.flow_incoming[] = UInt64(0)
        handler.flow_delivered = UInt64(0)
        handler.flow_reply = nothing
        handler.flow_target = UInt64(0)
        handler.ordered_reset_callback
    end
    if isnothing(callback)
        @lock handler.lock handler.ordered_resetting = false
    else
        try
            callback(max(1, start_seq))
            return nothing
        catch err
            @lock handler.lock handler.ordered_resetting = false
            rethrow()
        end
    end
    nothing
end

function _handle_ordered_push_data!(handler::_JetStreamPushControlHandler, client::Client,
                                    msg::M)::Bool where {M<:AbstractMsg}
    handler.ordered || return false
    parsed = _push_msg_metadata(msg)
    isnothing(parsed) && return false
    reset_seq = @lock handler.lock begin
        handler.ordered || return 0
        if parsed.consumer_sequence != handler.next_consumer_seq
            handler.last_stream_seq + 1
        else
            handler.next_consumer_seq = parsed.consumer_sequence + 1
            handler.last_stream_seq = parsed.stream_sequence
            0
        end
    end
    reset_seq == 0 && return false
    try
        _request_ordered_push_reset!(handler, reset_seq)
    catch err
        _report_error(client, CleanupError("reset ordered push consumer", err))
    end
    true
end

_handle_ordered_push_data!(::_SubscriptionControlHandler, ::Client, ::AbstractMsg)::Bool = false

function _handle_push_sequence_heartbeat!(handler::_JetStreamPushControlHandler, sub::Subscription, msg::AbstractMsg)
    last_consumer = _header_int(msg, _JS_HEADER_LAST_CONSUMER)
    isnothing(last_consumer) && return nothing
    reset_seq = 0
    report_err::Union{ConsumerSequenceMismatchError,Nothing} = nothing
    have_sequence = false
    @lock handler.lock begin
        have_sequence = handler.last_stream_seq > 0
        if have_sequence
            delivered = handler.next_consumer_seq - 1
            if last_consumer != delivered
                if handler.ordered
                    last_consumer > delivered && (reset_seq = handler.last_stream_seq + 1)
                else
                    report_err = ConsumerSequenceMismatchError(max(1, handler.last_stream_seq),
                                                               delivered, last_consumer)
                end
            end
        end
    end
    have_sequence || return nothing
    isnothing(report_err) || return _report_error(sub.client, report_err)
    reset_seq == 0 && return nothing
    try
        _request_ordered_push_reset!(handler, reset_seq)
    catch err
        _report_error(sub.client, CleanupError("reset ordered push consumer after heartbeat gap", err))
    end
    nothing
end

struct _OrderedSubscriptionRemap
    old_sid::Int
    new_sid::Int
    old_subject::String
    new_subject::String
    old_server_active::Bool
end

function _remap_ordered_subscription!(sub::Subscription, deliver::String)::_OrderedSubscriptionRemap
    client = sub.client
    @lock sub.lock begin
        sub.closed && throw(ConnectionClosedError("subscription is closed"))
        @lock client.lock begin
            old_sid = sub.sid
            old_subject = sub.subject
            old_server_active = sub.server_active
            _delete_subscription_locked!(client, old_sid, sub)
            client.sid += 1
            new_sid = client.sid
            sub.sid = new_sid
            sub.subject = deliver
            sub.server_active = false
            client.subscriptions[new_sid] = sub
            _set_subscription_snapshot_locked!(client, new_sid, sub)
            _OrderedSubscriptionRemap(old_sid, new_sid, old_subject, deliver, old_server_active)
        end
    end
end

function _restore_ordered_subscription_mapping!(sub::Subscription, remap::_OrderedSubscriptionRemap;
                                                server_active::Bool=false)::Bool
    client = sub.client
    @lock sub.lock begin
        sub.closed && return false
        @lock client.lock begin
            sub.sid == remap.new_sid || return false
            get(client.subscriptions, remap.new_sid, nothing) === sub || return false
            _delete_subscription_locked!(client, remap.new_sid, sub)
            sub.sid = remap.old_sid
            sub.subject = remap.old_subject
            sub.server_active = server_active
            client.subscriptions[remap.old_sid] = sub
            _set_subscription_snapshot_locked!(client, remap.old_sid, sub)
            true
        end
    end
end

function _send_ordered_subscription_reset!(sub::Subscription, remap::_OrderedSubscriptionRemap)
    _send_raw(sub.client, string(_unsub_cmd(remap.old_sid),
                                 _sub_cmd(remap.new_subject, sub.queue, remap.new_sid));
              force_flush=true)
    @lock sub.lock begin
        if !sub.closed && sub.sid == remap.new_sid
            sub.server_delivered_base = sub.delivered
            sub.server_active = true
        end
    end
    nothing
end

function _rollback_ordered_subscription_reset!(sub::Subscription, remap::_OrderedSubscriptionRemap,
                                               reset_sent::Bool)
    restored = _restore_ordered_subscription_mapping!(
        sub, remap;
        server_active=!reset_sent && status(sub.client) == ConnectionStatus.CONNECTED &&
                      remap.old_server_active,
    )
    restored || return nothing
    reset_sent || return nothing
    _send_raw(sub.client, string(_unsub_cmd(remap.new_sid),
                                 _sub_cmd(remap.old_subject, sub.queue, remap.old_sid));
              force_flush=true)
    @lock sub.lock begin
        if !sub.closed && sub.sid == remap.old_sid && sub.subject == remap.old_subject
            sub.server_delivered_base = sub.delivered
            sub.server_active = true
        end
    end
    nothing
end

function _finish_ordered_reset!(handler::Union{_JetStreamPushControlHandler,Nothing})
    isnothing(handler) && return nothing
    @lock handler.lock handler.ordered_resetting = false
    nothing
end

function _ordered_delete_consumer_task(psub::PushSubscription, consumer::String)
    isempty(consumer) && return nothing
    try
        consumer_delete(psub.js, psub.stream, consumer; timeout=psub.js.timeout)
    catch err
        _consumer_missing(err) || _report_error(psub.js.client, CleanupError("delete ordered push consumer $consumer", err))
    end
    nothing
end

function _ordered_push_reset_task(psub::PushSubscription, base_config::Dict{String,Any}, start_seq::Int)
    handler = psub.control_handler
    remap = nothing
    reset_sent = false
    try
        (@lock psub.close_lock psub.closed) && return nothing
        old_consumer = psub.consumer
        deliver = new_inbox(psub.js.client)
        remap = _remap_ordered_subscription!(psub.sub, deliver)
        _send_ordered_subscription_reset!(psub.sub, remap)
        reset_sent = true

        name = @lock psub.js.client.lock randstring(psub.js.client.rng, 16)
        cfg = _ordered_push_reset_config(base_config, name, deliver, start_seq)
        info = _consumer_create_payload_request(psub.js, psub.stream, cfg; timeout=psub.js.timeout, action="create")
        closed = @lock psub.close_lock begin
            if !psub.closed
                psub.consumer = info.name
                psub.info = info
                false
            else
                true
            end
        end
        if closed
            try
                consumer_delete(psub.js, psub.stream, info.name; timeout=psub.js.timeout)
            catch err
                _consumer_missing(err) || _report_error(psub.js.client, CleanupError("delete closed ordered push consumer $(info.name)", err))
            end
            return nothing
        end
        @async _ordered_delete_consumer_task(psub, old_consumer)
    catch err
        if !isnothing(remap)
            try
                _rollback_ordered_subscription_reset!(psub.sub, remap, reset_sent)
            catch cleanup_err
                _report_error(psub.js.client,
                              CleanupError("rollback ordered push subscription reset", cleanup_err))
            end
        end
        _report_error(psub.js.client, CleanupError("reset ordered push consumer", err))
    finally
        _finish_ordered_reset!(handler)
    end
    nothing
end

function _schedule_ordered_push_reset!(psub::PushSubscription, base_config::Dict{String,Any}, start_seq::Int)
    psub.ordered_reset_task = @async _ordered_push_reset_task(psub, base_config, start_seq)
    nothing
end

function _stream_by_subject(js::JetStreamContext, subject::AbstractString; timeout::Real=js.timeout,
                            cancel_token::MaybeCancellationToken=nothing)
    subject = _validate_subject(subject)
    names = stream_names(js; subject, timeout, cancel_token)
    isempty(names) && throw(JetStreamError(404, nothing, "no stream found for subject $subject"))
    first(names)
end

function pull_subscribe(js::JetStreamContext, subject::AbstractString; stream::Union{AbstractString,Nothing}=nothing, durable::Union{AbstractString,Nothing}=nothing,
                        config::Union{ConsumerConfig,AbstractDict{String,<:Any}}=ConsumerConfig(),
                        timeout::Real=js.timeout, cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    subject = _validate_subject(subject)
    timeout = _positive_timeout_seconds("timeout", timeout)
    verify_config = config isa ConsumerConfig
    cfg = _js_config_payload(config)
    _validate_pull_consumer_config!(cfg)
    _validate_consumer_config_payload!(cfg)

    stream = isnothing(stream) ? _stream_by_subject(js, subject; timeout, cancel_token) : _validate_api_name("stream", stream)
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
            _consumer_create_payload_request(js, stream, cfg; timeout, action="create",
                                             verify_config, cancel_token)
        else
            consumer, _created = _bind_or_create_consumer(js, stream, bind_name, cfg, bind_fields;
                                                          timeout, verify_config, cancel_token)
            _validate_existing_pull_consumer(consumer)
        end
    if !haskey(cfg, "name") || isnothing(cfg["name"])
        cfg["name"] = info.name
    end
    try
        priority_policy, priority_groups = _validate_pull_consumer_priority_config(info)
        PullSubscription(js, stream, info.name, ReentrantLock(), ReentrantLock(),
                         delete_on_close, false, priority_policy, priority_groups)
    catch err
        if delete_on_close
            try
                consumer_delete(js, stream, info.name; timeout, cancel_token)
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

function _optional_positive_int_option(name::AbstractString, value)::Union{Int,Nothing}
    isnothing(value) && return nothing
    _positive_integer_option(name, value)
end

function _optional_pull_priority(value)::Union{Int,Nothing}
    isnothing(value) && return nothing
    _integer_range_option("pull priority", value, 0, 9, "an integer from 0 to 9")
end

function _validate_pull_priority_group(value)::Union{String,Nothing}
    isnothing(value) && return nothing
    group = _validate_queue(value)
    group
end

function _validate_pull_request_scheduling(prefix::AbstractString, min_pending, min_ack_pending,
                                           priority_group, priority)
    min_pending = _optional_positive_int_option("$prefix min_pending", min_pending)
    min_ack_pending = _optional_positive_int_option("$prefix min_ack_pending", min_ack_pending)
    priority_group = _validate_pull_priority_group(priority_group)
    priority = _optional_pull_priority(priority)
    if isnothing(priority_group) &&
       (!isnothing(min_pending) || !isnothing(min_ack_pending) || !isnothing(priority))
        throw(ArgumentError("$prefix priority_group is required with min_pending, min_ack_pending, or priority"))
    end
    min_pending, min_ack_pending, priority_group, priority
end

function _validate_pull_request_priority!(psub::PullSubscription, prefix::AbstractString,
                                          min_pending::Union{Int,Nothing},
                                          min_ack_pending::Union{Int,Nothing},
                                          priority_group::Union{String,Nothing},
                                          priority::Union{Int,Nothing})
    has_groups = !isempty(psub.priority_groups)
    if has_groups
        isnothing(priority_group) &&
            throw(ArgumentError("$prefix priority_group is required for priority consumer $(psub.consumer)"))
        priority_group in psub.priority_groups ||
            throw(ArgumentError("$prefix priority_group is not configured for consumer $(psub.consumer)"))
    elseif !isnothing(priority_group)
        throw(ArgumentError("$prefix priority_group is not supported for consumer $(psub.consumer)"))
    end

    if !isnothing(min_pending) || !isnothing(min_ack_pending)
        psub.priority_policy == PriorityPolicy.OVERFLOW ||
            throw(ArgumentError("$prefix min_pending and min_ack_pending require priority_policy=overflow"))
    end
    if !isnothing(priority)
        psub.priority_policy == PriorityPolicy.PRIORITIZED ||
            throw(ArgumentError("$prefix priority requires priority_policy=prioritized"))
    end
    nothing
end

function _bool_option(name::AbstractString, value)::Bool
    value isa Bool || throw(ArgumentError("$name must be a Bool"))
    value
end

function _check_pull_subscription_open(psub::PullSubscription)
    (@lock psub.close_lock psub.closed) && throw(ConnectionClosedError("pull subscription is closed"))
    nothing
end

function _begin_pull_fetch!(psub::PullSubscription)
    @lock psub.close_lock begin
        psub.closed && throw(ConnectionClosedError("pull subscription is closed"))
        psub.active_stream && throw(ArgumentError("pull subscription already has an active message stream"))
        psub.active_fetches += 1
    end
    nothing
end

function _end_pull_fetch!(psub::PullSubscription)
    @lock psub.close_lock begin
        psub.active_fetches = max(0, psub.active_fetches - 1)
    end
    nothing
end

function _begin_pull_stream!(psub::PullSubscription)
    @lock psub.close_lock begin
        psub.closed && throw(ConnectionClosedError("pull subscription is closed"))
        (psub.active_stream || psub.active_fetches > 0) &&
            throw(ArgumentError("pull subscription already has an active fetch or message stream"))
        psub.active_stream = true
    end
    nothing
end

function _end_pull_stream!(psub::PullSubscription)
    @lock psub.close_lock psub.active_stream = false
    nothing
end

function _validate_pull_fetch(psub::PullSubscription, batch, timeout::Real, expires::Real,
                              heartbeat::Union{Nothing,Real}, max_bytes, no_wait,
                              min_pending, min_ack_pending, priority_group, priority)
    batch = _positive_integer_option("fetch batch", batch)
    max_bytes = _optional_positive_int_option("fetch max_bytes", max_bytes)
    no_wait = _bool_option("fetch no_wait", no_wait)
    timeout = _positive_timeout_seconds("fetch timeout", timeout)
    expires = _positive_timeout_seconds("fetch expires", expires)
    timeout > expires || throw(ArgumentError("fetch timeout must be greater than expires"))
    min_pending, min_ack_pending, priority_group, priority =
        _validate_pull_request_scheduling("fetch", min_pending, min_ack_pending,
                                          priority_group, priority)
    _check_pull_subscription_open(psub)
    _validate_pull_request_priority!(psub, "fetch", min_pending, min_ack_pending,
                                     priority_group, priority)
    batch, timeout, expires, _pull_fetch_heartbeat(expires, heartbeat), max_bytes, no_wait,
        min_pending, min_ack_pending, priority_group, priority
end

function _throw_pull_fetch_wait_interrupted(closed::Bool, st::ConnectionStatus.T)
    st in (ConnectionStatus.RECONNECTING, ConnectionStatus.DISCONNECTED, ConnectionStatus.CONNECTING) &&
        throw(FetchDisconnectedError())
    st == ConnectionStatus.DRAINING && throw(ConnectionDrainingError())
    st == ConnectionStatus.CLOSED && throw(ConnectionClosedError())
    closed && throw(ConnectionClosedError("subscription is closed"))
    throw(TimeoutError("next message timed out"))
end

_saturating_add_int(a::Int, b::Int)::Int =
    a > typemax(Int) - b ? typemax(Int) : a + b

function _saturating_mul_int(a::Int, b::Int)::Int
    (a == 0 || b == 0) && return 0
    a > typemax(Int) ÷ b ? typemax(Int) : a * b
end

function _pull_delivery_pending_msgs_limit(client::Client, batch::Int)::Int
    requested = _saturating_add_int(batch, 8)
    min(max(requested, 8), client.options.sub_pending_msgs_limit)
end

function _pull_delivery_pending_bytes_limit(client::Client, pending_msgs_limit::Int,
                                            max_bytes::Union{Int,Nothing})::Int
    isnothing(max_bytes) && return client.options.sub_pending_bytes_limit
    header_budget = _saturating_mul_int(client.options.max_header_bytes, pending_msgs_limit)
    requested = _saturating_add_int(max_bytes::Int, header_budget)
    max(requested, client.options.sub_pending_bytes_limit)
end

function _pull_delivery_limits(client::Client, batch::Int,
                               max_bytes::Union{Int,Nothing})::Tuple{Int,Int}
    pending_msgs_limit = _pull_delivery_pending_msgs_limit(client, batch)
    pending_bytes_limit = _pull_delivery_pending_bytes_limit(client, pending_msgs_limit, max_bytes)
    pending_msgs_limit, pending_bytes_limit
end

function _pull_delivery_capacity_ok(sub::Subscription, pending_msgs_limit::Int,
                                    pending_bytes_limit::Int)::Bool
    sub.pending_msgs_limit >= pending_msgs_limit && sub.pending_bytes_limit >= pending_bytes_limit
end

function _pull_fetch_delivery_idle(sub::Subscription)::Bool
    @lock sub.lock !sub.closed && !isready(sub.messages) && sub.pending_bytes == 0
end

function _remove_active_pull_delivery_locked!(psub::PullSubscription, sub::Subscription)
    index = findfirst(active -> active === sub, psub.active_deliveries)
    isnothing(index) || deleteat!(psub.active_deliveries, index)
    nothing
end

function _register_pull_delivery!(psub::PullSubscription{C},
                                  sub::Subscription{C}) where {C<:Client}
    close_now = @lock psub.close_lock begin
        if psub.closed
            true
        else
            push!(psub.active_deliveries, sub)
            false
        end
    end
    if close_now
        closed_err = ConnectionClosedError("pull subscription is closed")
        try
            close(sub)
        catch cleanup_err
            throw(Base.CompositeException([closed_err,
                                           CleanupError("close pull delivery subscription", cleanup_err)]))
        end
        throw(closed_err)
    end
    nothing
end

function _unregister_pull_delivery!(psub::PullSubscription, sub::Subscription)
    @lock psub.close_lock begin
        _remove_active_pull_delivery_locked!(psub, sub)
    end
    nothing
end

function _subscribe_pull_delivery!(psub::PullSubscription{C}, batch::Int,
                                   max_bytes::Union{Int,Nothing};
                                   cancel_token::MaybeCancellationToken=nothing)::Subscription{C} where {C<:Client}
    client = psub.js.client
    pending_msgs_limit, pending_bytes_limit = _pull_delivery_limits(client, batch, max_bytes)
    sub = subscribe(client, new_inbox(client); pending_msgs_limit, pending_bytes_limit,
                    cancel_token)
    _register_pull_delivery!(psub, sub)
    sub
end

function _close_pull_delivery!(psub::PullSubscription, sub::Subscription,
                               label::AbstractString="close pull delivery subscription")
    _unregister_pull_delivery!(psub, sub)
    try
        close(sub)
    catch err
        throw(CleanupError(label, err))
    end
    nothing
end

function _acquire_pull_fetch_delivery!(psub::PullSubscription{C}, batch::Int,
                                       max_bytes::Union{Int,Nothing};
                                       cancel_token::MaybeCancellationToken=nothing)::Tuple{Subscription{C},Bool} where {C<:Client}
    client = psub.js.client
    pending_msgs_limit, pending_bytes_limit = _pull_delivery_limits(client, batch, max_bytes)
    retire::Union{Subscription{C},Nothing} = nothing
    cached::Union{Subscription{C},Nothing} = @lock psub.close_lock begin
        psub.closed && throw(ConnectionClosedError("pull subscription is closed"))
        sub = psub.fetch_delivery
        if !psub.fetch_delivery_in_use && !isnothing(sub)
            if _pull_delivery_capacity_ok(sub, pending_msgs_limit, pending_bytes_limit)
                psub.fetch_delivery_in_use = true
                sub
            else
                psub.fetch_delivery = nothing
                _remove_active_pull_delivery_locked!(psub, sub)
                retire = sub
                nothing
            end
        else
            nothing
        end
    end
    if !isnothing(cached)
        return cached, true
    end
    if !isnothing(retire)
        try
            close(retire)
        catch err
            throw(CleanupError("close undersized pull fetch delivery subscription", err))
        end
    end

    sub = subscribe(client, new_inbox(client); pending_msgs_limit, pending_bytes_limit,
                    cancel_token)
    close_now = false
    use_cached = false
    @lock psub.close_lock begin
        if psub.closed
            close_now = true
        elseif isnothing(psub.fetch_delivery) && !psub.fetch_delivery_in_use
            psub.fetch_delivery = sub
            psub.fetch_delivery_in_use = true
            push!(psub.active_deliveries, sub)
            use_cached = true
        else
            push!(psub.active_deliveries, sub)
        end
    end
    if close_now
        closed_err = ConnectionClosedError("pull subscription is closed")
        try
            close(sub)
        catch cleanup_err
            throw(Base.CompositeException([closed_err,
                                           CleanupError("close pull delivery subscription", cleanup_err)]))
        end
        throw(closed_err)
    end
    sub, use_cached
end

function _release_pull_fetch_delivery!(psub::PullSubscription, sub::Subscription,
                                       cached::Bool, reusable::Bool,
                                       label::AbstractString="close pull fetch delivery subscription")
    if cached
        keep_cached = reusable && _pull_fetch_delivery_idle(sub)
        close_now = @lock psub.close_lock begin
            if psub.fetch_delivery === sub && keep_cached && !psub.closed
                psub.fetch_delivery_in_use = false
                false
            else
                psub.fetch_delivery === sub && (psub.fetch_delivery = nothing)
                psub.fetch_delivery_in_use = false
                _remove_active_pull_delivery_locked!(psub, sub)
                true
            end
        end
        if close_now
            try
                close(sub)
            catch err
                throw(CleanupError(label, err))
            end
        end
        return nothing
    end
    _close_pull_delivery!(psub, sub, label)
end

function _next_pull_fetch_msg(sub::Subscription, timeout::Real;
                              cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    client = sub.client
    deadline = time() + Float64(timeout)
    while true
        ready, msg = _take_subscription_msg_ready!(sub)
        ready && return msg

        closed, empty = @lock sub.lock (sub.closed, !isready(sub.messages))
        st = status(client)
        closed && empty && _throw_pull_fetch_wait_interrupted(closed, st)
        ready = @lock sub.lock begin
            _wait_until_condition_locked(sub.condition, _remaining_timeout(deadline); cancel_token) do
                isready(sub.messages) || sub.closed || status(client) != ConnectionStatus.CONNECTED
            end
        end
        ready || throw(TimeoutError("next message timed out"))
        ready, msg = _take_subscription_msg_ready!(sub)
        ready && return msg
        closed = @lock sub.lock sub.closed
        st = status(client)
        (closed || st != ConnectionStatus.CONNECTED) && _throw_pull_fetch_wait_interrupted(closed, st)
    end
end

function _publish_pull_fetch_request(psub::PullSubscription, request_subject::String,
                                     payload::AbstractVector{UInt8}, reply::String;
                                     cancel_token::MaybeCancellationToken=nothing)
    try
        _publish(psub.js.client, request_subject, payload; reply,
                 buffer_on_reconnect=false, force_flush=true, cancel_token)
    catch err
        if err isa ConnectionReconnectingError ||
           (err isa ConnectionClosedError && status(psub.js.client) == ConnectionStatus.DISCONNECTED)
            throw(FetchDisconnectedError())
        end
        rethrow()
    end
    nothing
end

function _prepare_pull_fetch_request!(psub::PullSubscription, request_subject::String,
                                      batch::Int, expires_ns::Int, heartbeat_ns::Int,
                                      max_bytes::Union{Int,Nothing}, no_wait::Bool,
                                      min_pending::Union{Int,Nothing},
                                      min_ack_pending::Union{Int,Nothing},
                                      priority_group::Union{String,Nothing},
                                      priority::Union{Int,Nothing}, reply::String,
                                      buffer::Vector{UInt8};
                                      cancel_token::MaybeCancellationToken=nothing)
    _check_pull_subscription_open(psub)
    pin_id = @lock psub.fetch_lock psub.pin_id
    payload = _pull_fetch_request_payload!(buffer, batch, expires_ns, heartbeat_ns, max_bytes,
                                           no_wait, pin_id, min_pending, min_ack_pending,
                                           priority_group, priority)
    _publish_pull_fetch_request(psub, request_subject, payload, reply; cancel_token)
    nothing
end

function fetch(psub::PullSubscription{C}, batch=1; timeout::Real=psub.js.timeout,
               expires::Real=_pull_fetch_default_expires(timeout),
               heartbeat::Union{Nothing,Real}=nothing, max_bytes=nothing,
               no_wait=false, min_pending=nothing, min_ack_pending=nothing,
               priority_group=nothing, priority=nothing,
               cancel_token::MaybeCancellationToken=nothing) where {C}
    _throw_if_cancelled(cancel_token)
    batch, timeout_seconds, expires_seconds, heartbeat_seconds, max_bytes_int, no_wait_bool,
        min_pending, min_ack_pending, priority_group, priority =
        _validate_pull_fetch(psub, batch, timeout, expires, heartbeat, max_bytes, no_wait,
                             min_pending, min_ack_pending, priority_group, priority)
    _begin_pull_fetch!(psub)
    delivery::Union{Subscription{C},Nothing} = nothing
    cached_delivery::Bool = false
    reusable_delivery::Bool = false
    result = JetStreamMsg{C}[]
    primary_error = nothing
    try
        delivery, cached_delivery = _acquire_pull_fetch_delivery!(psub, batch, max_bytes_int;
                                                                  cancel_token)
        request_subject = psub.next_subject
        reply = delivery.subject
        heartbeat_ns = heartbeat_seconds > 0 ? _seconds_to_nanoseconds(heartbeat_seconds) : 0
        payload_buffer = UInt8[]
        _prepare_pull_fetch_request!(psub, request_subject, batch,
                                     _seconds_to_nanoseconds(expires_seconds),
                                     heartbeat_ns, max_bytes_int, no_wait_bool,
                                     min_pending, min_ack_pending,
                                     priority_group, priority, reply, payload_buffer;
                                     cancel_token)
        sizehint!(result, _pull_delivery_pending_msgs_limit(psub.js.client, batch))
        deadline = time() + timeout_seconds
        heartbeat_deadline = heartbeat_seconds > 0 ? time() + 2 * heartbeat_seconds : Inf
        while length(result) < batch && time() < deadline
            wait_deadline = min(deadline, heartbeat_deadline)
            remaining = max(0.001, wait_deadline - time())
            try
                msg = _next_pull_fetch_msg(delivery, remaining; cancel_token)
                action, err = _jetstream_status_action(msg, request_subject)
                heartbeat_seconds > 0 && (heartbeat_deadline = time() + 2 * heartbeat_seconds)
                pin_id = header(msg, "Nats-Pin-Id")
                !isnothing(pin_id) && !isempty(pin_id) && (@lock psub.fetch_lock psub.pin_id = pin_id)
                if action in (:idle_heartbeat, :flow_control, :control)
                    continue
                elseif action in (:no_messages, :timeout, :batch_completed, :max_bytes_exceeded)
                    reusable_delivery = true
                    break
                elseif action == :message
                    push!(result, JetStreamMsg(msg, psub.js.client))
                else
                    action == :consumer_deleted && (@lock psub.close_lock psub.server_deleted = true)
                    action == :pin_id_mismatch && (@lock psub.fetch_lock psub.pin_id = nothing)
                    throw(err)
                end
            catch err
                if err isa TimeoutError
                    if heartbeat_seconds > 0 && time() < deadline && time() >= heartbeat_deadline
                        throw(_jetstream_heartbeat_error())
                    end
                    break
                elseif err isa FetchDisconnectedError
                    isempty(result) || break
                end
                rethrow()
            end
        end
        length(result) == batch && (reusable_delivery = true)
    catch err
        primary_error = err
    finally
        if !isnothing(delivery)
            try
                _release_pull_fetch_delivery!(psub, delivery, cached_delivery,
                                              isnothing(primary_error) && reusable_delivery,
                                              "close pull fetch delivery subscription")
            catch cleanup_err
                primary_error = isnothing(primary_error) ? cleanup_err :
                                Base.CompositeException([primary_error, cleanup_err])
            end
        end
        _end_pull_fetch!(psub)
    end
    isnothing(primary_error) || throw(primary_error)
    result
end

struct _PullStreamClosed <: Exception end

function _pull_stream_closed(state::_PullMessageStreamState)::Bool
    @lock state.lock state.closed
end

function _pull_stream_error(state::_PullMessageStreamState)
    @lock state.lock state.error
end

function _pull_stream_set_error!(state::_PullMessageStreamState, err)
    exception = err isa Exception ? err : ErrorException(string(err))
    @lock state.lock begin
        isnothing(state.error) && (state.error = exception)
        state.closed = true
        _pull_stream_clear_requests!(state)
    end
    nothing
end

function _pull_stream_close_state!(state::_PullMessageStreamState)
    @lock state.lock begin
        was_closed = state.closed
        state.closed = true
        _pull_stream_clear_requests!(state)
        was_closed
    end
end

function _pull_stream_threshold(name::AbstractString, value, limit::Int)::Int
    threshold = _positive_integer_option(name, value)
    threshold <= limit || throw(ArgumentError("$name must not exceed its request limit"))
    threshold
end

function _validate_pull_messages(psub::PullSubscription, batch, max_bytes, expires::Real,
                                 heartbeat::Union{Nothing,Real}, threshold_messages,
                                 threshold_bytes, channel_size, stop_after,
                                 min_pending, min_ack_pending, priority_group,
                                 priority)::Tuple{_PullStreamConfig,Int}
    batch = _positive_integer_option("messages batch", batch)
    max_bytes = _optional_positive_int_option("messages max_bytes", max_bytes)
    expires = _positive_timeout_seconds("messages expires", expires)
    heartbeat = _pull_fetch_heartbeat(expires, heartbeat)
    channel_size = _positive_integer_option("messages channel_size", channel_size)
    threshold_messages =
        isnothing(threshold_messages) ? max(1, min(batch, channel_size) ÷ 2) :
        _pull_stream_threshold("messages threshold_messages", threshold_messages, channel_size)
    if isnothing(max_bytes)
        isnothing(threshold_bytes) ||
            throw(ArgumentError("messages threshold_bytes requires max_bytes"))
        threshold_bytes = nothing
    else
        threshold_bytes =
            isnothing(threshold_bytes) ? max(1, max_bytes ÷ 2) :
            _pull_stream_threshold("messages threshold_bytes", threshold_bytes, max_bytes)
    end
    stop_after = _optional_positive_int_option("messages stop_after", stop_after)
    min_pending, min_ack_pending, priority_group, priority =
        _validate_pull_request_scheduling("messages", min_pending, min_ack_pending,
                                          priority_group, priority)
    _check_pull_subscription_open(psub)
    _validate_pull_request_priority!(psub, "messages", min_pending, min_ack_pending,
                                     priority_group, priority)
    _PullStreamConfig(batch, max_bytes, expires, heartbeat, threshold_messages,
                      threshold_bytes, min_pending, min_ack_pending, priority_group,
                      priority, stop_after, channel_size), channel_size
end

function _next_pull_stream_msg(stream::PullMessageStream, timeout::Real)
    sub = stream.delivery
    client = sub.client
    deadline = time() + Float64(timeout)
    while true
        _pull_stream_closed(stream.state) && throw(_PullStreamClosed())
        ready, msg = _take_subscription_msg_ready!(sub)
        ready && return msg

        closed, empty = @lock sub.lock (sub.closed, !isready(sub.messages))
        st = status(client)
        closed && empty && _throw_pull_fetch_wait_interrupted(closed, st)
        ready = @lock sub.lock begin
            _wait_until_condition_locked(sub.condition, _remaining_timeout(deadline)) do
                isready(sub.messages) || sub.closed || status(client) != ConnectionStatus.CONNECTED ||
                    _pull_stream_closed(stream.state)
            end
        end
        ready || throw(TimeoutError("next message timed out"))
        _pull_stream_closed(stream.state) && throw(_PullStreamClosed())
        ready, msg = _take_subscription_msg_ready!(sub)
        ready && return msg
        closed = @lock sub.lock sub.closed
        st = status(client)
        (closed || st != ConnectionStatus.CONNECTED) && _throw_pull_fetch_wait_interrupted(closed, st)
    end
end

function _pull_stream_pending_messages(state::_PullMessageStreamState)::Int
    state.buffered_messages + state.requested_messages
end

function _pull_stream_pending_bytes(state::_PullMessageStreamState)::Int
    state.buffered_bytes + state.requested_bytes
end

function _pull_stream_request_batch(config::_PullStreamConfig,
                                    state::_PullMessageStreamState)::Int
    requested = state.requested_messages
    available = config.channel_size - state.buffered_messages - requested
    available > 0 || return 0
    batch = min(config.batch, available)
    if !isnothing(config.stop_after)
        remaining = (config.stop_after::Int) - state.delivered - requested
        remaining > 0 || return 0
        batch = min(batch, remaining)
    end
    batch
end

_pull_stream_reserved_bytes(max_bytes::Union{Int,Nothing})::Int =
    isnothing(max_bytes) ? 0 : max_bytes::Int

function _pull_stream_reserve_counters!(state::_PullMessageStreamState,
                                        request::_PullStreamRequest)
    state.requested_messages += request.remaining_messages
    state.requested_bytes += request.remaining_bytes
    nothing
end

function _pull_stream_release_counters!(state::_PullMessageStreamState,
                                        messages::Int, bytes::Int)
    state.requested_messages = max(0, state.requested_messages - messages)
    state.requested_bytes = max(0, state.requested_bytes - bytes)
    nothing
end

function _pull_stream_request_max_bytes(config::_PullStreamConfig,
                                        state::_PullMessageStreamState)::Union{Int,Nothing}
    isnothing(config.max_bytes) && return nothing
    remaining = (config.max_bytes::Int) - _pull_stream_pending_bytes(state)
    remaining > 0 ? remaining : 0
end

function _pull_stream_should_refill(config::_PullStreamConfig,
                                    state::_PullMessageStreamState)::Bool
    _pull_stream_pending_messages(state) <= config.threshold_messages && return true
    !isnothing(config.threshold_bytes) &&
        _pull_stream_pending_bytes(state) <= (config.threshold_bytes::Int)
end

function _reserve_pull_stream_request!(config::_PullStreamConfig,
                                       state::_PullMessageStreamState)::Union{_PullStreamReservation,Nothing}
    @lock state.lock begin
        state.closed && return nothing
        _pull_stream_should_refill(config, state) || return nothing
        batch = _pull_stream_request_batch(config, state)
        batch > 0 || return nothing
        max_bytes = _pull_stream_request_max_bytes(config, state)
        max_bytes === 0 && return nothing
        request = _PullStreamRequest(batch, _pull_stream_reserved_bytes(max_bytes))
        push!(state.requests, request)
        _pull_stream_reserve_counters!(state, request)
        _PullStreamReservation(request, batch, max_bytes)
    end
end

function _pull_stream_request_index(requests::Vector{_PullStreamRequest},
                                    request::_PullStreamRequest)::Int
    @inbounds for i in eachindex(requests)
        requests[i] === request && return i
    end
    0
end

function _unreserve_pull_stream_request!(state::_PullMessageStreamState, request::_PullStreamRequest)
    @lock state.lock begin
        index = _pull_stream_request_index(state.requests, request)
        if index != 0
            deleteat!(state.requests, index)
            _pull_stream_release_counters!(state, request.remaining_messages, request.remaining_bytes)
        end
    end
    nothing
end

function _publish_pull_stream_request!(stream::PullMessageStream, config::_PullStreamConfig,
                                       state::_PullMessageStreamState)::Bool
    psub = stream.subscription
    @lock psub.fetch_lock begin
        reservation = _reserve_pull_stream_request!(config, state)
        isnothing(reservation) && return false
        heartbeat_ns = config.heartbeat > 0 ? _seconds_to_nanoseconds(config.heartbeat) : 0
        payload = _pull_fetch_request_payload!(stream.payload_buffer, reservation.batch,
                                               _seconds_to_nanoseconds(config.expires),
                                               heartbeat_ns, reservation.max_bytes, false, psub.pin_id,
                                               config.min_pending, config.min_ack_pending,
                                               config.priority_group, config.priority)
        try
            _publish_pull_fetch_request(psub, psub.next_subject, payload, stream.delivery.subject)
        catch
            _unreserve_pull_stream_request!(state, reservation.request)
            rethrow()
        end
    end
    true
end

function _pull_stream_msg_bytes(msg::AbstractMsg)::Int
    max(1, msg.header_bytes + length(msg.data))
end

function _pull_stream_decrement_requested!(state::_PullMessageStreamState, msg::Msg)
    bytes = _pull_stream_msg_bytes(msg)
    _pull_stream_release_counters!(state, 1, bytes)
    if !isempty(state.requests)
        request = first(state.requests)
        request.remaining_messages = max(0, request.remaining_messages - 1)
        request.remaining_bytes = max(0, request.remaining_bytes - bytes)
        request.remaining_messages == 0 && deleteat!(state.requests, 1)
    end
    nothing
end

function _pull_stream_clear_requests!(state::_PullMessageStreamState)
    empty!(state.requests)
    state.requested_messages = 0
    state.requested_bytes = 0
    nothing
end

function _pull_stream_pending_header_value(msg::Msg, name::AbstractString)::Union{Int,Nothing}
    value = header(msg, name)
    (isnothing(value) || isempty(value)) && return nothing
    parsed = tryparse(Int, value)
    isnothing(parsed) && throw(ProtocolError("invalid $name header: $value"))
    parsed >= 0 || throw(ProtocolError("invalid $name header: $value"))
    parsed
end

_pull_stream_pending_header(msg::Msg, name::AbstractString)::Int =
    something(_pull_stream_pending_header_value(msg, name), 0)

function _pull_stream_status_pending(msg::Msg)::Tuple{Int,Int}
    _pull_stream_pending_header(msg, "Nats-Pending-Messages"),
        _pull_stream_pending_header(msg, "Nats-Pending-Bytes")
end

function _pull_stream_release_terminal_request!(state::_PullMessageStreamState,
                                                msg::Msg)::Bool
    isempty(state.requests) && return false
    request = popfirst!(state.requests)
    messages = something(_pull_stream_pending_header_value(msg, "Nats-Pending-Messages"),
                         request.remaining_messages)
    bytes = something(_pull_stream_pending_header_value(msg, "Nats-Pending-Bytes"),
                      request.remaining_bytes)
    _pull_stream_release_counters!(state, messages, bytes)
    true
end

function _pull_stream_maybe_refill!(stream::PullMessageStream, config::_PullStreamConfig=stream.config)
    _publish_pull_stream_request!(stream, config, stream.state)
    nothing
end

function _pull_stream_put!(stream::PullMessageStream, msg::Msg)::Int
    bytes = _pull_stream_msg_bytes(msg)
    @lock stream.message_lock begin
        while isopen(stream.messages) && length(stream.messages) >= _queue_capacity(stream.messages)
            _pull_stream_closed(stream.state) && throw(_PullStreamClosed())
            wait(stream.message_condition)
        end
        isopen(stream.messages) || throw(_PullStreamClosed())
        @lock stream.state.lock begin
            stream.state.closed && throw(_PullStreamClosed())
            requested_messages_before = stream.state.requested_messages
            requested_bytes_before = stream.state.requested_bytes
            _pull_stream_release_counters!(stream.state, 1, bytes)
            stream.state.buffered_messages += 1
            stream.state.buffered_bytes += bytes
            stream.state.delivered += 1
            try
                put!(stream.messages, msg)
            catch
                stream.state.requested_messages = requested_messages_before
                stream.state.requested_bytes = requested_bytes_before
                stream.state.buffered_messages = max(0, stream.state.buffered_messages - 1)
                stream.state.buffered_bytes = max(0, stream.state.buffered_bytes - bytes)
                stream.state.delivered = max(0, stream.state.delivered - 1)
                rethrow()
            end
        end
        notify(stream.message_condition)
    end
    1
end

function _pull_stream_loop(stream::PullMessageStream, config::_PullStreamConfig)
    psub = stream.subscription
    local_timeout = config.expires + min(config.expires * 0.1, 5.0)
    heartbeat_deadline = config.heartbeat > 0 ? time() + 2 * config.heartbeat : Inf
    try
        while !_pull_stream_closed(stream.state)
            if !isnothing(config.stop_after) &&
               (@lock stream.state.lock stream.state.delivered >= (config.stop_after::Int))
                break
            end
            _pull_stream_maybe_refill!(stream, config)
            wait_deadline = min(time() + local_timeout, heartbeat_deadline)
            msg = try
                _next_pull_stream_msg(stream, max(0.001, wait_deadline - time()))
            catch err
                if err isa TimeoutError
                    if config.heartbeat > 0 && time() >= heartbeat_deadline
                        throw(_jetstream_heartbeat_error())
                    end
                    @lock stream.state.lock _pull_stream_clear_requests!(stream.state)
                    continue
                elseif err isa _PullStreamClosed
                    break
                end
                rethrow()
            end

            action, err = _jetstream_status_action(msg, psub.next_subject)
            if action != :message
                has_request = @lock stream.state.lock !isempty(stream.state.requests)
                has_request || continue
                config.heartbeat > 0 && (heartbeat_deadline = time() + 2 * config.heartbeat)
                pin_id = header(msg, "Nats-Pin-Id")
                !isnothing(pin_id) && !isempty(pin_id) && (@lock psub.fetch_lock psub.pin_id = pin_id)
                if action in (:idle_heartbeat, :flow_control, :control)
                    continue
                elseif action in (:no_messages, :timeout, :batch_completed, :max_bytes_exceeded)
                    released = @lock stream.state.lock _pull_stream_release_terminal_request!(stream.state, msg)
                    released || continue
                    _pull_stream_maybe_refill!(stream, config)
                    continue
                else
                    action == :consumer_deleted && (@lock psub.close_lock psub.server_deleted = true)
                    action == :pin_id_mismatch && (@lock psub.fetch_lock psub.pin_id = nothing)
                    throw(err)
                end
            end

            config.heartbeat > 0 && (heartbeat_deadline = time() + 2 * config.heartbeat)
            pin_id = header(msg, "Nats-Pin-Id")
            !isnothing(pin_id) && !isempty(pin_id) && (@lock psub.fetch_lock psub.pin_id = pin_id)
            _pull_stream_put!(stream, msg)
            _pull_stream_maybe_refill!(stream, config)
        end
    catch err
        err isa _PullStreamClosed && return nothing
        if _pull_stream_closed(stream.state) && err isa InvalidStateException
            return nothing
        end
        _pull_stream_set_error!(stream.state, err)
        rethrow()
    finally
        try
            _close_pull_delivery!(psub, stream.delivery, "close pull stream delivery subscription")
        catch cleanup_err
            _pull_stream_set_error!(stream.state, cleanup_err)
        end
        _pull_stream_close_state!(stream.state)
        @lock stream.message_lock begin
            isopen(stream.messages) && close(stream.messages)
            notify(stream.message_condition; all=true)
        end
        _end_pull_stream!(psub)
    end
    nothing
end

function messages(psub::PullSubscription{C}; batch=100, max_bytes=nothing,
                  expires::Real=30.0, heartbeat::Union{Nothing,Real}=nothing,
                  threshold_messages=nothing, threshold_bytes=nothing,
                  channel_size=batch, stop_after=nothing,
                  min_pending=nothing, min_ack_pending=nothing,
                  priority_group=nothing, priority=nothing,
                  cancel_token::MaybeCancellationToken=nothing) where {C}
    _throw_if_cancelled(cancel_token)
    config, channel_size = _validate_pull_messages(psub, batch, max_bytes, expires, heartbeat,
                                                   threshold_messages, threshold_bytes,
                                                   channel_size, stop_after,
                                                   min_pending, min_ack_pending,
                                                   priority_group, priority)
    _begin_pull_stream!(psub)
    delivery = nothing
    try
        delivery_batch = _saturating_add_int(channel_size, config.batch)
        delivery = _subscribe_pull_delivery!(psub, delivery_batch, config.max_bytes; cancel_token)
        state = _PullMessageStreamState()
        queue_lock = ReentrantLock()
        queue_condition = Base.Threads.Condition(queue_lock)
        queue = MsgQueue{Msg}(channel_size)
        stream = PullMessageStream{C,typeof(psub)}(psub, delivery, queue, queue_lock,
                                                   queue_condition, config, UInt8[],
                                                   Task(() -> nothing), nothing, state)
        task = @async _pull_stream_loop(stream, config)
        stream.task = task
        stream
    catch err
        cleanup_error = nothing
        if !isnothing(delivery)
            try
                _close_pull_delivery!(psub, delivery, "close pull stream delivery subscription")
            catch cleanup_err
                cleanup_error = cleanup_err
            end
        end
        _end_pull_stream!(psub)
        isnothing(cleanup_error) || throw(Base.CompositeException([err, cleanup_error]))
        rethrow()
    end
end

function _pull_consume_callback_loop(stream::PullMessageStream, callback)
    try
        for msg in stream
            callback(msg)
        end
    catch err
        _pull_stream_set_error!(stream.state, err)
        close(stream)
        rethrow()
    end
    nothing
end

function consume(callback, psub::PullSubscription; kwargs...)
    stream = messages(psub; kwargs...)
    stream.callback_task = @async _pull_consume_callback_loop(stream, callback)
    stream
end

function Base.close(stream::PullMessageStream; timeout::Real=stream.subscription.js.timeout,
                    cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    timeout = _positive_timeout_seconds("timeout", timeout)
    deadline = time() + timeout
    already_closed = _pull_stream_close_state!(stream.state)
    errors = Any[]
    if !already_closed
        try
            _close_pull_delivery!(stream.subscription, stream.delivery,
                                  "close pull stream delivery subscription")
        catch err
            push!(errors, err)
        end
    end
    @lock stream.message_lock begin
        isopen(stream.messages) && close(stream.messages)
        notify(stream.message_condition; all=true)
    end
    _wait_task!(errors, "stop pull message stream $(stream.subscription.consumer)",
                stream.task; timeout, interrupt=true, deadline)
    callback_task = stream.callback_task
    if !isnothing(callback_task) && callback_task !== current_task()
        _wait_task!(errors, "stop pull consume callback $(stream.subscription.consumer)",
                    callback_task; timeout, interrupt=true, deadline)
    end
    err = _pull_stream_error(stream.state)
    isnothing(err) || push!(errors, err)
    _throw_errors(errors)
    nothing
end

function Base.take!(stream::PullMessageStream)
    while true
        msg = EMPTY_MSG
        ready = @lock stream.message_lock begin
            while !isready(stream.messages) && isopen(stream.messages)
                wait(stream.message_condition)
            end
            if isready(stream.messages)
                msg = take!(stream.messages)
                @lock stream.state.lock begin
                    stream.state.buffered_messages = max(0, stream.state.buffered_messages - 1)
                    stream.state.buffered_bytes = max(0, stream.state.buffered_bytes - _pull_stream_msg_bytes(msg))
                end
                notify(stream.message_condition)
                true
            else
                false
            end
        end
        if ready
            jsmsg = JetStreamMsg(msg, stream.subscription.js.client)
            try
                _pull_stream_maybe_refill!(stream)
            catch err
                _pull_stream_set_error!(stream.state, err)
                _notify_subscription_waiters!(stream.delivery; all=true)
                @lock stream.message_lock begin
                    isopen(stream.messages) && close(stream.messages)
                    notify(stream.message_condition; all=true)
                end
            end
            return jsmsg
        end
        err = _pull_stream_error(stream.state)
        isnothing(err) || throw(err)
        throw(InvalidStateException("pull message stream is closed", :closed))
    end
end

function Base.iterate(stream::PullMessageStream, state=nothing)
    try
        take!(stream), nothing
    catch err
        queue_closed = @lock stream.message_lock !isopen(stream.messages)
        err isa InvalidStateException && queue_closed && return nothing
        rethrow()
    end
end

function Base.wait(stream::PullMessageStream)
    try
        wait(stream.task)
    catch err
        err isa TaskFailedException && err.task === stream.task || rethrow()
    end
    task = stream.callback_task
    if !isnothing(task)
        try
            wait(task)
        catch err
            err isa TaskFailedException && err.task === task || rethrow()
        end
    end
    err = _pull_stream_error(stream.state)
    isnothing(err) || throw(err)
    stream
end

Base.fetch(stream::PullMessageStream) = wait(stream)
Base.isopen(stream::PullMessageStream) =
    (@lock stream.message_lock isopen(stream.messages)) && !_pull_stream_closed(stream.state)

function _push_subscribe(js::JetStreamContext, subject::AbstractString; stream::Union{AbstractString,Nothing}=nothing, durable::Union{AbstractString,Nothing}=nothing,
                         queue::Union{AbstractString,Nothing}=nothing, callback=nothing, manual_ack::Bool=false,
                         config::Union{ConsumerConfig,AbstractDict{String,<:Any}}=ConsumerConfig(),
                         timeout::Real=js.timeout, ordered::Bool=false, borrowed::Bool=false,
                         cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    subject = _validate_subject(subject)
    timeout = _positive_timeout_seconds("timeout", timeout)
    borrowed = _connect_option_bool("borrowed", borrowed)
    borrowed && isnothing(callback) &&
        throw(ArgumentError("borrowed push subscriptions require a callback"))
    verify_config = config isa ConsumerConfig
    cfg = _js_config_payload(config)
    queue_explicit = !isnothing(queue)
    local_queue = _resolve_push_queue!(cfg, queue)
    ordered && _prepare_ordered_push_consumer_config!(cfg, local_queue)
    _validate_push_queue_control_config!(cfg, local_queue)
    _validate_push_consumer_control_config!(cfg)
    _validate_consumer_config_payload!(cfg)
    bind_fields = Set{String}(keys(cfg))
    !isnothing(queue) && push!(bind_fields, "deliver_group")

    stream = isnothing(stream) ? _stream_by_subject(js, subject; timeout, cancel_token) : _validate_api_name("stream", stream)
    durable = isnothing(durable) ? nothing : _validate_api_name("consumer", durable)
    ordered && !isnothing(durable) && throw(ArgumentError("ordered push consumers do not support durable consumers"))
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
    ordered && !isnothing(bind_name) && throw(ArgumentError("ordered push consumers cannot bind existing consumers"))
    delete_on_close = isnothing(bind_name)
    ordered_base_config = ordered ? _copy_config_payload(cfg) : nothing
    info::Union{ConsumerInfo,Nothing} = nothing
    create_consumer = false
    if isnothing(bind_name)
        _set_config_default!(cfg, "name", @lock js.client.lock randstring(js.client.rng, 16))
        _set_config_default!(cfg, "deliver_subject", deliver)
        create_consumer = true
    else
        existing = _consumer_info_or_nothing(js, stream, bind_name; timeout, cancel_token)
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
    wrapped_callback = _jetstream_push_callback(js, callback, auto_ack; borrowed)
    sub = subscribe(js.client, deliver_subject; queue=local_queue, callback=wrapped_callback,
                    borrowed=!isnothing(wrapped_callback) && borrowed,
                    _control_handler=control_handler, cancel_token)
    consumer_created = false
    try
        if create_consumer
            try
                info = _consumer_create_payload_request(js, stream, cfg; timeout, action="create",
                                                        verify_config, cancel_token)
                consumer_created = true
            catch err
                (!isnothing(bind_name) && _consumer_create_conflict(err)) || rethrow()
                try
                    close(sub)
                catch cleanup_err
                    throw(Base.CompositeException([err, CleanupError("close provisional push subscription $deliver_subject", cleanup_err)]))
                end
                existing = consumer_info(js, stream, bind_name; timeout, cancel_token)
                info = _bind_existing_push_consumer!(existing, cfg, bind_fields, local_queue, queue_explicit)
                deliver_subject = String(cfg["deliver_subject"])
                retry_auto_ack = _push_callback_auto_ack(manual_ack, callback, info)
                wrapped_callback = _jetstream_push_callback(js, callback, retry_auto_ack; borrowed)
                sub = subscribe(js.client, deliver_subject; queue=local_queue, callback=wrapped_callback,
                                borrowed=!isnothing(wrapped_callback) && borrowed,
                                _control_handler=control_handler, cancel_token)
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
                                false, nothing, nothing, control_handler, info)
        if ordered
            base_config = ordered_base_config::Dict{String,Any}
            @lock control_handler.lock begin
                control_handler.ordered = true
                control_handler.next_consumer_seq = 1
                control_handler.last_stream_seq = 0
                control_handler.sequence_state_anchored = false
                control_handler.ordered_resetting = false
                control_handler.ordered_reset_callback =
                    start_seq -> _schedule_ordered_push_reset!(psub, base_config, start_seq)
            end
        end
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
                consumer_delete(js, stream, (info::ConsumerInfo).name; timeout, cancel_token)
            catch cleanup_err
                push!(cleanup_errors, CleanupError("delete push consumer $((info::ConsumerInfo).name)", cleanup_err))
            end
        end
        isempty(cleanup_errors) ? rethrow() : throw(Base.CompositeException(vcat(Any[err], cleanup_errors)))
    end
end

function push_subscribe(js::JetStreamContext, subject::AbstractString; stream::Union{AbstractString,Nothing}=nothing, durable::Union{AbstractString,Nothing}=nothing,
                        queue::Union{AbstractString,Nothing}=nothing, callback=nothing, manual_ack::Bool=false,
                        config::Union{ConsumerConfig,AbstractDict{String,<:Any}}=ConsumerConfig(),
                        timeout::Real=js.timeout, ordered::Bool=false, borrowed::Bool=false,
                        cancel_token::MaybeCancellationToken=nothing)
    _push_subscribe(js, subject; stream, durable, queue, callback, manual_ack, config, timeout,
                    ordered, borrowed, cancel_token)
end

function _mark_push_server_deleted!(psub::PushSubscription)
    handler = psub.control_handler
    @lock psub.close_lock psub.server_deleted = true
    isnothing(handler) || (handler.consumer_deleted[] = true)
    nothing
end

function _push_server_deleted(psub::PushSubscription)::Bool
    handler = psub.control_handler
    @lock psub.close_lock begin
        if psub.server_deleted
            true
        else
            deleted = !isnothing(handler) && handler.consumer_deleted[]
            deleted && (psub.server_deleted = true)
            deleted
        end
    end
end

function _close_pull_subscription(psub::PullSubscription; timeout::Real=psub.js.timeout,
                                  cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    timeout = _positive_timeout_seconds("timeout", timeout)
    deadline = time() + timeout
    already_closed, deliveries = @lock psub.close_lock begin
        was_closed = psub.closed
        psub.closed = true
        deliveries = copy(psub.active_deliveries)
        empty!(psub.active_deliveries)
        psub.fetch_delivery = nothing
        psub.fetch_delivery_in_use = false
        was_closed, deliveries
    end
    errors = Any[]
    if !already_closed
        for sub in deliveries
            try
                close(sub)
            catch err
                push!(errors, CleanupError("close pull delivery subscription", err))
            end
        end
    end
    server_deleted = @lock psub.close_lock psub.server_deleted
    if psub.delete_on_close && !server_deleted
        try
            _delete_consumer_for_close!(
                () -> (@lock psub.close_lock psub.server_deleted = true),
                psub.js, psub.stream, psub.consumer, "delete pull consumer";
                timeout=_remaining_timeout_or_throw(deadline, "close pull subscription"; cancel_token),
                cancel_token)
        catch err
            push!(errors, err isa CleanupError ? err : CleanupError("delete pull consumer $(psub.consumer)", err))
        end
    end
    _throw_errors(errors)
    nothing
end

close(psub::PullSubscription; timeout::Real=psub.js.timeout,
      cancel_token::MaybeCancellationToken=nothing) =
    _close_pull_subscription(psub; timeout, cancel_token)

function _close_push_subscription(psub::PushSubscription; timeout::Real=psub.js.timeout,
                                  cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    timeout = _positive_timeout_seconds("timeout", timeout)
    deadline = time() + timeout
    already_closed = @lock psub.close_lock begin
        was_closed = psub.closed
        psub.closed = true
        was_closed
    end
    errors = Any[]
    if !already_closed
        try
            close(psub.sub)
        catch err
            push!(errors, err)
        end
        _wait_task!(errors, "stop push heartbeat monitor $(psub.consumer)", psub.heartbeat_task;
                    interrupt=true, deadline)
        _wait_task!(errors, "stop ordered push reset $(psub.consumer)", psub.ordered_reset_task;
                    interrupt=true, deadline)
    end
    server_deleted = _push_server_deleted(psub)
    if psub.delete_on_close && !server_deleted
        try
            _delete_consumer_for_close!(
                () -> _mark_push_server_deleted!(psub),
                psub.js, psub.stream, psub.consumer, "delete push consumer";
                timeout=_remaining_timeout_or_throw(deadline, "close push subscription"; cancel_token),
                cancel_token)
        catch err
            push!(errors, err isa CleanupError ? err : CleanupError("delete push consumer $(psub.consumer)", err))
        end
    end
    _throw_errors(errors)
    nothing
end

close(psub::PushSubscription; timeout::Real=psub.js.timeout,
      cancel_token::MaybeCancellationToken=nothing) =
    _close_push_subscription(psub; timeout, cancel_token)

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
    timestamp_ns, first = _ack_parse_int_token(reply, first, last, false)
    pending = first == 0 ? 0 : _ack_parse_int(reply, first, last)

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
    if token_count == 8 || token_count == 9
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

function _ack_reply_subject(msg::AbstractJetStreamMsg)::String
    isnothing(msg.reply) && throw(JetStreamError(400, nothing, "message has no ack reply subject"))
    msg.reply
end

_ack_terminal(kind::Symbol)::Bool = kind != :progress

function _ack_already_acknowledged()
    throw(JetStreamError(400, nothing, "message already acknowledged"))
end

function _begin_ack!(msg::AbstractJetStreamMsg)
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

function _finish_ack!(msg::AbstractJetStreamMsg, terminal::Bool, succeeded::Bool)
    next_state = terminal && succeeded ? _JS_ACK_DONE : _JS_ACK_OPEN
    @atomic msg._ack_state = next_state
    nothing
end

_acknowledged(msg::AbstractJetStreamMsg)::Bool = (@atomic msg._ack_state) == _JS_ACK_DONE

function _ack_publish_raw(client::Client, reply::String, kind::Symbol; delay=nothing,
                          cancel_token::MaybeCancellationToken=nothing)::Nothing
    _throw_if_cancelled(cancel_token)
    payload = _ack_payload(kind; delay)
    terminal = _ack_terminal(kind)
    _publish_unchecked(client, reply, payload;
                       buffer_on_reconnect=!terminal,
                       force_flush=terminal,
                       cancel_token)
    nothing
end

function _ack_request_raw(client::Client, reply::String, kind::Symbol; delay=nothing,
                          timeout::Real=1.0,
                          cancel_token::MaybeCancellationToken=nothing)::Msg
    _throw_if_cancelled(cancel_token)
    timeout = _positive_timeout_seconds("timeout", timeout)
    payload = _ack_payload(kind; delay)
    mux = _ensure_request_mux(client; cancel_token)
    token, waiter = _register_request_waiter!(client, mux, timeout)
    response_subject = waiter.reply
    try
        frame = _publish_frame(reply, response_subject, payload, EMPTY_BYTES)
        _publish_frame_unchecked(client, frame; buffer_on_reconnect=false, force_flush=true,
                                 cancel_token)
        response = _wait_request_reply(mux, waiter, timeout; cancel_token)
        code = _status_header(response)
        if code == 503
            throw(NoRespondersError(reply))
        elseif !isnothing(code) && code >= 400
            throw(ProtocolError("request failed with status $code $(_status_description(response))"))
        end
        response
    finally
        _remove_request_waiter!(client, mux, token, waiter)
    end
end

function _ack_publish(msg::AbstractJetStreamMsg, kind::Symbol; delay=nothing,
                      cancel_token::MaybeCancellationToken=nothing)::Nothing
    _throw_if_cancelled(cancel_token)
    reply = _ack_reply_subject(msg)
    terminal = _ack_terminal(kind)
    _begin_ack!(msg)
    succeeded = false
    try
        _ack_publish_raw(msg._client, reply, kind; delay, cancel_token)
        succeeded = true
    finally
        _finish_ack!(msg, terminal, succeeded)
    end
    nothing
end

function _ack_request(msg::AbstractJetStreamMsg, kind::Symbol; delay=nothing, timeout::Real=1.0,
                      cancel_token::MaybeCancellationToken=nothing)::Msg
    _throw_if_cancelled(cancel_token)
    timeout = _positive_timeout_seconds("timeout", timeout)
    reply = _ack_reply_subject(msg)
    terminal = _ack_terminal(kind)
    _begin_ack!(msg)
    succeeded = false
    try
        response = _ack_request_raw(msg._client, reply, kind; delay, timeout, cancel_token)
        succeeded = true
        response
    finally
        _finish_ack!(msg, terminal, succeeded)
    end
end

ack(msg::AbstractJetStreamMsg; cancel_token::MaybeCancellationToken=nothing)::Nothing =
    _ack_publish(msg, :ack; cancel_token)
ack_sync(msg::AbstractJetStreamMsg; timeout::Real=1.0,
         cancel_token::MaybeCancellationToken=nothing)::Msg =
    _ack_request(msg, :ack; timeout, cancel_token)
nak(msg::AbstractJetStreamMsg; delay=nothing, cancel_token::MaybeCancellationToken=nothing)::Nothing =
    _ack_publish(msg, :nak; delay, cancel_token)
in_progress(msg::AbstractJetStreamMsg; cancel_token::MaybeCancellationToken=nothing)::Nothing =
    _ack_publish(msg, :progress; cancel_token)
term(msg::AbstractJetStreamMsg; cancel_token::MaybeCancellationToken=nothing)::Nothing =
    _ack_publish(msg, :term; cancel_token)
