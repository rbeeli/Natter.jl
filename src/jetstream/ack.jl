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
                       mode=terminal ? :queued : :replayable,
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
        _publish_frame_unchecked(client, frame; force_flush=true, cancel_token)
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
