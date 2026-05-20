const _PreparedFrameBytes = Union{Base.CodeUnits{UInt8,String},ImmutableBytes}

struct PublishFrame <: _AbstractPublishFrame
    subject::String
    reply::Union{String,Nothing}
    payload::_PreparedFrameBytes
    headers::_PreparedFrameBytes
    payload_size::Int
    serialized_size::Int
    wire::Union{ImmutableBytes,Nothing}
end

struct _PublishFrame{P<:AbstractVector{UInt8},H<:AbstractVector{UInt8}} <: _AbstractPublishFrame
    subject::String
    reply::Union{String,Nothing}
    payload::P
    headers::H
end

function PublishFrame(subject::AbstractString, reply::Union{AbstractString,Nothing},
                      data, headers)
    subject = _validate_publish_subject(subject)
    reply = isnothing(reply) ? nothing : _validate_publish_subject(reply)
    payload = _prepared_payload_bytes(data)
    header_bytes = _prepared_header_bytes(headers)
    frame = _PublishFrame(subject, reply, payload, header_bytes)
    payload_size = _pub_payload_size(frame)
    serialized_size = _serialized_size(frame)
    # Prepared frames are intended for repeated hot-path publishes. Cache a
    # contiguous wire image for small frames so those publishes skip formatting.
    wire = serialized_size <= DEFAULT_DIRECT_WRITE_THRESHOLD ?
           ImmutableBytes(_pub_cmd(frame); copy=false) : nothing
    PublishFrame(subject, reply, payload, header_bytes, payload_size, serialized_size, wire)
end

PublishFrame(subject::AbstractString, data=nothing;
             reply::Union{AbstractString,Nothing}=nothing,
             headers=nothing) =
    PublishFrame(subject, reply, data, headers)

function _publish_frame(subject::AbstractString, reply::Union{AbstractString,Nothing},
                        data, headers)
    subject = _validate_publish_subject(subject)
    reply = isnothing(reply) ? nothing : _validate_publish_subject(reply)
    _PublishFrame(subject, reply, _payload_bytes(data), _publish_header_bytes(headers))
end

_pub_payload_size(payload::AbstractVector{UInt8}, hdr::AbstractVector{UInt8}) = length(hdr) + length(payload)
_pub_payload_size(frame::_AbstractPublishFrame) = _pub_payload_size(frame.payload, frame.headers)
_pub_payload_size(frame::PublishFrame) = frame.payload_size

function _pending_publish_entry(frame::_AbstractPublishFrame, frame_size::Int=_serialized_size(frame))
    data = _pub_cmd(frame)
    length(data) == frame_size || throw(AssertionError("pending publish size mismatch"))
    _PendingPublishEntry(data, _pub_payload_size(frame), length(frame.headers))
end

function _decimal_digits(value::Int)::Int
    value >= 0 || throw(ArgumentError("value must be non-negative"))
    digits = 1
    while value >= 10
        value = div(value, 10)
        digits += 1
    end
    digits
end

function _serialized_size(frame::_AbstractPublishFrame)::Int
    payload_len = length(frame.payload)
    subject_len = ncodeunits(frame.subject)
    reply_len = isnothing(frame.reply) ? 0 : ncodeunits(frame.reply)
    if isempty(frame.headers)
        # "PUB " subject [" " reply] " " payload_len CRLF payload CRLF
        return 4 + subject_len + 1 + reply_len + (isnothing(frame.reply) ? 0 : 1) +
               _decimal_digits(payload_len) + 2 + payload_len + 2
    end
    headers_len = length(frame.headers)
    total = headers_len + payload_len
    # "HPUB " subject [" " reply] " " headers_len " " total CRLF headers payload CRLF
    5 + subject_len + 1 + reply_len + (isnothing(frame.reply) ? 0 : 1) +
    _decimal_digits(headers_len) + 1 + _decimal_digits(total) + 2 +
    headers_len + payload_len + 2
end

_serialized_size(frame::PublishFrame)::Int = frame.serialized_size

_cached_pub_wire(::_AbstractPublishFrame) = nothing
_cached_pub_wire(frame::PublishFrame) = frame.wire

function _copy_codeunits!(dest::Vector{UInt8}, pos::Int, value::AbstractString)::Int
    n = ncodeunits(value)
    copyto!(dest, pos, codeunits(value), 1, n)
    pos + n
end

function _copy_bytes!(dest::Vector{UInt8}, pos::Int, bytes::AbstractVector{UInt8})::Int
    n = length(bytes)
    copyto!(dest, pos, bytes, firstindex(bytes), n)
    pos + n
end

function _copy_decimal!(dest::Vector{UInt8}, pos::Int, value::Int)::Int
    digits = _decimal_digits(value)
    idx = pos + digits - 1
    while true
        dest[idx] = UInt8('0') + UInt8(rem(value, 10))
        value = div(value, 10)
        value == 0 && break
        idx -= 1
    end
    pos + digits
end

function _write_decimal(io, value::Int)
    digits = _decimal_digits(value)
    divisor = 1
    for _ in 2:digits
        divisor *= 10
    end
    while divisor > 0
        digit = div(value, divisor)
        write(io, UInt8('0') + UInt8(digit))
        value -= digit * divisor
        divisor = div(divisor, 10)
    end
    nothing
end

function _copy_byte!(dest::Vector{UInt8}, pos::Int, byte::UInt8)::Int
    dest[pos] = byte
    pos + 1
end

function _pub_cmd!(out::Vector{UInt8}, frame::_AbstractPublishFrame)::Vector{UInt8}
    resize!(out, _serialized_size(frame))
    pos = 1
    if isempty(frame.headers)
        pos = _copy_codeunits!(out, pos, "PUB ")
        pos = _copy_codeunits!(out, pos, frame.subject)
        pos = _copy_byte!(out, pos, UInt8(' '))
        if !isnothing(frame.reply)
            pos = _copy_codeunits!(out, pos, frame.reply)
            pos = _copy_byte!(out, pos, UInt8(' '))
        end
        pos = _copy_decimal!(out, pos, length(frame.payload))
        pos = _copy_bytes!(out, pos, CRLF_BYTES)
        pos = _copy_bytes!(out, pos, frame.payload)
    else
        total = _pub_payload_size(frame)
        pos = _copy_codeunits!(out, pos, "HPUB ")
        pos = _copy_codeunits!(out, pos, frame.subject)
        pos = _copy_byte!(out, pos, UInt8(' '))
        if !isnothing(frame.reply)
            pos = _copy_codeunits!(out, pos, frame.reply)
            pos = _copy_byte!(out, pos, UInt8(' '))
        end
        pos = _copy_decimal!(out, pos, length(frame.headers))
        pos = _copy_byte!(out, pos, UInt8(' '))
        pos = _copy_decimal!(out, pos, total)
        pos = _copy_bytes!(out, pos, CRLF_BYTES)
        pos = _copy_bytes!(out, pos, frame.headers)
        pos = _copy_bytes!(out, pos, frame.payload)
    end
    pos = _copy_bytes!(out, pos, CRLF_BYTES)
    pos == length(out) + 1 || throw(AssertionError("publish frame size mismatch"))
    out
end

_pub_cmd(frame::_AbstractPublishFrame)::Vector{UInt8} = _pub_cmd!(UInt8[], frame)

function _pub_prefix_size(frame::_AbstractPublishFrame)::Int
    subject_len = ncodeunits(frame.subject)
    reply_len = isnothing(frame.reply) ? 0 : ncodeunits(frame.reply)
    if isempty(frame.headers)
        return 4 + subject_len + 1 + reply_len + (isnothing(frame.reply) ? 0 : 1) +
               _decimal_digits(length(frame.payload)) + 2
    end
    headers_len = length(frame.headers)
    total = headers_len + length(frame.payload)
    5 + subject_len + 1 + reply_len + (isnothing(frame.reply) ? 0 : 1) +
    _decimal_digits(headers_len) + 1 + _decimal_digits(total) + 2
end

function _pub_prefix!(out::Vector{UInt8}, frame::_AbstractPublishFrame)::Vector{UInt8}
    resize!(out, _pub_prefix_size(frame))
    pos = 1
    if isempty(frame.headers)
        pos = _copy_codeunits!(out, pos, "PUB ")
        pos = _copy_codeunits!(out, pos, frame.subject)
        pos = _copy_byte!(out, pos, UInt8(' '))
        if !isnothing(frame.reply)
            pos = _copy_codeunits!(out, pos, frame.reply)
            pos = _copy_byte!(out, pos, UInt8(' '))
        end
        pos = _copy_decimal!(out, pos, length(frame.payload))
    else
        total = _pub_payload_size(frame)
        pos = _copy_codeunits!(out, pos, "HPUB ")
        pos = _copy_codeunits!(out, pos, frame.subject)
        pos = _copy_byte!(out, pos, UInt8(' '))
        if !isnothing(frame.reply)
            pos = _copy_codeunits!(out, pos, frame.reply)
            pos = _copy_byte!(out, pos, UInt8(' '))
        end
        pos = _copy_decimal!(out, pos, length(frame.headers))
        pos = _copy_byte!(out, pos, UInt8(' '))
        pos = _copy_decimal!(out, pos, total)
    end
    pos = _copy_bytes!(out, pos, CRLF_BYTES)
    pos == length(out) + 1 || throw(AssertionError("publish prefix size mismatch"))
    out
end

function _write_pub_frame_direct(io, frame::_AbstractPublishFrame, scratch::Vector{UInt8},
                                 contiguous_threshold::Int)
    frame_size = _serialized_size(frame)
    if contiguous_threshold > 0 && frame_size <= contiguous_threshold
        wire = _cached_pub_wire(frame)
        isnothing(wire) ? write(io, _pub_cmd!(scratch, frame)) : write(io, wire)
    else
        write(io, _pub_prefix!(scratch, frame))
        isempty(frame.headers) || write(io, frame.headers)
        write(io, frame.payload)
        write(io, CRLF)
    end
    nothing
end

function _write_pub_frame(io, frame::_AbstractPublishFrame)
    if isempty(frame.headers)
        write(io, "PUB ")
        write(io, frame.subject)
        write(io, UInt8(' '))
        if !isnothing(frame.reply)
            write(io, frame.reply)
            write(io, UInt8(' '))
        end
        _write_decimal(io, length(frame.payload))
        write(io, CRLF)
        write(io, frame.payload)
    else
        write(io, "HPUB ")
        write(io, frame.subject)
        write(io, UInt8(' '))
        if !isnothing(frame.reply)
            write(io, frame.reply)
            write(io, UInt8(' '))
        end
        _write_decimal(io, length(frame.headers))
        write(io, UInt8(' '))
        _write_decimal(io, _pub_payload_size(frame))
        write(io, CRLF)
        write(io, frame.headers)
        write(io, frame.payload)
    end
    write(io, CRLF)
    nothing
end

function _sub_cmd(subject::String, queue::Union{String,Nothing}, sid::Int)
    isnothing(queue) || isempty(queue) ? "SUB $subject $sid$CRLF" : "SUB $subject $queue $sid$CRLF"
end

function _subscription_setup_cmd(subject::String, queue::Union{String,Nothing}, sid::Int,
                                 remaining::Int)::String
    sub_cmd = _sub_cmd(subject, queue, sid)
    remaining > 0 ? string(sub_cmd, _unsub_cmd(sid, remaining)) : sub_cmd
end

function _validate_core_max_msgs(max_msgs)::Int
    _nonnegative_integer_option("max_msgs", max_msgs)
end

function _unsub_cmd(sid::Int, max_msgs=0)
    max_msgs = _validate_core_max_msgs(max_msgs)
    max_msgs > 0 ? "UNSUB $sid $max_msgs$CRLF" : "UNSUB $sid$CRLF"
end

function _validate_subject_token(wildcard::UInt8, token_chars::Int, allow_wildcards::Bool,
                                 kind::AbstractString)
    wildcard == 0x00 && return nothing
    token_chars == 1 || throw(ArgumentError("wildcards must occupy a complete $kind token"))
    allow_wildcards || throw(ArgumentError("$kind cannot contain wildcards"))
    nothing
end

@inline _invalid_subject_space(byte::UInt8)::Bool =
    byte <= 0x20 || byte == 0x7f

function _validate_subject(subject::AbstractString; allow_wildcards::Bool=true,
                           kind::AbstractString="subject")
    s = String(subject)
    isempty(s) && throw(ArgumentError("$kind cannot be empty"))
    token_chars = 0
    token_wildcard = UInt8(0)
    previous_dot = false
    for byte in codeunits(s)
        _invalid_subject_space(byte) &&
            throw(ArgumentError("$kind cannot contain whitespace or control characters"))
        if byte == UInt8('.')
            if token_chars == 0
                msg = previous_dot ? "$kind cannot contain consecutive dots" : "$kind cannot start with '.'"
                throw(ArgumentError(msg))
            end
            token_wildcard == UInt8('>') && throw(ArgumentError("$kind wildcard '>' must be the final token"))
            _validate_subject_token(token_wildcard, token_chars, allow_wildcards, kind)
            token_chars = 0
            token_wildcard = UInt8(0)
            previous_dot = true
        else
            if byte == UInt8('>') || byte == UInt8('*')
                token_wildcard = byte
            end
            token_chars += 1
            previous_dot = false
        end
    end
    previous_dot && throw(ArgumentError("$kind cannot end with '.'"))
    _validate_subject_token(token_wildcard, token_chars, allow_wildcards, kind)
    s
end

_validate_publish_subject(subject::AbstractString) =
    _validate_subject(subject; allow_wildcards=false, kind="publish subject")

function _validate_queue(queue::Union{AbstractString,Nothing})
    isnothing(queue) && return nothing
    _validate_subject(queue; allow_wildcards=false, kind="queue")
end

_payload_bytes(::Nothing) = EMPTY_BYTES
_payload_bytes(data::Vector{UInt8}) = data
_payload_bytes(data::AbstractVector{UInt8}) = data
_payload_bytes(data::AbstractString) = codeunits(String(data))
_payload_bytes(data) =
    throw(ArgumentError("payload data must be nothing, an AbstractString, or an AbstractVector{UInt8}; encode structured values explicitly"))

_prepared_payload_bytes(::Nothing) = codeunits("")
_prepared_payload_bytes(data::String) = codeunits(data)
_prepared_payload_bytes(data::AbstractString) = _prepared_payload_bytes(String(data))
_prepared_payload_bytes(data::AbstractVector{UInt8}) =
    isempty(data) ? codeunits("") : ImmutableBytes(data)
_prepared_payload_bytes(data) =
    throw(ArgumentError("payload data must be nothing, an AbstractString, or an AbstractVector{UInt8}; encode structured values explicitly"))

_publish_header_bytes(::Nothing) = EMPTY_BYTES
function _publish_header_bytes(headers::AbstractVector{UInt8})
    isempty(headers) && return EMPTY_BYTES
    try
        _validate_headers(headers)
    catch err
        err isa ProtocolError && throw(ArgumentError(err.message))
        rethrow()
    end
    headers
end
_publish_header_bytes(headers::Headers) = _headers_bytes(headers)
_publish_header_bytes(headers) = _publish_header_bytes(_headers_from_input(headers))

_prepared_header_bytes(::Nothing) = codeunits("")
function _prepared_header_bytes(headers::AbstractVector{UInt8})
    isempty(headers) && return codeunits("")
    try
        _validate_headers(headers)
    catch err
        err isa ProtocolError && throw(ArgumentError(err.message))
        rethrow()
    end
    ImmutableBytes(headers)
end
function _prepared_header_bytes(headers::Headers)
    bytes = _headers_bytes(headers)
    isempty(bytes) ? codeunits("") : ImmutableBytes(bytes; copy=false)
end
_prepared_header_bytes(headers) = _prepared_header_bytes(_headers_from_input(headers))

_bytes(data) = Vector{UInt8}(_payload_bytes(data))
