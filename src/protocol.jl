function _json_dict(bytes_or_string)
    obj = JSON3.read(bytes_or_string)
    Dict{String,Any}(String(k) => v for (k, v) in pairs(obj))
end

function _server_info(bytes_or_string)::ServerInfo
    obj = JSON3.read(bytes_or_string)
    info = ServerInfo()
    for (raw_key, value) in pairs(obj)
        key = String(raw_key)
        if key == "max_payload" && !isnothing(value)
            info.max_payload = Int(value)
        elseif key == "tls_required" && !isnothing(value)
            info.tls_required = Bool(value)
        elseif key == "tls_available" && !isnothing(value)
            info.tls_available = Bool(value)
        elseif key == "connect_urls" && !isnothing(value)
            info.connect_urls = String[String(url) for url in value]
        elseif key == "version" && !isnothing(value)
            info.version = String(value)
        elseif key == "headers" && !isnothing(value)
            info.headers = Bool(value)
        elseif key == "ldm" && !isnothing(value)
            info.ldm = Bool(value)
        end
    end
    info
end

_ascii_upper(byte::UInt8) = UInt8('a') <= byte <= UInt8('z') ? byte - 0x20 : byte

function _ascii_eq_ci(value::AbstractString, expected::AbstractString)::Bool
    ncodeunits(value) == ncodeunits(expected) || return false
    @inbounds for i in 1:ncodeunits(expected)
        _ascii_upper(codeunit(value, i)) == codeunit(expected, i) || return false
    end
    true
end

function _ascii_startswith_ci(value::AbstractString, expected::AbstractString)::Bool
    ncodeunits(value) >= ncodeunits(expected) || return false
    @inbounds for i in 1:ncodeunits(expected)
        _ascii_upper(codeunit(value, i)) == codeunit(expected, i) || return false
    end
    true
end

@inline _is_hspace(byte::UInt8)::Bool = byte == UInt8(' ') || byte == UInt8('\t')

function _ascii_eq_ci(bytes::AbstractVector{UInt8}, first::Int, last::Int, expected::AbstractString)::Bool
    last - first + 1 == ncodeunits(expected) || return false
    @inbounds for i in 1:ncodeunits(expected)
        _ascii_upper(bytes[first + i - 1]) == codeunit(expected, i) || return false
    end
    true
end

function _ascii_startswith_ci(bytes::AbstractVector{UInt8}, first::Int, last::Int, expected::AbstractString)::Bool
    last - first + 1 >= ncodeunits(expected) || return false
    @inbounds for i in 1:ncodeunits(expected)
        _ascii_upper(bytes[first + i - 1]) == codeunit(expected, i) || return false
    end
    true
end

function _ascii_startswith(bytes::AbstractVector{UInt8}, first::Int, last::Int, expected::AbstractString)::Bool
    last - first + 1 >= ncodeunits(expected) || return false
    @inbounds for i in 1:ncodeunits(expected)
        bytes[first + i - 1] == codeunit(expected, i) || return false
    end
    true
end

function _bytes_string(bytes::Vector{UInt8}, first::Int, last::Int)::String
    last >= first || return ""
    unsafe_string(pointer(bytes, first), last - first + 1)
end

function _bytes_string(bytes::AbstractVector{UInt8}, first::Int, last::Int)::String
    last >= first || return ""
    String(@view bytes[first:last])
end

_reader_available(reader::ProtocolReader)::Int = reader.last - reader.first + 1

function _drop_consumed!(reader::ProtocolReader)
    reader.first == 1 && return nothing
    available = _reader_available(reader)
    if available <= 0
        empty!(reader.buffer)
        reader.first = 1
        reader.last = 0
    else
        copyto!(reader.buffer, 1, reader.buffer, reader.first, available)
        resize!(reader.buffer, available)
        reader.first = 1
        reader.last = available
    end
    nothing
end

function _fill_reader!(reader::ProtocolReader)
    _drop_consumed!(reader)
    n = try
        readbytes!(reader.io, reader.scratch, 1)
    catch err
        err isa EOFError || rethrow()
        throw(err)
    end
    n == 0 && throw(EOFError())
    extra = min(_bytesavailable(reader.io), length(reader.scratch) - n)
    if extra > 0
        unsafe_read(reader.io, pointer(reader.scratch, n + 1), UInt(extra))
        n += extra
    end
    old = length(reader.buffer)
    resize!(reader.buffer, old + n)
    copyto!(reader.buffer, old + 1, reader.scratch, 1, n)
    reader.last = length(reader.buffer)
    nothing
end

function _bytesavailable(io)::Int
    try
        bytesavailable(io)
    catch err
        err isa MethodError || rethrow()
        0
    end
end

function _read_exact_bytes(io, n::Int)::Vector{UInt8}
    n == 0 && return UInt8[]
    data = Vector{UInt8}(undef, n)
    unsafe_read(io, pointer(data), UInt(n))
    data
end

_read_exact_bytes_no_drop(io, n::Int) = _read_exact_bytes(io, n)

function _read_exact_bytes_no_drop(reader::ProtocolReader, n::Int)::Vector{UInt8}
    n == 0 && return UInt8[]
    data = Vector{UInt8}(undef, n)
    offset = 1
    remaining = n
    while remaining > 0
        available = _reader_available(reader)
        if available > 0
            take = min(available, remaining)
            copyto!(data, offset, reader.buffer, reader.first, take)
            reader.first += take
            offset += take
            remaining -= take
        else
            unsafe_read(reader.io, pointer(data, offset), UInt(remaining))
            remaining = 0
        end
    end
    data
end

function _read_exact_bytes(reader::ProtocolReader, n::Int)::Vector{UInt8}
    data = _read_exact_bytes_no_drop(reader, n)
    _drop_consumed!(reader)
    data
end

function _readline_crlf_range(reader::ProtocolReader, max_control_line::Int)::UnitRange{Int}
    while true
        newline = _find_byte(reader.buffer, UInt8('\n'), reader.first, reader.last)
        if !isnothing(newline)
            line_bytes = newline - reader.first + 1
            line_bytes > max_control_line && throw(ProtocolError("protocol line exceeds configured limit of $max_control_line bytes"))
            line_first = reader.first
            data_end = newline - 1
            if data_end >= reader.first && reader.buffer[data_end] == UInt8('\r')
                data_end -= 1
            end
            reader.first = newline + 1
            return line_first:data_end
        end
        _reader_available(reader) > max_control_line && throw(ProtocolError("protocol line exceeds configured limit of $max_control_line bytes"))
        try
            _fill_reader!(reader)
        catch err
            err isa EOFError || rethrow()
            _reader_available(reader) == 0 && throw(ProtocolError("unexpected EOF while reading protocol line"))
            throw(ProtocolError("protocol line missing LF"))
        end
    end
end

function _readline_crlf(reader::ProtocolReader, max_control_line::Int)
    range = _readline_crlf_range(reader, max_control_line)
    line = _bytes_string(reader.buffer, first(range), last(range))
    _drop_consumed!(reader)
    line
end

_readline_crlf(io, max_control_line::Int) =
    _readline_crlf(ProtocolReader(io; read_size=1), max_control_line)

function _validate_payload_size(size::Int, max_payload::Int)
    size >= 0 || throw(ProtocolError("message payload size cannot be negative"))
    size <= max_payload || throw(ProtocolError("message payload size $size exceeds configured limit of $max_payload bytes"))
    size
end

function _validate_header_size(size::Int, max_header_bytes::Int)
    size >= 0 || throw(ProtocolError("message header size cannot be negative"))
    size <= max_header_bytes || throw(ProtocolError("message header size $size exceeds configured limit of $max_header_bytes bytes"))
    size
end

_drop_payload_consumed!(io) = nothing
_drop_payload_consumed!(reader::ProtocolReader) = _drop_consumed!(reader)

function _read_byte_no_drop(io)::UInt8
    read(io, UInt8)
end

function _read_byte_no_drop(reader::ProtocolReader)::UInt8
    if _reader_available(reader) > 0
        byte = reader.buffer[reader.first]
        reader.first += 1
        return byte
    end
    unsafe_read(reader.io, pointer(reader.scratch), UInt(1))
    reader.scratch[1]
end

function _read_payload_trailer_no_drop!(io)
    b1 = _read_byte_no_drop(io)
    b2 = _read_byte_no_drop(io)
    b1 == CRLF_BYTES[1] && b2 == CRLF_BYTES[2] || throw(ProtocolError("message payload missing CRLF trailer"))
    nothing
end

function _read_exact_payload(io, n::Int)
    data = try
        _read_exact_bytes_no_drop(io, n)
    catch err
        err isa EOFError || rethrow()
        throw(ProtocolError("unexpected EOF while reading payload"))
    end
    try
        _read_payload_trailer_no_drop!(io)
    catch err
        err isa EOFError || rethrow()
        throw(ProtocolError("message payload missing CRLF trailer"))
    end
    _drop_payload_consumed!(io)
    data
end

function _read_exact_header_payload(io, hsize::Int, total::Int)
    hsize <= total || throw(ProtocolError("header size exceeds message size"))
    header = try
        _read_exact_bytes_no_drop(io, hsize)
    catch err
        err isa EOFError || rethrow()
        throw(ProtocolError("unexpected EOF while reading payload"))
    end
    data = try
        _read_exact_bytes_no_drop(io, total - hsize)
    catch err
        err isa EOFError || rethrow()
        throw(ProtocolError("unexpected EOF while reading payload"))
    end
    try
        _read_payload_trailer_no_drop!(io)
    catch err
        err isa EOFError || rethrow()
        throw(ProtocolError("message payload missing CRLF trailer"))
    end
    _drop_payload_consumed!(io)
    header, data
end

function _skip_hspace(bytes::AbstractVector{UInt8}, pos::Int, stop::Int)::Int
    @inbounds while pos <= stop && _is_hspace(bytes[pos])
        pos += 1
    end
    pos
end

function _next_token(bytes::AbstractVector{UInt8}, pos::Int, stop::Int)::Tuple{Int,Int,Int}
    pos = _skip_hspace(bytes, pos, stop)
    pos > stop && return (0, -1, pos)
    token_start = pos
    @inbounds while pos <= stop && !_is_hspace(bytes[pos])
        pos += 1
    end
    token_start, pos - 1, pos
end

function _has_more_tokens(bytes::AbstractVector{UInt8}, pos::Int, stop::Int)::Bool
    _skip_hspace(bytes, pos, stop) <= stop
end

function _parse_int_token(bytes::AbstractVector{UInt8}, first::Int, last::Int)::Int
    first <= last || throw(ProtocolError("malformed integer field in protocol line"))
    negative = false
    pos = first
    @inbounds begin
        if bytes[pos] == UInt8('-')
            negative = true
            pos += 1
        elseif bytes[pos] == UInt8('+')
            pos += 1
        end
        pos <= last || throw(ProtocolError("malformed integer field in protocol line"))
        value = 0
        while pos <= last
            byte = bytes[pos]
            UInt8('0') <= byte <= UInt8('9') || throw(ProtocolError("malformed integer field in protocol line"))
            digit = Int(byte - UInt8('0'))
            value <= (typemax(Int) - digit) ÷ 10 || throw(ProtocolError("integer field exceeds Int range"))
            value = value * 10 + digit
            pos += 1
        end
    end
    negative ? -value : value
end

function _malformed_control_line(kind::AbstractString, bytes::AbstractVector{UInt8}, first::Int, last::Int)
    throw(ProtocolError("malformed $kind control line: $(_bytes_string(bytes, first, last))"))
end

function _read_msg_line(reader::ProtocolReader, line_first::Int, line_last::Int, max_payload::Int)
    bytes = reader.buffer
    subject_start, subject_end, pos = _next_token(bytes, line_first + 4, line_last)
    subject_start == 0 && _malformed_control_line("MSG", bytes, line_first, line_last)
    sid_start, sid_end, pos = _next_token(bytes, pos, line_last)
    sid_start == 0 && _malformed_control_line("MSG", bytes, line_first, line_last)
    third_start, third_end, pos = _next_token(bytes, pos, line_last)
    third_start == 0 && _malformed_control_line("MSG", bytes, line_first, line_last)
    fourth_start, fourth_end, pos = _next_token(bytes, pos, line_last)
    _has_more_tokens(bytes, pos, line_last) && _malformed_control_line("MSG", bytes, line_first, line_last)

    subject = _bytes_string(bytes, subject_start, subject_end)
    sid = _parse_int_token(bytes, sid_start, sid_end)
    reply = fourth_start == 0 ? nothing : _bytes_string(bytes, third_start, third_end)
    size_start, size_end = fourth_start == 0 ? (third_start, third_end) : (fourth_start, fourth_end)
    size = _validate_payload_size(_parse_int_token(bytes, size_start, size_end), max_payload)
    payload = _read_exact_payload(reader, size)
    :MSG, Msg(subject, reply, payload, Headers(), nothing, sid, false)
end

function _read_hmsg_line(reader::ProtocolReader, line_first::Int, line_last::Int,
                         max_payload::Int, max_header_bytes::Int)
    bytes = reader.buffer
    subject_start, subject_end, pos = _next_token(bytes, line_first + 5, line_last)
    subject_start == 0 && _malformed_control_line("HMSG", bytes, line_first, line_last)
    sid_start, sid_end, pos = _next_token(bytes, pos, line_last)
    sid_start == 0 && _malformed_control_line("HMSG", bytes, line_first, line_last)
    third_start, third_end, pos = _next_token(bytes, pos, line_last)
    third_start == 0 && _malformed_control_line("HMSG", bytes, line_first, line_last)
    fourth_start, fourth_end, pos = _next_token(bytes, pos, line_last)
    fourth_start == 0 && _malformed_control_line("HMSG", bytes, line_first, line_last)
    fifth_start, fifth_end, pos = _next_token(bytes, pos, line_last)
    _has_more_tokens(bytes, pos, line_last) && _malformed_control_line("HMSG", bytes, line_first, line_last)

    subject = _bytes_string(bytes, subject_start, subject_end)
    sid = _parse_int_token(bytes, sid_start, sid_end)
    has_reply = fifth_start != 0
    reply = has_reply ? _bytes_string(bytes, third_start, third_end) : nothing
    hsize_start, hsize_end = has_reply ? (fourth_start, fourth_end) : (third_start, third_end)
    total_start, total_end = has_reply ? (fifth_start, fifth_end) : (fourth_start, fourth_end)
    hsize = _validate_header_size(_parse_int_token(bytes, hsize_start, hsize_end), max_header_bytes)
    total = _validate_payload_size(_parse_int_token(bytes, total_start, total_end), max_payload)
    header_bytes, payload = _read_exact_header_payload(reader, hsize, total)
    hdrs = _parse_headers(header_bytes)
    :MSG, Msg(subject, reply, payload, hdrs, nothing, sid, false, hsize)
end

function _read_control_or_msg(reader::ProtocolReader; max_control_line::Int=DEFAULT_MAX_CONTROL_LINE,
                              max_payload::Int=DEFAULT_MAX_INBOUND_PAYLOAD,
                              max_header_bytes::Int=DEFAULT_MAX_HEADER_BYTES)
    line_range = _readline_crlf_range(reader, max_control_line)
    line_first = first(line_range)
    line_last = last(line_range)
    line_last >= line_first || throw(ProtocolError("empty protocol line"))
    bytes = reader.buffer
    if _ascii_startswith_ci(bytes, line_first, line_last, "INFO ")
        info = _server_info(@view bytes[line_first + 5:line_last])
        _drop_consumed!(reader)
        return (:INFO, info)
    elseif _ascii_eq_ci(bytes, line_first, line_last, "PING")
        _drop_consumed!(reader)
        return (:PING, nothing)
    elseif _ascii_eq_ci(bytes, line_first, line_last, "PONG")
        _drop_consumed!(reader)
        return (:PONG, nothing)
    elseif _ascii_eq_ci(bytes, line_first, line_last, "+OK")
        _drop_consumed!(reader)
        return (:OK, nothing)
    elseif _ascii_startswith_ci(bytes, line_first, line_last, "-ERR")
        msg = line_last - line_first + 1 >= 6 ? strip(_bytes_string(bytes, line_first + 5, line_last)) : ""
        msg = strip(String(msg), ['\'', ' '])
        _drop_consumed!(reader)
        return (:ERR, msg)
    elseif _ascii_startswith_ci(bytes, line_first, line_last, "MSG ")
        return _read_msg_line(reader, line_first, line_last, max_payload)
    elseif _ascii_startswith_ci(bytes, line_first, line_last, "HMSG ")
        return _read_hmsg_line(reader, line_first, line_last, max_payload, max_header_bytes)
    else
        line = _bytes_string(bytes, line_first, line_last)
        throw(ProtocolError("unknown protocol line: $line"))
    end
end

function _read_control_or_msg(io; max_control_line::Int=DEFAULT_MAX_CONTROL_LINE,
                              max_payload::Int=DEFAULT_MAX_INBOUND_PAYLOAD,
                              max_header_bytes::Int=DEFAULT_MAX_HEADER_BYTES)
    _read_control_or_msg(ProtocolReader(io; read_size=1);
                         max_control_line,
                         max_payload,
                         max_header_bytes)
end

function _read_control_or_msg(io, opts::ConnectOptions)
    _read_control_or_msg(io;
                         max_control_line=opts.max_control_line,
                         max_payload=opts.max_inbound_payload,
                         max_header_bytes=opts.max_header_bytes)
end

function _find_byte(bytes::AbstractVector{UInt8}, byte::UInt8, pos::Int, stop::Int)
    @inbounds while pos <= stop
        bytes[pos] == byte && return pos
        pos += 1
    end
    nothing
end

function _has_header_terminator(raw::AbstractVector{UInt8}, first::Int, last::Int)::Bool
    pos = first
    @inbounds while pos + 3 <= last
        raw[pos] == UInt8('\r') && raw[pos + 1] == UInt8('\n') &&
            raw[pos + 2] == UInt8('\r') && raw[pos + 3] == UInt8('\n') && return true
        pos += 1
    end
    false
end

function _all_digits(bytes::AbstractVector{UInt8}, first::Int, last::Int)::Bool
    first <= last || return false
    @inbounds for pos in first:last
        UInt8('0') <= bytes[pos] <= UInt8('9') || return false
    end
    true
end

function _parse_header_protocol_line!(headers::Headers, raw::AbstractVector{UInt8}, line_first::Int, line_last::Int)
    _ascii_startswith(raw, line_first, line_last, "NATS/1.0") || throw(ProtocolError("invalid NATS header block"))
    pos = _skip_hspace(raw, line_first + ncodeunits("NATS/1.0"), line_last)
    pos > line_last && return headers
    status_start, status_end, pos = _next_token(raw, pos, line_last)
    if _all_digits(raw, status_start, status_end)
        headers["Status"] = [_bytes_string(raw, status_start, status_end)]
        description_start = _skip_hspace(raw, pos, line_last)
        description_start <= line_last && (headers["Description"] = [_bytes_string(raw, description_start, line_last)])
    end
    headers
end

function _parse_headers(raw::AbstractVector{UInt8})
    h = Headers()
    isempty(raw) && return h
    raw_first = firstindex(raw)
    raw_last = lastindex(raw)
    _has_header_terminator(raw, raw_first, raw_last) || throw(ProtocolError("NATS header block missing terminator"))

    newline = _find_byte(raw, UInt8('\n'), raw_first, raw_last)
    isnothing(newline) && throw(ProtocolError("NATS header block missing terminator"))
    protocol_end = newline - 1
    protocol_end >= raw_first && raw[protocol_end] == UInt8('\r') && (protocol_end -= 1)
    _parse_header_protocol_line!(h, raw, raw_first, protocol_end)

    pos = newline + 1
    while pos <= raw_last
        newline = _find_byte(raw, UInt8('\n'), pos, raw_last)
        isnothing(newline) && throw(ProtocolError("NATS header block missing terminator"))
        line_end = newline - 1
        line_end >= pos && raw[line_end] == UInt8('\r') && (line_end -= 1)
        line_end < pos && return h

        colon = _find_byte(raw, UInt8(':'), pos, line_end)
        if isnothing(colon)
            throw(ProtocolError("malformed NATS header line"))
        end
        if colon == pos
            pos = newline + 1
            continue
        end
        value_start = colon + 1
        value_start = _skip_hspace(raw, value_start, line_end)
        key = _bytes_string(raw, pos, colon - 1)
        value = value_start <= line_end ? _bytes_string(raw, value_start, line_end) : ""
        push!(get!(h, key, String[]), value)
        pos = newline + 1
    end
    h
end

function _status_header(msg::Msg)
    value = header(msg, "Status")
    !isnothing(value) && !isempty(value) && all(isdigit, value) && return parse(Int, value)
    nothing
end

function _status_description(msg::Msg)
    value = header(msg, "Description")
    isnothing(value) ? "" : value
end

function _valid_header_field_name_char(byte::UInt8)::Bool
    UInt8('A') <= byte <= UInt8('Z') && return true
    UInt8('a') <= byte <= UInt8('z') && return true
    UInt8('0') <= byte <= UInt8('9') && return true
    byte == UInt8('!') || byte == UInt8('#') || byte == UInt8('$') ||
        byte == UInt8('%') || byte == UInt8('&') || byte == UInt8('\'') ||
        byte == UInt8('*') || byte == UInt8('+') || byte == UInt8('-') ||
        byte == UInt8('.') || byte == UInt8('^') || byte == UInt8('_') ||
        byte == UInt8('`') || byte == UInt8('|') || byte == UInt8('~')
end

function _validate_header_pair(key::AbstractString, value::AbstractString)
    k = String(key)
    isempty(k) && throw(ArgumentError("header key cannot be empty"))
    for byte in codeunits(k)
        _valid_header_field_name_char(byte) || throw(ArgumentError("header key contains an invalid character"))
    end
    v = String(value)
    if occursin('\r', v) || occursin('\n', v)
        throw(ArgumentError("header value contains an invalid character"))
    end
    k, v
end

function _headers_bytes(headers::Headers)
    isempty(headers) && return EMPTY_BYTES
    io = IOBuffer()
    write(io, "NATS/1.0", CRLF)
    for (k, values) in headers
        for v in values
            key, value = _validate_header_pair(k, v)
            write(io, key, ": ", value, CRLF)
        end
    end
    write(io, CRLF)
    take!(io)
end

struct PublishFrame{Payload<:AbstractVector{UInt8}}
    subject::String
    reply::Union{String,Nothing}
    payload::Payload
    headers::Vector{UInt8}
end

PublishFrame(subject::String, reply::Union{String,Nothing}, payload::AbstractVector{UInt8}, headers::Headers) =
    PublishFrame(subject, reply, payload, _headers_bytes(headers))

_pub_payload_size(payload::AbstractVector{UInt8}, hdr::AbstractVector{UInt8}) = length(hdr) + length(payload)
_pub_payload_size(frame::PublishFrame) = _pub_payload_size(frame.payload, frame.headers)

function _decimal_digits(value::Int)::Int
    value >= 0 || throw(ArgumentError("value must be non-negative"))
    digits = 1
    while value >= 10
        value = div(value, 10)
        digits += 1
    end
    digits
end

function _serialized_size(frame::PublishFrame)::Int
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

function _copy_byte!(dest::Vector{UInt8}, pos::Int, byte::UInt8)::Int
    dest[pos] = byte
    pos + 1
end

function _pub_cmd(frame::PublishFrame)::Vector{UInt8}
    out = Vector{UInt8}(undef, _serialized_size(frame))
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

function _write_pub_frame(io, frame::PublishFrame)
    if isempty(frame.headers)
        print(io, "PUB ", frame.subject, ' ')
        if !isnothing(frame.reply)
            print(io, frame.reply, ' ')
        end
        print(io, length(frame.payload), CRLF)
        write(io, frame.payload)
    else
        print(io, "HPUB ", frame.subject, ' ')
        if !isnothing(frame.reply)
            print(io, frame.reply, ' ')
        end
        print(io, length(frame.headers), ' ', _pub_payload_size(frame), CRLF)
        write(io, frame.headers)
        write(io, frame.payload)
    end
    write(io, CRLF_BYTES)
    nothing
end

function _pub_cmd(subject::String, reply::Union{String,Nothing}, payload::Vector{UInt8}, headers::Headers)
    _pub_cmd(PublishFrame(subject, reply, payload, headers))
end

function _pub_cmd(subject::String, reply::Union{String,Nothing}, payload::AbstractVector{UInt8}, hdr::Vector{UInt8})
    _pub_cmd(PublishFrame(subject, reply, payload, hdr))
end

function _sub_cmd(subject::String, queue::Union{String,Nothing}, sid::Int)
    isnothing(queue) || isempty(queue) ? "SUB $subject $sid$CRLF" : "SUB $subject $queue $sid$CRLF"
end

_unsub_cmd(sid::Int, max_msgs::Int=0) = max_msgs > 0 ? "UNSUB $sid $max_msgs$CRLF" : "UNSUB $sid$CRLF"

function _validate_subject(subject::AbstractString; allow_wildcards::Bool=true)
    s = String(subject)
    isempty(s) && throw(ArgumentError("subject cannot be empty"))
    occursin(r"\s", s) && throw(ArgumentError("subject cannot contain whitespace"))
    startswith(s, ".") && throw(ArgumentError("subject cannot start with '.'"))
    endswith(s, ".") && throw(ArgumentError("subject cannot end with '.'"))
    occursin("..", s) && throw(ArgumentError("subject cannot contain consecutive dots"))
    tokens = split(s, ".")
    for (i, token) in pairs(tokens)
        if token == ">"
            i == length(tokens) || throw(ArgumentError("subject wildcard '>' must be the final token"))
            allow_wildcards || throw(ArgumentError("publish subjects cannot contain wildcards"))
        elseif token == "*"
            allow_wildcards || throw(ArgumentError("publish subjects cannot contain wildcards"))
        elseif occursin(">", token) || occursin("*", token)
            throw(ArgumentError("wildcards must occupy a complete subject token"))
        end
    end
    s
end

_validate_publish_subject(subject::AbstractString) = _validate_subject(subject; allow_wildcards=false)

function _validate_queue(queue::Union{String,Nothing})
    isnothing(queue) && return nothing
    q = String(queue)
    isempty(q) && throw(ArgumentError("queue cannot be empty"))
    occursin(r"\s", q) && throw(ArgumentError("queue cannot contain whitespace"))
    q
end

_payload_bytes(::Nothing) = EMPTY_BYTES
_payload_bytes(data::Vector{UInt8}) = data
_payload_bytes(data::AbstractVector{UInt8}) = data
_payload_bytes(data::AbstractString) = codeunits(String(data))
_payload_bytes(data) = codeunits(JSON3.write(data))

_bytes(data) = Vector{UInt8}(_payload_bytes(data))
