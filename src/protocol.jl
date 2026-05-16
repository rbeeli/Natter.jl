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
        elseif key == "proto" && !isnothing(value)
            info.proto = Int(value)
        elseif key == "headers" && !isnothing(value)
            info.headers = Bool(value)
        elseif key == "nonce" && !isnothing(value)
            info.nonce = String(value)
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

function _bytes_equal_string(bytes::AbstractVector{UInt8}, first::Int, last::Int, value::String)::Bool
    len = last - first + 1
    len == ncodeunits(value) || return false
    @inbounds for i in 1:len
        bytes[first + i - 1] == codeunit(value, i) || return false
    end
    true
end

function _cached_subject!(reader::ProtocolReader, sid::Int, first::Int, last::Int)::String
    cached = get(reader.subject_cache, sid, nothing)
    if !isnothing(cached) && _bytes_equal_string(reader.buffer, first, last, cached)
        return cached
    end
    subject = _bytes_string(reader.buffer, first, last)
    reader.subject_cache[sid] = subject
    subject
end

_reader_available(reader::ProtocolReader)::Int = reader.last - reader.first + 1

const _READER_COMPACT_MIN_PREFIX = 8192

function _drop_consumed!(reader::ProtocolReader)
    reader.first == 1 && return nothing
    available = _reader_available(reader)
    if available <= 0
        empty!(reader.buffer)
        reader.first = 1
        reader.last = 0
    elseif reader.first > _READER_COMPACT_MIN_PREFIX && reader.first > available
        copyto!(reader.buffer, 1, reader.buffer, reader.first, available)
        resize!(reader.buffer, available)
        reader.first = 1
        reader.last = available
    end
    nothing
end

function _read_some!(io::IOStream, scratch::Vector{UInt8})::Int
    readbytes!(io, scratch, length(scratch); all=false)
end

function _read_some!(io::MbedTLS.SSLContext, scratch::Vector{UInt8})::Int
    n = readbytes!(io, scratch, 1; all=true)
    n == 0 && return 0

    pending = min(bytesavailable(io), length(scratch) - n)
    if pending > 0
        GC.@preserve scratch unsafe_read(io, pointer(scratch, n + 1), UInt(pending))
        n += pending
    end
    n
end

function _read_some!(io::Base.LibuvStream, scratch::Vector{UInt8})::Int
    Base.wait_readnb(io, 1)
    n = min(bytesavailable(io), length(scratch))
    n == 0 && return 0
    readbytes!(io, scratch, n)
end

function _read_some!(io::Base.GenericIOBuffer, scratch::Vector{UInt8})::Int
    readbytes!(io, scratch, length(scratch))
end

function _read_some!(io, scratch::Vector{UInt8})::Int
    try
        readbytes!(io, scratch, length(scratch); all=false)
    catch err
        err isa MethodError || rethrow()
        readbytes!(io, scratch, length(scratch))
    end
end

function _read_some_to_buffer!(io, buffer::Vector{UInt8}, offset::Int, max_bytes::Int,
                               scratch::Vector{UInt8})::Int
    n = _read_some!(io, scratch)
    n == 0 && return 0
    resize!(buffer, offset + n - 1)
    copyto!(buffer, offset, scratch, 1, n)
    n
end

function _read_some_to_buffer!(io::Base.LibuvStream, buffer::Vector{UInt8}, offset::Int,
                               max_bytes::Int, scratch::Vector{UInt8})::Int
    max_bytes > 0 || return 0
    old = offset - 1
    resize!(buffer, old + max_bytes)
    n = 0
    try
        Base.wait_readnb(io, 1)
        n = min(bytesavailable(io), max_bytes)
        if n > 0
            GC.@preserve buffer unsafe_read(io, pointer(buffer, offset), UInt(n))
        end
    catch
        resize!(buffer, old)
        rethrow()
    end
    resize!(buffer, old + n)
    n
end

function _read_some_to_buffer!(io::MbedTLS.SSLContext, buffer::Vector{UInt8}, offset::Int,
                               max_bytes::Int, scratch::Vector{UInt8})::Int
    max_bytes > 0 || return 0
    old = offset - 1
    resize!(buffer, old + max_bytes)
    n = 0
    try
        GC.@preserve buffer begin
            unsafe_read(io, pointer(buffer, offset), UInt(1))
            n = 1
            pending = min(bytesavailable(io), max_bytes - n)
            if pending > 0
                unsafe_read(io, pointer(buffer, offset + n), UInt(pending))
                n += pending
            end
        end
    catch
        resize!(buffer, old)
        rethrow()
    end
    resize!(buffer, old + n)
    n
end

function _fill_reader!(reader::ProtocolReader)
    _drop_consumed!(reader)
    old = length(reader.buffer)
    n = try
        _read_some_to_buffer!(reader.io, reader.buffer, old + 1, length(reader.scratch),
                              reader.scratch)
    catch err
        err isa EOFError || rethrow()
        throw(err)
    end
    n == 0 && throw(EOFError())
    reader.last = length(reader.buffer)
    nothing
end

function _read_exact_bytes_no_drop(io, n::Int)::Vector{UInt8}
    n == 0 && return UInt8[]
    data = Vector{UInt8}(undef, n)
    unsafe_read(io, pointer(data), UInt(n))
    data
end

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

function _read_payload_trailer_byte!(reader::ProtocolReader)::UInt8
    if isempty(reader.scratch)
        return read(reader.io, UInt8)
    end
    unsafe_read(reader.io, pointer(reader.scratch), UInt(1))
    @inbounds reader.scratch[1]
end

function _read_payload_trailer_no_drop!(io)
    first = read(io, UInt8)
    second = read(io, UInt8)
    first == CRLF_BYTES[1] && second == CRLF_BYTES[2] || throw(ProtocolError("message payload missing CRLF trailer"))
    nothing
end

function _read_payload_trailer_no_drop!(reader::ProtocolReader)
    available = _reader_available(reader)
    if available >= 2
        @inbounds begin
            first = reader.buffer[reader.first]
            second = reader.buffer[reader.first + 1]
        end
        reader.first += 2
    elseif available == 1
        @inbounds first = reader.buffer[reader.first]
        reader.first += 1
        second = _read_payload_trailer_byte!(reader)
    elseif length(reader.scratch) >= 2
        unsafe_read(reader.io, pointer(reader.scratch), UInt(2))
        @inbounds begin
            first = reader.scratch[1]
            second = reader.scratch[2]
        end
    else
        first = _read_payload_trailer_byte!(reader)
        second = _read_payload_trailer_byte!(reader)
    end
    first == CRLF_BYTES[1] && second == CRLF_BYTES[2] || throw(ProtocolError("message payload missing CRLF trailer"))
    nothing
end

function _read_exact_payload(io, n::Int)
    data = _read_payload_bytes_no_drop(io, n)
    _read_payload_trailer_or_error!(io)
    _drop_payload_consumed!(io)
    data
end

function _read_exact_header_payload(io, hsize::Int, total::Int)
    hsize <= total || throw(ProtocolError("header size exceeds message size"))
    header = _read_payload_bytes_no_drop(io, hsize)
    data = _read_payload_bytes_no_drop(io, total - hsize)
    _read_payload_trailer_or_error!(io)
    _drop_payload_consumed!(io)
    header, data
end

function _read_payload_bytes_no_drop(io, n::Int)
    try
        _read_exact_bytes_no_drop(io, n)
    catch err
        err isa EOFError || rethrow()
        throw(ProtocolError("unexpected EOF while reading payload"))
    end
end

function _read_payload_trailer_or_error!(io)
    try
        _read_payload_trailer_no_drop!(io)
    catch err
        err isa EOFError || rethrow()
        throw(ProtocolError("message payload missing CRLF trailer"))
    end
    nothing
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

function _read_msg_line(reader::ProtocolReader, line_first::Int, line_last::Int,
                        max_payload::Int)
    bytes = reader.buffer
    subject_start, subject_end, pos = _next_token(bytes, line_first + 4, line_last)
    subject_start == 0 && _malformed_control_line("MSG", bytes, line_first, line_last)
    sid_start, sid_end, pos = _next_token(bytes, pos, line_last)
    sid_start == 0 && _malformed_control_line("MSG", bytes, line_first, line_last)
    third_start, third_end, pos = _next_token(bytes, pos, line_last)
    third_start == 0 && _malformed_control_line("MSG", bytes, line_first, line_last)
    fourth_start, fourth_end, pos = _next_token(bytes, pos, line_last)
    _has_more_tokens(bytes, pos, line_last) && _malformed_control_line("MSG", bytes, line_first, line_last)

    sid = _parse_int_token(bytes, sid_start, sid_end)
    subject = _cached_subject!(reader, sid, subject_start, subject_end)
    reply = fourth_start == 0 ? nothing : _bytes_string(bytes, third_start, third_end)
    size_start, size_end = fourth_start == 0 ? (third_start, third_end) : (fourth_start, fourth_end)
    size = _validate_payload_size(_parse_int_token(bytes, size_start, size_end), max_payload)
    payload = _read_exact_payload(reader, size)
    _protocol_msg_frame(Msg(subject, reply, payload, nothing, sid))
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

    sid = _parse_int_token(bytes, sid_start, sid_end)
    subject = _cached_subject!(reader, sid, subject_start, subject_end)
    has_reply = fifth_start != 0
    reply = has_reply ? _bytes_string(bytes, third_start, third_end) : nothing
    hsize_start, hsize_end = has_reply ? (fourth_start, fourth_end) : (third_start, third_end)
    total_start, total_end = has_reply ? (fifth_start, fifth_end) : (fourth_start, fourth_end)
    hsize = _validate_header_size(_parse_int_token(bytes, hsize_start, hsize_end), max_header_bytes)
    total = _validate_payload_size(_parse_int_token(bytes, total_start, total_end), max_payload)
    header_bytes, payload = _read_exact_header_payload(reader, hsize, total)
    _validate_headers(header_bytes)
    hdrs = LazyHeaders(header_bytes)
    _protocol_msg_frame(Msg(subject, reply, payload, hdrs, sid, hsize))
end

function _read_control_or_msg(reader::ProtocolReader; max_control_line::Int=DEFAULT_MAX_CONTROL_LINE,
                              max_payload::Int=DEFAULT_MAX_INBOUND_PAYLOAD,
                              max_header_bytes::Int=DEFAULT_MAX_HEADER_BYTES)
    line_range = _readline_crlf_range(reader, max_control_line)
    line_first = first(line_range)
    line_last = last(line_range)
    line_last >= line_first || throw(ProtocolError("empty protocol line"))
    bytes = reader.buffer
    line_len = line_last - line_first + 1
    command = @inbounds bytes[line_first]
    @inbounds begin
        if command == UInt8('M')
            if line_len >= 4 &&
                    bytes[line_first + 1] == UInt8('S') &&
                    bytes[line_first + 2] == UInt8('G') &&
                    bytes[line_first + 3] == UInt8(' ')
                return _read_msg_line(reader, line_first, line_last, max_payload)
            end
        elseif command == UInt8('H')
            if line_len >= 5 &&
                    bytes[line_first + 1] == UInt8('M') &&
                    bytes[line_first + 2] == UInt8('S') &&
                    bytes[line_first + 3] == UInt8('G') &&
                    bytes[line_first + 4] == UInt8(' ')
                return _read_hmsg_line(reader, line_first, line_last, max_payload, max_header_bytes)
            end
        elseif command == UInt8('I')
            if line_len >= 5 &&
                    bytes[line_first + 1] == UInt8('N') &&
                    bytes[line_first + 2] == UInt8('F') &&
                    bytes[line_first + 3] == UInt8('O') &&
                    bytes[line_first + 4] == UInt8(' ')
                info = _server_info(@view bytes[line_first + 5:line_last])
                _drop_consumed!(reader)
                return _protocol_info_frame(info)
            end
        elseif command == UInt8('P')
            if line_len == 4 &&
                    bytes[line_first + 1] == UInt8('I') &&
                    bytes[line_first + 2] == UInt8('N') &&
                    bytes[line_first + 3] == UInt8('G')
                _drop_consumed!(reader)
                return _protocol_control_frame(:PING)
            elseif line_len == 4 &&
                    bytes[line_first + 1] == UInt8('O') &&
                    bytes[line_first + 2] == UInt8('N') &&
                    bytes[line_first + 3] == UInt8('G')
                _drop_consumed!(reader)
                return _protocol_control_frame(:PONG)
            end
        elseif command == UInt8('+')
            if line_len == 3 &&
                    bytes[line_first + 1] == UInt8('O') &&
                    bytes[line_first + 2] == UInt8('K')
                _drop_consumed!(reader)
                return _protocol_control_frame(:OK)
            end
        elseif command == UInt8('-')
            if line_len >= 4 &&
                    bytes[line_first + 1] == UInt8('E') &&
                    bytes[line_first + 2] == UInt8('R') &&
                    bytes[line_first + 3] == UInt8('R')
                msg = line_len >= 6 ? strip(_bytes_string(bytes, line_first + 5, line_last)) : ""
                msg = String(strip(String(msg), ['\'', ' ']))
                _drop_consumed!(reader)
                return _protocol_err_frame(msg)
            end
        end
    end
    line = _bytes_string(bytes, line_first, line_last)
    throw(ProtocolError("unknown protocol line: $line"))
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

function _validate_header_protocol_line(raw::AbstractVector{UInt8}, line_first::Int, line_last::Int)
    _ascii_startswith(raw, line_first, line_last, "NATS/1.0") || throw(ProtocolError("invalid NATS header block"))
    nothing
end

function _validate_headers(raw::AbstractVector{UInt8})
    isempty(raw) && return nothing
    raw_first = firstindex(raw)
    raw_last = lastindex(raw)

    newline = _find_byte(raw, UInt8('\n'), raw_first, raw_last)
    isnothing(newline) && throw(ProtocolError("NATS header block missing terminator"))
    protocol_end = newline - 1
    protocol_end >= raw_first && raw[protocol_end] == UInt8('\r') && (protocol_end -= 1)
    _validate_header_protocol_line(raw, raw_first, protocol_end)

    pos = newline + 1
    while pos <= raw_last
        newline = _find_byte(raw, UInt8('\n'), pos, raw_last)
        isnothing(newline) && throw(ProtocolError("NATS header block missing terminator"))
        line_end = newline - 1
        line_end >= pos && raw[line_end] == UInt8('\r') && (line_end -= 1)
        if line_end < pos
            newline == raw_last && return nothing
            throw(ProtocolError("NATS header block contains trailing bytes"))
        end

        colon = _find_byte(raw, UInt8(':'), pos, line_end)
        isnothing(colon) && throw(ProtocolError("malformed NATS header line"))
        pos = newline + 1
    end
    throw(ProtocolError("NATS header block missing terminator"))
end

function _parse_headers(raw::AbstractVector{UInt8})
    h = Headers()
    isempty(raw) && return h
    raw_first = firstindex(raw)
    raw_last = lastindex(raw)

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
        if line_end < pos
            newline == raw_last && return h
            throw(ProtocolError("NATS header block contains trailing bytes"))
        end

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
    throw(ProtocolError("NATS header block missing terminator"))
end

function _lazy_header_protocol_line(raw::AbstractVector{UInt8})
    isempty(raw) && return 1, 0
    raw_first = firstindex(raw)
    raw_last = lastindex(raw)
    newline = _find_byte(raw, UInt8('\n'), raw_first, raw_last)
    isnothing(newline) && return 1, 0
    line_end = newline - 1
    line_end >= raw_first && raw[line_end] == UInt8('\r') && (line_end -= 1)
    raw_first, line_end
end

function _lazy_status_header(headers::LazyHeaders)
    raw = headers.raw
    line_first, line_last = _lazy_header_protocol_line(raw)
    line_first <= line_last || return nothing
    _ascii_startswith(raw, line_first, line_last, "NATS/1.0") || return nothing
    pos = _skip_hspace(raw, line_first + ncodeunits("NATS/1.0"), line_last)
    pos <= line_last || return nothing
    status_start, status_end, _ = _next_token(raw, pos, line_last)
    _all_digits(raw, status_start, status_end) || return nothing
    _parse_int_token(raw, status_start, status_end)
end

function _lazy_status_description(headers::LazyHeaders)
    raw = headers.raw
    line_first, line_last = _lazy_header_protocol_line(raw)
    line_first <= line_last || return ""
    _ascii_startswith(raw, line_first, line_last, "NATS/1.0") || return ""
    pos = _skip_hspace(raw, line_first + ncodeunits("NATS/1.0"), line_last)
    pos <= line_last || return ""
    status_start, status_end, pos = _next_token(raw, pos, line_last)
    _all_digits(raw, status_start, status_end) || return ""
    description_start = _skip_hspace(raw, pos, line_last)
    description_start <= line_last ? _bytes_string(raw, description_start, line_last) : ""
end

function _ascii_equal_foldcase(a::AbstractString, b::AbstractString)::Bool
    ncodeunits(a) == ncodeunits(b) || return false
    @inbounds for i in 1:ncodeunits(a)
        _ascii_lower(codeunit(a, i)) == _ascii_lower(codeunit(b, i)) || return false
    end
    true
end

function _header_key_matches(raw::AbstractVector{UInt8}, first::Int, last::Int, key::String)::Bool
    last - first + 1 == ncodeunits(key) || return false
    @inbounds for i in 1:ncodeunits(key)
        _ascii_lower(raw[first + i - 1]) == _ascii_lower(codeunit(key, i)) || return false
    end
    true
end

function _lazy_description_header(headers::LazyHeaders)
    raw = headers.raw
    line_first, line_last = _lazy_header_protocol_line(raw)
    line_first <= line_last || return nothing
    _ascii_startswith(raw, line_first, line_last, "NATS/1.0") || return nothing
    pos = _skip_hspace(raw, line_first + ncodeunits("NATS/1.0"), line_last)
    pos <= line_last || return nothing
    status_start, status_end, pos = _next_token(raw, pos, line_last)
    _all_digits(raw, status_start, status_end) || return nothing
    description_start = _skip_hspace(raw, pos, line_last)
    description_start <= line_last ? _bytes_string(raw, description_start, line_last) : nothing
end

function _lazy_header_first(headers::LazyHeaders, key::AbstractString)
    key_string = String(key)
    if _ascii_equal_foldcase(key_string, "Status")
        status = _lazy_status_header(headers)
        !isnothing(status) && return string(status)
    elseif _ascii_equal_foldcase(key_string, "Description")
        description = _lazy_description_header(headers)
        !isnothing(description) && return description
    end

    raw = headers.raw
    raw_first = firstindex(raw)
    raw_last = lastindex(raw)
    newline = _find_byte(raw, UInt8('\n'), raw_first, raw_last)
    isnothing(newline) && return nothing
    pos = newline + 1
    while pos <= raw_last
        newline = _find_byte(raw, UInt8('\n'), pos, raw_last)
        isnothing(newline) && return nothing
        line_end = newline - 1
        line_end >= pos && raw[line_end] == UInt8('\r') && (line_end -= 1)
        line_end < pos && return nothing

        colon = _find_byte(raw, UInt8(':'), pos, line_end)
        if !isnothing(colon) && colon > pos && _header_key_matches(raw, pos, colon - 1, key_string)
            value_start = _skip_hspace(raw, colon + 1, line_end)
            return value_start <= line_end ? _bytes_string(raw, value_start, line_end) : ""
        end
        pos = newline + 1
    end
    nothing
end

function _status_header(msg::AbstractMsg)
    msg.headers isa LazyHeaders && return _lazy_status_header(msg.headers)
    value = header(msg, "Status")
    !isnothing(value) && !isempty(value) && all(isdigit, value) && return parse(Int, value)
    nothing
end

function _status_description(msg::AbstractMsg)
    msg.headers isa LazyHeaders && return _lazy_status_description(msg.headers)
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
    out = Vector{UInt8}(undef, _headers_wire_size(headers))
    pos = 1
    pos = _copy_codeunits!(out, pos, "NATS/1.0")
    pos = _copy_bytes!(out, pos, CRLF_BYTES)
    for (k, values) in headers
        for v in values
            key, value = _validate_header_pair(k, v)
            pos = _copy_codeunits!(out, pos, key)
            pos = _copy_byte!(out, pos, UInt8(':'))
            pos = _copy_byte!(out, pos, UInt8(' '))
            pos = _copy_codeunits!(out, pos, value)
            pos = _copy_bytes!(out, pos, CRLF_BYTES)
        end
    end
    pos = _copy_bytes!(out, pos, CRLF_BYTES)
    pos == length(out) + 1 || throw(AssertionError("header frame size mismatch"))
    out
end

struct PublishFrame{P<:AbstractVector{UInt8},H<:AbstractVector{UInt8}} <: _AbstractPublishFrame
    subject::String
    reply::Union{String,Nothing}
    payload::P
    headers::H

    function PublishFrame(subject::AbstractString, reply::Union{AbstractString,Nothing},
                          data, headers)
        subject = _validate_publish_subject(subject)
        reply = isnothing(reply) ? nothing : _validate_publish_subject(reply)
        payload = _prepared_payload_bytes(data)
        header_bytes = _prepared_header_bytes(headers)
        new{typeof(payload),typeof(header_bytes)}(subject, reply, payload, header_bytes)
    end
end

PublishFrame(subject::AbstractString, data=nothing;
             reply::Union{AbstractString,Nothing}=nothing,
             headers=nothing) =
    PublishFrame(subject, reply, data, headers)

struct _PublishFrame{P<:AbstractVector{UInt8},H<:AbstractVector{UInt8}} <: _AbstractPublishFrame
    subject::String
    reply::Union{String,Nothing}
    payload::P
    headers::H
end

function _publish_frame(subject::AbstractString, reply::Union{AbstractString,Nothing},
                        data, headers)
    subject = _validate_publish_subject(subject)
    reply = isnothing(reply) ? nothing : _validate_publish_subject(reply)
    _PublishFrame(subject, reply, _payload_bytes(data), _publish_header_bytes(headers))
end

_pub_payload_size(payload::AbstractVector{UInt8}, hdr::AbstractVector{UInt8}) = length(hdr) + length(payload)
_pub_payload_size(frame::_AbstractPublishFrame) = _pub_payload_size(frame.payload, frame.headers)

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

function _pub_cmd(frame::_AbstractPublishFrame)::Vector{UInt8}
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

function _write_pub_frame_direct(io, frame::_AbstractPublishFrame, scratch::Vector{UInt8})
    write(io, _pub_prefix!(scratch, frame))
    isempty(frame.headers) || write(io, frame.headers)
    write(io, frame.payload)
    write(io, CRLF)
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
