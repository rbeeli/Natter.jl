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

function _replace_reader_buffer!(reader::ProtocolReader, available::Int)
    buffer = Vector{UInt8}(undef, available)
    available > 0 && copyto!(buffer, 1, reader.buffer, reader.first, available)
    reader.buffer = buffer
    reader.first = 1
    reader.last = available
    nothing
end

function _drop_consumed!(reader::ProtocolReader)
    reader.first == 1 && return nothing
    available = _reader_available(reader)
    if available <= 0
        if length(reader.buffer) > reader.shrink_threshold
            reader.buffer = UInt8[]
        else
            empty!(reader.buffer)
        end
        reader.first = 1
        reader.last = 0
    elseif reader.first > _READER_COMPACT_MIN_PREFIX && reader.first > available
        if length(reader.buffer) > reader.shrink_threshold && available <= reader.shrink_threshold
            _replace_reader_buffer!(reader, available)
        else
            copyto!(reader.buffer, 1, reader.buffer, reader.first, available)
            resize!(reader.buffer, available)
            reader.first = 1
            reader.last = available
        end
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

function _readline_crlf_bounds(reader::ProtocolReader, max_control_line::Int)::Tuple{Int,Int}
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
            return line_first, data_end
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

struct _NoBorrowedPayloads end
(::_NoBorrowedPayloads)(::Int)::Bool = false

struct _ProtocolMessageRoute
    borrow_payload::Bool
end

@inline _message_route(value::Bool) = _ProtocolMessageRoute(value)
@inline _message_route(route) = route
@inline _resolve_message_route(route_resolver, sid::Int) = _message_route(route_resolver(sid))
@inline _route_borrow_payload(route::_ProtocolMessageRoute)::Bool = route.borrow_payload
@inline _route_borrow_payload(_route)::Bool = false
@inline _dispatch_protocol_msg(msg_handler, _route, msg::_ProtocolMsg) = msg_handler(msg)

function _ensure_payload_buffered!(reader::ProtocolReader, n::Int)
    while _reader_available(reader) < n
        try
            _fill_reader!(reader)
        catch err
            err isa EOFError || rethrow()
            throw(ProtocolError("unexpected EOF while reading payload"))
        end
    end
    nothing
end

function _borrow_exact_payload(reader::ProtocolReader, n::Int)
    _ensure_payload_buffered!(reader, n + 2)
    payload_first = reader.first
    payload_last = payload_first + n - 1
    trailer_first = payload_last + 1
    payload = @view reader.buffer[payload_first:payload_last]
    @inbounds begin
        first = reader.buffer[trailer_first]
        second = reader.buffer[trailer_first + 1]
    end
    first == CRLF_BYTES[1] && second == CRLF_BYTES[2] ||
        throw(ProtocolError("message payload missing CRLF trailer"))
    reader.first = trailer_first + 2
    payload
end

function _borrow_exact_header_payload(reader::ProtocolReader, hsize::Int, total::Int)
    hsize <= total || throw(ProtocolError("header size exceeds message size"))
    _ensure_payload_buffered!(reader, total + 2)
    header_first = reader.first
    payload_first = header_first + hsize
    payload_last = header_first + total - 1
    trailer_first = payload_last + 1
    @inbounds begin
        first = reader.buffer[trailer_first]
        second = reader.buffer[trailer_first + 1]
    end
    first == CRLF_BYTES[1] && second == CRLF_BYTES[2] ||
        throw(ProtocolError("message payload missing CRLF trailer"))
    header = @view reader.buffer[header_first:(payload_first - 1)]
    payload = @view reader.buffer[payload_first:payload_last]
    reader.first = trailer_first + 2
    header, payload
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

function _parse_unsigned_int_token(bytes::AbstractVector{UInt8}, first::Int, last::Int,
                                   field::AbstractString)::Int
    first <= last || throw(ProtocolError("malformed $field field in protocol line"))
    value = 0
    @inbounds begin
        pos = first
        while pos <= last
            byte = bytes[pos]
            UInt8('0') <= byte <= UInt8('9') ||
                throw(ProtocolError("$field field must be an unsigned integer"))
            digit = Int(byte - UInt8('0'))
            value <= (typemax(Int) - digit) ÷ 10 ||
                throw(ProtocolError("$field field exceeds Int range"))
            value = value * 10 + digit
            pos += 1
        end
    end
    value
end

function _parse_positive_int_token(bytes::AbstractVector{UInt8}, first::Int, last::Int,
                                   field::AbstractString)::Int
    value = _parse_unsigned_int_token(bytes, first, last, field)
    value > 0 || throw(ProtocolError("$field field must be a positive integer"))
    value
end

function _malformed_control_line(kind::AbstractString, bytes::AbstractVector{UInt8}, first::Int, last::Int)
    throw(ProtocolError("malformed $kind control line: $(_bytes_string(bytes, first, last))"))
end

function _read_msg_line_handle(reader::ProtocolReader, line_first::Int, line_last::Int,
                               max_payload::Int, route_resolver, msg_handler)
    bytes = reader.buffer
    subject_start, subject_end, pos = _next_token(bytes, line_first + 4, line_last)
    subject_start == 0 && _malformed_control_line("MSG", bytes, line_first, line_last)
    sid_start, sid_end, pos = _next_token(bytes, pos, line_last)
    sid_start == 0 && _malformed_control_line("MSG", bytes, line_first, line_last)
    third_start, third_end, pos = _next_token(bytes, pos, line_last)
    third_start == 0 && _malformed_control_line("MSG", bytes, line_first, line_last)
    fourth_start, fourth_end, pos = _next_token(bytes, pos, line_last)
    _has_more_tokens(bytes, pos, line_last) && _malformed_control_line("MSG", bytes, line_first, line_last)

    sid = _parse_positive_int_token(bytes, sid_start, sid_end, "sid")
    has_reply = fourth_start != 0
    size_start, size_end = fourth_start == 0 ? (third_start, third_end) : (fourth_start, fourth_end)
    size = _validate_payload_size(_parse_unsigned_int_token(bytes, size_start, size_end, "payload size"), max_payload)
    route = _resolve_message_route(route_resolver, sid)
    borrowed = _route_borrow_payload(route)
    reply_start, reply_end = has_reply ? (third_start, third_end) : (0, -1)
    _try_handle_msg_control(msg_handler, route, reader, sid, subject_start, subject_end,
                            reply_start, reply_end, size, borrowed) && return nothing

    subject = _cached_subject!(reader, sid, subject_start, subject_end)
    reply = has_reply ? _bytes_string(bytes, third_start, third_end) : nothing
    if borrowed
        payload = _borrow_exact_payload(reader, size)
        return _dispatch_protocol_msg(msg_handler, route,
                                      BorrowedMsg(subject, reply, payload, nothing, sid, 0))
    end
    payload = _read_exact_payload(reader, size)
    _dispatch_protocol_msg(msg_handler, route, Msg(subject, reply, payload, nothing, sid))
end

_read_msg_line(reader::ProtocolReader, line_first::Int, line_last::Int,
               max_payload::Int, borrow_payload) =
    _read_msg_line_handle(reader, line_first, line_last, max_payload, borrow_payload,
                          _ProtocolMsgFrameHandler())

function _try_handle_msg_control(_handler, _route, _reader::ProtocolReader, _sid::Int,
                                 _subject_start::Int, _subject_end::Int,
                                 _reply_start::Int, _reply_end::Int,
                                 _size::Int, _borrowed::Bool)::Bool
    false
end

function _try_handle_hmsg_control(_handler, _route, _reader::ProtocolReader, _sid::Int,
                                  _subject_start::Int, _subject_end::Int,
                                  _reply_start::Int, _reply_end::Int,
                                  _hsize::Int, _total::Int, _borrowed::Bool)::Bool
    false
end

function _read_hmsg_line_handle(reader::ProtocolReader, line_first::Int, line_last::Int,
                                max_payload::Int, max_header_bytes::Int, route_resolver,
                                msg_handler)
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

    sid = _parse_positive_int_token(bytes, sid_start, sid_end, "sid")
    has_reply = fifth_start != 0
    hsize_start, hsize_end = has_reply ? (fourth_start, fourth_end) : (third_start, third_end)
    total_start, total_end = has_reply ? (fifth_start, fifth_end) : (fourth_start, fourth_end)
    hsize = _validate_header_size(_parse_unsigned_int_token(bytes, hsize_start, hsize_end, "header size"), max_header_bytes)
    total = _validate_payload_size(_parse_unsigned_int_token(bytes, total_start, total_end, "payload size"), max_payload)
    route = _resolve_message_route(route_resolver, sid)
    borrowed = _route_borrow_payload(route)
    reply_start, reply_end = has_reply ? (third_start, third_end) : (0, -1)
    _try_handle_hmsg_control(msg_handler, route, reader, sid, subject_start, subject_end,
                             reply_start, reply_end, hsize, total, borrowed) && return nothing

    subject = _cached_subject!(reader, sid, subject_start, subject_end)
    reply = has_reply ? _bytes_string(bytes, third_start, third_end) : nothing
    header_bytes, payload = borrowed ? _borrow_exact_header_payload(reader, hsize, total) :
                            _read_exact_header_payload(reader, hsize, total)
    status, description_first, description_last = _validate_headers(header_bytes)
    hdrs = RawHeaders(header_bytes, status, description_first, description_last)
    msg = borrowed ? BorrowedMsg(subject, reply, payload, hdrs, sid, hsize) :
          Msg(subject, reply, payload, hdrs, sid, hsize)
    _dispatch_protocol_msg(msg_handler, route, msg)
end

_read_hmsg_line(reader::ProtocolReader, line_first::Int, line_last::Int,
                max_payload::Int, max_header_bytes::Int, borrow_payload) =
    _read_hmsg_line_handle(reader, line_first, line_last, max_payload, max_header_bytes,
                           borrow_payload, _ProtocolMsgFrameHandler())

struct _ProtocolMsgFrameHandler end
@inline (::_ProtocolMsgFrameHandler)(msg::_ProtocolMsg) = _protocol_msg_frame(msg)

function _read_control_or_msg_impl(reader::ProtocolReader, max_control_line::Int,
                                   max_payload::Int, max_header_bytes::Int,
                                   route_resolver, msg_handler)
    line_first, line_last = _readline_crlf_bounds(reader, max_control_line)
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
                return _read_msg_line_handle(reader, line_first, line_last, max_payload,
                                             route_resolver, msg_handler)
            end
        elseif command == UInt8('H')
            if line_len >= 5 &&
                    bytes[line_first + 1] == UInt8('M') &&
                    bytes[line_first + 2] == UInt8('S') &&
                    bytes[line_first + 3] == UInt8('G') &&
                    bytes[line_first + 4] == UInt8(' ')
                return _read_hmsg_line_handle(reader, line_first, line_last, max_payload,
                                              max_header_bytes, route_resolver, msg_handler)
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

function _read_control_or_msg(reader::ProtocolReader; max_control_line::Int=DEFAULT_MAX_CONTROL_LINE,
                              max_payload::Int=DEFAULT_MAX_INBOUND_PAYLOAD,
                              max_header_bytes::Int=DEFAULT_MAX_HEADER_BYTES,
                              borrow_payload=_NoBorrowedPayloads())
    _read_control_or_msg_impl(reader, max_control_line, max_payload, max_header_bytes,
                              borrow_payload, _ProtocolMsgFrameHandler())
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

function _read_control_or_msg(reader::ProtocolReader, opts::ConnectOptions, route_resolver)
    _read_control_or_msg(reader;
                         max_control_line=opts.max_control_line,
                         max_payload=opts.max_inbound_payload,
                         max_header_bytes=opts.max_header_bytes,
                         borrow_payload=route_resolver)
end

function _read_control_or_msg_dispatch(reader::ProtocolReader, opts::ConnectOptions,
                                       route_resolver, msg_handler)
    _read_control_or_msg_impl(reader, opts.max_control_line, opts.max_inbound_payload,
                              opts.max_header_bytes, route_resolver, msg_handler)
end
