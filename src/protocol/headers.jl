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

function _header_protocol_status(raw::AbstractVector{UInt8}, line_first::Int,
                                 line_last::Int)::Tuple{Int,Int,Int}
    pos = _header_protocol_status_start(raw, line_first, line_last)
    isnothing(pos) && return 0, 1, 0
    pos > line_last && return 0, 1, 0
    status_start, status_end, pos = _next_token(raw, pos, line_last)
    _all_digits(raw, status_start, status_end) || return 0, 1, 0
    status = _parse_int_token(raw, status_start, status_end)
    description_start = _skip_hspace(raw, pos, line_last)
    description_start <= line_last ? (status, description_start, line_last) : (status, 1, 0)
end

function _header_protocol_status_start(raw::AbstractVector{UInt8}, line_first::Int,
                                       line_last::Int)
    _ascii_startswith(raw, line_first, line_last, "NATS/1.0") || return nothing
    pos = line_first + ncodeunits("NATS/1.0")
    pos > line_last && return pos
    @inbounds _is_hspace(raw[pos]) || return nothing
    _skip_hspace(raw, pos, line_last)
end

function _validate_header_protocol_status(raw::AbstractVector{UInt8}, line_first::Int,
                                          line_last::Int)::Tuple{Int,Int,Int}
    pos = _header_protocol_status_start(raw, line_first, line_last)
    isnothing(pos) && throw(ProtocolError("invalid NATS header block"))
    pos > line_last && return 0, 1, 0
    status_start, status_end, pos = _next_token(raw, pos, line_last)
    _all_digits(raw, status_start, status_end) ||
        throw(ProtocolError("invalid NATS header block"))
    status = _parse_int_token(raw, status_start, status_end)
    description_start = _skip_hspace(raw, pos, line_last)
    description_start <= line_last ? (status, description_start, line_last) : (status, 1, 0)
end

function _parse_header_protocol_line!(headers::Headers, raw::AbstractVector{UInt8}, line_first::Int, line_last::Int)
    status, description_start, description_end =
        _validate_header_protocol_status(raw, line_first, line_last)
    if status != 0
        headers["Status"] = [string(status)]
        description_start <= description_end &&
            (headers["Description"] = [_bytes_string(raw, description_start, description_end)])
    end
    headers
end

function _validate_header_protocol_line(raw::AbstractVector{UInt8}, line_first::Int, line_last::Int)
    _validate_header_protocol_status(raw, line_first, line_last)
    nothing
end

function _validate_raw_header_line(raw::AbstractVector{UInt8}, line_first::Int, colon::Int, line_last::Int)
    line_first < colon || throw(ProtocolError("header key cannot be empty"))
    _valid_header_field_name(raw, line_first, colon - 1) ||
        throw(ProtocolError("header key contains an invalid character"))
    _valid_header_value(raw, colon + 1, line_last) ||
        throw(ProtocolError("header value contains an invalid character"))
    nothing
end

function _validate_headers(raw::AbstractVector{UInt8})::Tuple{Int,Int,Int}
    isempty(raw) && return 0, 1, 0
    raw_first = firstindex(raw)
    raw_last = lastindex(raw)

    newline = _find_byte(raw, UInt8('\n'), raw_first, raw_last)
    isnothing(newline) && throw(ProtocolError("NATS header block missing terminator"))
    protocol_end = newline - 1
    protocol_end >= raw_first && raw[protocol_end] == UInt8('\r') && (protocol_end -= 1)
    status, description_first, description_last =
        _validate_header_protocol_status(raw, raw_first, protocol_end)

    pos = newline + 1
    while pos <= raw_last
        newline = _find_byte(raw, UInt8('\n'), pos, raw_last)
        isnothing(newline) && throw(ProtocolError("NATS header block missing terminator"))
        line_end = newline - 1
        line_end >= pos && raw[line_end] == UInt8('\r') && (line_end -= 1)
        if line_end < pos
            newline == raw_last && return status, description_first, description_last
            throw(ProtocolError("NATS header block contains trailing bytes"))
        end

        colon = _find_byte(raw, UInt8(':'), pos, line_end)
        isnothing(colon) && throw(ProtocolError("malformed NATS header line"))
        _validate_raw_header_line(raw, pos, colon, line_end)
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
        _validate_raw_header_line(raw, pos, colon, line_end)
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
    pos = _header_protocol_status_start(raw, line_first, line_last)
    isnothing(pos) && return nothing
    pos <= line_last || return nothing
    status_start, status_end, _ = _next_token(raw, pos, line_last)
    _all_digits(raw, status_start, status_end) || return nothing
    _parse_int_token(raw, status_start, status_end)
end

function _lazy_status_description(headers::LazyHeaders)
    raw = headers.raw
    description_start, description_end = _lazy_status_description_range(headers)
    description_start <= description_end ? _bytes_string(raw, description_start, description_end) : ""
end

function _lazy_status_description_range(headers::LazyHeaders)::Tuple{Int,Int}
    raw = headers.raw
    line_first, line_last = _lazy_header_protocol_line(raw)
    line_first <= line_last || return 1, 0
    pos = _header_protocol_status_start(raw, line_first, line_last)
    isnothing(pos) && return 1, 0
    pos <= line_last || return 1, 0
    status_start, status_end, pos = _next_token(raw, pos, line_last)
    _all_digits(raw, status_start, status_end) || return 1, 0
    description_start = _skip_hspace(raw, pos, line_last)
    description_start <= line_last ? (description_start, line_last) : (1, 0)
end

_raw_status_header(headers::RawHeaders) = headers.status == 0 ? nothing : headers.status
_raw_status_description_range(headers::RawHeaders)::Tuple{Int,Int} =
    (headers.description_first, headers.description_last)

function _raw_status_description(headers::RawHeaders)
    raw = headers.raw
    description_start, description_end = _raw_status_description_range(headers)
    description_start <= description_end ? _bytes_string(raw, description_start, description_end) : ""
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

function _scan_raw_header_first(raw::AbstractVector{UInt8}, key::String)
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
        if !isnothing(colon) && colon > pos && _header_key_matches(raw, pos, colon - 1, key)
            value_start = _skip_hspace(raw, colon + 1, line_end)
            return value_start <= line_end ? _bytes_string(raw, value_start, line_end) : ""
        end
        pos = newline + 1
    end
    nothing
end

function _lazy_description_header(headers::LazyHeaders)
    raw = headers.raw
    line_first, line_last = _lazy_header_protocol_line(raw)
    line_first <= line_last || return nothing
    pos = _header_protocol_status_start(raw, line_first, line_last)
    isnothing(pos) && return nothing
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

    _scan_raw_header_first(headers.raw, key_string)
end

function _raw_header_first(headers::RawHeaders, key::AbstractString)
    key_string = String(key)
    if _ascii_equal_foldcase(key_string, "Status")
        status = _raw_status_header(headers)
        !isnothing(status) && return string(status)
    elseif _ascii_equal_foldcase(key_string, "Description")
        description_start, description_end = _raw_status_description_range(headers)
        if description_start <= description_end
            return _bytes_string(headers.raw, description_start, description_end)
        end
    end

    _scan_raw_header_first(headers.raw, key_string)
end

function _status_header(msg::AbstractMsg)
    msg.headers isa LazyHeaders && return _lazy_status_header(msg.headers)
    if msg.headers isa RawHeaders
        status = _raw_status_header(msg.headers)
        !isnothing(status) && return status
    end
    value = header(msg, "Status")
    !isnothing(value) && !isempty(value) && all(isdigit, value) && return parse(Int, value)
    nothing
end

function _status_description(msg::AbstractMsg)
    msg.headers isa LazyHeaders && return _lazy_status_description(msg.headers)
    msg.headers isa RawHeaders && return _raw_status_description(msg.headers)
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

function _valid_header_field_name(bytes::AbstractVector{UInt8}, first::Int, last::Int)::Bool
    first <= last || return false
    @inbounds for pos in first:last
        _valid_header_field_name_char(bytes[pos]) || return false
    end
    true
end

_valid_header_field_name(bytes::AbstractVector{UInt8}) =
    _valid_header_field_name(bytes, firstindex(bytes), lastindex(bytes))

function _valid_header_value(bytes::AbstractVector{UInt8}, first::Int, last::Int)::Bool
    @inbounds for pos in first:last
        byte = bytes[pos]
        (byte == UInt8('\r') || byte == UInt8('\n')) && return false
    end
    true
end

_valid_header_value(bytes::AbstractVector{UInt8}) =
    _valid_header_value(bytes, firstindex(bytes), lastindex(bytes))

function _validate_header_pair(key::AbstractString, value::AbstractString)
    k = String(key)
    isempty(k) && throw(ArgumentError("header key cannot be empty"))
    _valid_header_field_name(codeunits(k)) ||
        throw(ArgumentError("header key contains an invalid character"))
    v = String(value)
    if !_valid_header_value(codeunits(v))
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
