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

function _read_exact_bytes(reader::ProtocolReader, n::Int)::Vector{UInt8}
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
            _drop_consumed!(reader)
        else
            unsafe_read(reader.io, pointer(data, offset), UInt(remaining))
            remaining = 0
        end
    end
    data
end

function _readline_crlf(reader::ProtocolReader, max_control_line::Int)
    while true
        newline = findnext(==(UInt8('\n')), reader.buffer, reader.first)
        if !isnothing(newline)
            line_bytes = newline - reader.first + 1
            line_bytes > max_control_line && throw(ProtocolError("protocol line exceeds configured limit of $max_control_line bytes"))
            data_end = newline - 1
            if data_end >= reader.first && reader.buffer[data_end] == UInt8('\r')
                data_end -= 1
            end
            line = data_end >= reader.first ? String(@view reader.buffer[reader.first:data_end]) : ""
            reader.first = newline + 1
            _drop_consumed!(reader)
            return line
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

function _read_exact_payload(io, n::Int)
    data = try
        _read_exact_bytes(io, n)
    catch err
        err isa EOFError || rethrow()
        throw(ProtocolError("unexpected EOF while reading payload"))
    end
    trailer = try
        _read_exact_bytes(io, 2)
    catch err
        err isa EOFError || rethrow()
        throw(ProtocolError("message payload missing CRLF trailer"))
    end
    trailer == CRLF_BYTES || throw(ProtocolError("message payload missing CRLF trailer"))
    data
end

function _read_control_or_msg(io; max_control_line::Int=DEFAULT_MAX_CONTROL_LINE,
                              max_payload::Int=DEFAULT_MAX_INBOUND_PAYLOAD,
                              max_header_bytes::Int=DEFAULT_MAX_HEADER_BYTES)
    line = _readline_crlf(io, max_control_line)
    isempty(line) && throw(ProtocolError("empty protocol line"))
    if _ascii_startswith_ci(line, "INFO ")
        return (:INFO, _server_info(SubString(line, 6)))
    elseif _ascii_eq_ci(line, "PING")
        return (:PING, nothing)
    elseif _ascii_eq_ci(line, "PONG")
        return (:PONG, nothing)
    elseif _ascii_eq_ci(line, "+OK")
        return (:OK, nothing)
    elseif _ascii_startswith_ci(line, "-ERR")
        msg = lastindex(line) >= 6 ? strip(SubString(line, 6:lastindex(line))) : ""
        msg = strip(String(msg), ['\'', ' '])
        return (:ERR, msg)
    elseif _ascii_startswith_ci(line, "MSG ")
        parts = split(line)
        length(parts) in (4, 5) || throw(ProtocolError("malformed MSG control line: $line"))
        subject = String(parts[2])
        sid = parse(Int, parts[3])
        reply = length(parts) == 5 ? String(parts[4]) : nothing
        size = _validate_payload_size(parse(Int, parts[end]), max_payload)
        payload = _read_exact_payload(io, size)
        return (:MSG, Msg(subject, reply, payload; client=nothing, sid=sid))
    elseif _ascii_startswith_ci(line, "HMSG ")
        parts = split(line)
        length(parts) in (5, 6) || throw(ProtocolError("malformed HMSG control line: $line"))
        subject = String(parts[2])
        sid = parse(Int, parts[3])
        reply = length(parts) == 6 ? String(parts[4]) : nothing
        hsize = _validate_header_size(parse(Int, parts[end - 1]), max_header_bytes)
        total = _validate_payload_size(parse(Int, parts[end]), max_payload)
        payload = _read_exact_payload(io, total)
        hsize <= total || throw(ProtocolError("header size exceeds message size"))
        hdrs = _parse_headers(@view payload[1:hsize])
        hsize == 0 || deleteat!(payload, 1:hsize)
        return (:MSG, Msg(subject, reply, payload; headers=hdrs, client=nothing, sid=sid))
    else
        throw(ProtocolError("unknown protocol line: $line"))
    end
end

function _read_control_or_msg(io, opts::ConnectOptions)
    _read_control_or_msg(io;
                         max_control_line=opts.max_control_line,
                         max_payload=opts.max_inbound_payload,
                         max_header_bytes=opts.max_header_bytes)
end

function _parse_headers(raw::AbstractVector{UInt8})
    h = Headers()
    isempty(raw) && return h
    text = String(raw)
    lines = split(text, CRLF; keepempty=false)
    isempty(lines) && return h
    protocol_line = first(lines)
    startswith(protocol_line, "NATS/1.0") || throw(ProtocolError("invalid NATS header block"))
    status_parts = split(protocol_line; limit=3)
    if length(status_parts) >= 2 && all(isdigit, String(status_parts[2]))
        h["Status"] = [String(status_parts[2])]
        length(status_parts) >= 3 && (h["Description"] = [String(status_parts[3])])
    end
    for line in Iterators.drop(lines, 1)
        isempty(line) && continue
        idx = findfirst(": ", line)
        if isnothing(idx)
            continue
        end
        key = String(line[firstindex(line):prevind(line, first(idx))])
        value = String(line[nextind(line, last(idx)):lastindex(line)])
        push!(get!(h, key, String[]), value)
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

function _validate_header_pair(key::AbstractString, value::AbstractString)
    k = strip(String(key))
    isempty(k) && throw(ArgumentError("header key cannot be empty"))
    if occursin('\r', k) || occursin('\n', k) || occursin(':', k)
        throw(ArgumentError("header key contains an invalid character"))
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
