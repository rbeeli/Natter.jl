function _json_dict(bytes_or_string)
    obj = JSON3.read(bytes_or_string)
    Dict{String,Any}(String(k) => v for (k, v) in pairs(obj))
end

function _read_exact_bytes(io, n::Int)::Vector{UInt8}
    n == 0 && return UInt8[]
    data = Vector{UInt8}(undef, n)
    unsafe_read(io, pointer(data), UInt(n))
    data
end

function _readline_crlf(io)
    raw = UInt8[]
    while true
        byte = try
            only(_read_exact_bytes(io, 1))
        catch err
            err isa EOFError || rethrow()
            break
        end
        push!(raw, byte)
        byte == UInt8('\n') && break
    end
    isempty(raw) && throw(ProtocolError("unexpected EOF while reading protocol line"))
    if last(raw) != UInt8('\n')
        throw(ProtocolError("protocol line missing LF"))
    end
    if length(raw) >= 2 && raw[end - 1] == UInt8('\r')
        resize!(raw, length(raw) - 2)
    else
        resize!(raw, length(raw) - 1)
    end
    String(raw)
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

function _read_control_or_msg(io)
    line = _readline_crlf(io)
    isempty(line) && throw(ProtocolError("empty protocol line"))
    up = uppercase(line)
    if startswith(up, "INFO ")
        return (:INFO, _json_dict(SubString(line, 6)))
    elseif up == "PING"
        return (:PING, nothing)
    elseif up == "PONG"
        return (:PONG, nothing)
    elseif up == "+OK"
        return (:OK, nothing)
    elseif startswith(up, "-ERR")
        msg = lastindex(line) >= 6 ? strip(SubString(line, 6:lastindex(line))) : ""
        msg = strip(String(msg), ['\'', ' '])
        return (:ERR, msg)
    elseif startswith(up, "MSG ")
        parts = split(line)
        length(parts) in (4, 5) || throw(ProtocolError("malformed MSG control line: $line"))
        subject = String(parts[2])
        sid = parse(Int, parts[3])
        reply = length(parts) == 5 ? String(parts[4]) : nothing
        size = parse(Int, parts[end])
        payload = _read_exact_payload(io, size)
        return (:MSG, Msg(subject, reply, payload; client=nothing, sid=sid))
    elseif startswith(up, "HMSG ")
        parts = split(line)
        length(parts) in (5, 6) || throw(ProtocolError("malformed HMSG control line: $line"))
        subject = String(parts[2])
        sid = parse(Int, parts[3])
        reply = length(parts) == 6 ? String(parts[4]) : nothing
        hsize = parse(Int, parts[end - 1])
        total = parse(Int, parts[end])
        payload = _read_exact_payload(io, total)
        hsize <= total || throw(ProtocolError("header size exceeds message size"))
        hdrs = _parse_headers(payload[1:hsize])
        data = payload[(hsize + 1):end]
        return (:MSG, Msg(subject, reply, data; headers=hdrs, client=nothing, sid=sid))
    else
        throw(ProtocolError("unknown protocol line: $line"))
    end
end

function _parse_headers(raw::Vector{UInt8})
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
    if haskey(msg.headers, "Status")
        value = first(msg.headers["Status"])
        all(isdigit, value) && return parse(Int, value)
    end
    nothing
end

function _status_description(msg::Msg)
    values = get(msg.headers, "Description", String[])
    isempty(values) ? "" : first(values)
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
    isempty(headers) && return UInt8[]
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

function _pub_cmd(subject::String, reply::Union{String,Nothing}, payload::Vector{UInt8}, headers::Headers)
    hdr = _headers_bytes(headers)
    io = IOBuffer()
    if isempty(hdr)
        if isnothing(reply)
            write(io, "PUB $subject $(length(payload))$CRLF")
        else
            write(io, "PUB $subject $reply $(length(payload))$CRLF")
        end
        write(io, payload)
    else
        total = length(hdr) + length(payload)
        if isnothing(reply)
            write(io, "HPUB $subject $(length(hdr)) $total$CRLF")
        else
            write(io, "HPUB $subject $reply $(length(hdr)) $total$CRLF")
        end
        write(io, hdr)
        write(io, payload)
    end
    write(io, CRLF)
    take!(io)
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

function _bytes(data)
    if data === nothing
        UInt8[]
    elseif data isa Vector{UInt8}
        copy(data)
    elseif data isa AbstractVector{UInt8}
        Vector{UInt8}(data)
    elseif data isa AbstractString
        Vector{UInt8}(codeunits(String(data)))
    else
        Vector{UInt8}(codeunits(JSON3.write(data)))
    end
end
