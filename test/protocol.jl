using TestItems

@testitem "protocol parser" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    frame = N._read_control_or_msg(IOBuffer(TestHelpers.bytes("INFO {\"server_id\":\"srv\",\"max_payload\":64,\"headers\":true}\r\n")))
    @test frame.op == :INFO
    data = N._protocol_info(frame)
    @test data.max_payload == 64
    @test data.headers == true

    info = N.ServerInfo(; headers=true)
    N._merge_server_info!(info, N.ServerInfo(; connect_urls=["127.0.0.1:4222"]))
    @test info.headers == true
    N._merge_server_info!(info, N.ServerInfo(; headers=false))
    @test info.headers == false

    frame = N._read_control_or_msg(IOBuffer(TestHelpers.bytes("MSG foo 2 _INBOX.1 5\r\nhello\r\n")))
    @test frame.op == :MSG
    msg = N._protocol_msg(frame)
    @test msg.subject == "foo"
    @test msg.sid == 2
    @test msg.reply == "_INBOX.1"
    @test String(msg) == "hello"

    hdr = N._headers_bytes(Headers("Trace" => ["abc"]))
    payload = vcat(hdr, TestHelpers.bytes("body"))
    raw = vcat(TestHelpers.bytes("HMSG events 9 $(length(hdr)) $(length(payload))\r\n"), payload, N.CRLF_BYTES)
    frame = N._read_control_or_msg(IOBuffer(raw))
    @test frame.op == :MSG
    msg = N._protocol_msg(frame)
    @test msg.subject == "events"
    @test header(msg, "Trace") == "abc"
    @test header(msg, "trace") == "abc"
    @test header(msg, "TRACE") == "abc"
    @test msg.header_bytes == length(hdr)
    @test N._msg_pending_bytes(msg) == length(payload)
    @test String(msg) == "body"

    hdr = N._headers_bytes(Headers("Trace" => ["abc", "def"]))
    payload = vcat(hdr, TestHelpers.bytes("work"))
    raw = vcat(TestHelpers.bytes("HMSG jobs 10 _INBOX.2 $(length(hdr)) $(length(payload))\r\n"),
               payload, N.CRLF_BYTES, TestHelpers.bytes("PING\r\n"))
    reader = N.ProtocolReader(IOBuffer(raw))
    frame = N._read_control_or_msg(reader)
    @test frame.op == :MSG
    msg = N._protocol_msg(frame)
    @test msg.subject == "jobs"
    @test msg.sid == 10
    @test msg.reply == "_INBOX.2"
    @test msg.headers["Trace"] == ["abc", "def"]
    @test String(msg) == "work"
    @test N._read_control_or_msg(reader).op == :PING

    status_headers = N._parse_headers(TestHelpers.bytes("NATS/1.0 503 No Responders\r\n\r\n"))
    status_msg = Msg("reply", nothing, UInt8[]; headers=status_headers)
    @test N._status_header(status_msg) == 503
    @test N._status_description(status_msg) == "No Responders"

    lower_status = Msg("reply", nothing, UInt8[]; headers=Headers("status" => ["404"], "description" => ["missing"]))
    @test N._status_header(lower_status) == 404
    @test N._status_description(lower_status) == "missing"

    parsed_headers = N._parse_headers(TestHelpers.bytes("NATS/1.0\r\nTrace:abc\r\nTabbed:\tvalue\r\nSpaced:   value\r\nEmpty:\r\n: skipped\r\n\r\n"))
    @test parsed_headers["Trace"] == ["abc"]
    @test parsed_headers["Tabbed"] == ["value"]
    @test parsed_headers["Spaced"] == ["value"]
    @test parsed_headers["Empty"] == [""]
    @test !haskey(parsed_headers, "")
    @test_throws ProtocolError N._parse_headers(TestHelpers.bytes("NATS/1.0\r\nMalformed\r\n\r\n"))
    @test_throws ProtocolError N._parse_headers(TestHelpers.bytes("NATS/1.0\r\nTrace: abc\r\n"))

    malformed_hdr = TestHelpers.bytes("NATS/1.0\r\nMalformed\r\n\r\n")
    malformed_payload = vcat(malformed_hdr, TestHelpers.bytes("body"))
    malformed_raw = vcat(TestHelpers.bytes("HMSG events 9 $(length(malformed_hdr)) $(length(malformed_payload))\r\n"), malformed_payload, N.CRLF_BYTES)
    @test_throws ProtocolError N._read_control_or_msg(IOBuffer(malformed_raw))

    frame = N._read_control_or_msg(IOBuffer(TestHelpers.bytes("-ERR 'Authorization Violation'\r\n")))
    @test frame.op == :ERR
    err = N._protocol_err(frame)
    @test err == "Authorization Violation"

    reader = N.ProtocolReader(IOBuffer(TestHelpers.bytes("PING\r\nPONG\r\n")))
    @test N._read_control_or_msg(reader).op == :PING
    @test N._read_control_or_msg(reader).op == :PONG
end

@testitem "protocol parser boundary is inferred" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    ping = @inferred N._read_control_or_msg(IOBuffer(TestHelpers.bytes("PING\r\n")))
    @test typeof(ping) === N._ProtocolFrame
    @test ping.op == :PING

    frame = @inferred N._read_control_or_msg(IOBuffer(TestHelpers.bytes("MSG foo 2 _INBOX.1 5\r\nhello\r\n")))
    @test typeof(frame) === N._ProtocolFrame
    @test frame.op == :MSG
    msg = @inferred N._protocol_msg(frame)
    @test typeof(msg) === Msg
    @test msg.subject == "foo"
    @test msg.reply == "_INBOX.1"
    @test String(msg) == "hello"

    err = @inferred N._read_control_or_msg(IOBuffer(TestHelpers.bytes("-ERR 'Authorization Violation'\r\n")))
    @test err.op == :ERR
    @test (@inferred N._protocol_err(err)) == "Authorization Violation"
end

@testitem "protocol parser enforces configured resource limits" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    @test_throws ProtocolError N._read_control_or_msg(IOBuffer(TestHelpers.bytes("PING\r\n")); max_control_line=3)
    @test_throws ProtocolError N._read_control_or_msg(IOBuffer(TestHelpers.bytes("MSG foo 1 5\r\nhello\r\n")); max_payload=4)

    hdr = N._headers_bytes(Headers("Trace" => ["abc"]))
    payload = vcat(hdr, TestHelpers.bytes("body"))
    raw = vcat(TestHelpers.bytes("HMSG events 9 $(length(hdr)) $(length(payload))\r\n"), payload, N.CRLF_BYTES)
    @test_throws ProtocolError N._read_control_or_msg(IOBuffer(raw); max_header_bytes=length(hdr) - 1)
end

@testitem "protocol reader uses lazy buffering" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct PartialReadIO <: IO
        data::Vector{UInt8}
        pos::Int
        requests::Vector{Tuple{Int,Bool}}
    end

    function Base.readbytes!(io::PartialReadIO, b::Vector{UInt8}, nb::Integer=length(b); all::Bool=true)
        push!(io.requests, (Int(nb), all))
        io.pos > length(io.data) && return 0
        n = min(Int(nb), length(io.data) - io.pos + 1)
        copyto!(b, 1, io.data, io.pos, n)
        io.pos += n
        n
    end

    io = PartialReadIO(TestHelpers.bytes("PING\r\n"), 1, Tuple{Int,Bool}[])
    reader = N.ProtocolReader(io; read_size=16)
    @test N._read_control_or_msg(reader).op == :PING
    @test io.requests == [(16, false)]

    raw = TestHelpers.bytes(repeat("PING\r\n", 4))
    reader = N.ProtocolReader(IOBuffer(raw); read_size=length(raw))
    @test N._read_control_or_msg(reader).op == :PING
    @test length(reader.buffer) == length(raw)
    @test reader.first == ncodeunits("PING\r\n") + 1
    @test reader.last == length(raw)

    for _ in 2:4
        @test N._read_control_or_msg(reader).op == :PING
    end
    @test isempty(reader.buffer)
    @test reader.first == 1
    @test reader.last == 0
end

@testitem "protocol payload trailer is read in one chunk" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct ExactReadIO <: IO
        data::Vector{UInt8}
        pos::Int
        reads::Vector{Int}
    end

    function Base.unsafe_read(io::ExactReadIO, p::Ptr{UInt8}, n::UInt)
        count = Int(n)
        push!(io.reads, count)
        length(io.data) - io.pos + 1 >= count || throw(EOFError())
        data = io.data
        GC.@preserve data unsafe_copyto!(p, pointer(data, io.pos), count)
        io.pos += count
        nothing
    end

    io = ExactReadIO(vcat(TestHelpers.bytes("hi"), N.CRLF_BYTES), 1, Int[])
    reader = N.ProtocolReader(io)
    @test String(N._read_exact_payload(reader, 2)) == "hi"
    @test io.reads == [2, 2]
end

@testitem "protocol payload trailer consumes buffered bytes without allocation" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    function buffered_trailer_reader(raw)
        reader = N.ProtocolReader(IOBuffer(UInt8[]))
        reader.buffer = copy(raw)
        reader.first = 1
        reader.last = length(raw)
        reader
    end

    consume_buffered_trailer!(reader) = N._read_payload_trailer_no_drop!(reader)

    raw = TestHelpers.bytes("\r\nPING\r\n")
    consume_buffered_trailer!(buffered_trailer_reader(raw))

    reader = buffered_trailer_reader(raw)
    @test (@allocated consume_buffered_trailer!(reader)) == 0
    @test reader.first == 3
    @test reader.last == length(raw)
    @test N._read_control_or_msg(reader).op == :PING
end

@testitem "protocol writer" setup=[TestHelpers] begin
    using JSON3
    using Natter

    const N = Natter

    @test String(N._pub_cmd("foo", nothing, TestHelpers.bytes("hi"), Headers())) == "PUB foo 2\r\nhi\r\n"
    @test String(N._pub_cmd("foo", "bar", TestHelpers.bytes("hi"), Headers())) == "PUB foo bar 2\r\nhi\r\n"
    @test N._unsub_cmd(7) == "UNSUB 7\r\n"
    @test N._unsub_cmd(7, 3) == "UNSUB 7 3\r\n"
    @test_throws ArgumentError N._unsub_cmd(7, -1)

    cmd = String(N._pub_cmd("foo", nothing, TestHelpers.bytes("hi"), Headers("A" => ["b"])))
    @test startswith(cmd, "HPUB foo ")
    @test occursin("NATS/1.0\r\nA: b\r\n\r\nhi\r\n", cmd)

    function write_frame(frame)
        io = IOBuffer()
        N._write_pub_frame(io, frame)
        take!(io)
    end
    plain_frame = N.PublishFrame("foo", nothing, TestHelpers.bytes("hi"), UInt8[])
    reply_frame = N.PublishFrame("foo", "bar", TestHelpers.bytes("hi"), UInt8[])
    header_frame = N.PublishFrame("foo", nothing, TestHelpers.bytes("hi"), N._headers_bytes(Headers("A" => ["b"])))
    @test write_frame(plain_frame) == N._pub_cmd(plain_frame)
    @test write_frame(reply_frame) == N._pub_cmd(reply_frame)
    @test write_frame(header_frame) == N._pub_cmd(header_frame)

    io = IOBuffer()
    for _ in 1:1000
        truncate(io, 0)
        seekstart(io)
        N._write_pub_frame(io, plain_frame)
        N._validate_publish_subject("foo.bar")
    end
    @test (@allocated begin
        truncate(io, 0)
        seekstart(io)
        N._write_pub_frame(io, plain_frame)
    end) == 0
    @test (@allocated N._validate_publish_subject("foo.bar")) == 0

    @test occursin("Trace-Id_1: ok\r\n", String(N._headers_bytes(Headers("Trace-Id_1" => ["ok"]))))
    for bad_key in ("", "Bad Key", " Bad", "Bad ", "Bad\tKey", "Bad:Key", "Bad/Key", "Bad(Key)", "Bad\r\nKey", "Badé")
        @test_throws ArgumentError N._headers_bytes(Headers(bad_key => ["x"]))
    end
    @test_throws ArgumentError N._headers_bytes(Headers("Good" => ["bad\r\nvalue"]))

    client = TestHelpers.fake_client()
    connect_cmd = N._connect_command(client, N.ServerInfo(), nothing, nothing)
    m = match(r"^CONNECT (.*)\r\n$", connect_cmd)
    @test m !== nothing
    body = JSON3.read(only(m.captures))
    @test body.headers == false
    @test body.no_responders == false
    @test body.lang == "julia"

    headers_cmd = N._connect_command(client, N.ServerInfo(; headers=true), nothing, nothing)
    headers_body = JSON3.read(only(match(r"^CONNECT (.*)\r\n$", headers_cmd).captures))
    @test headers_body.headers == true
    @test headers_body.no_responders == true

    token_client = TestHelpers.fake_client()
    token_cmd = N._connect_command(token_client, N.ServerInfo(), "secret", nothing)
    token_body = JSON3.read(only(match(r"^CONNECT (.*)\r\n$", token_cmd).captures))
    @test token_body.auth_token == "secret"

    userpass_cmd = N._connect_command(client, N.ServerInfo(), "user", "pass")
    userpass_body = JSON3.read(only(match(r"^CONNECT (.*)\r\n$", userpass_cmd).captures))
    @test userpass_body.user == "user"
    @test userpass_body.pass == "pass"
    @test !haskey(userpass_body, :auth_token)

    option_token_client = TestHelpers.fake_client(; opts=N.ConnectOptions(token="secret"))
    @test_throws ArgumentError N._connect_command(option_token_client, N.ServerInfo(), "user", "pass")
    @test_throws ArgumentError N._connect_command(option_token_client, N.ServerInfo(), "url-token", nothing)

    option_userpass_client = TestHelpers.fake_client(; opts=N.ConnectOptions(user="user", password="pass"))
    @test_throws ArgumentError N._connect_command(option_userpass_client, N.ServerInfo(), "secret", nothing)
    @test_throws ArgumentError N._connect_command(option_userpass_client, N.ServerInfo(), "url-user", "url-pass")
end
