using TestItems

@testitem "protocol parser" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    frame = N._read_control_or_msg(IOBuffer(TestHelpers.bytes("INFO {\"server_id\":\"srv\",\"max_payload\":64,\"headers\":true,\"nonce\":\"abc123\",\"proto\":1}\r\n")))
    @test frame.op == :INFO
    data = N._protocol_info(frame)
    @test data.max_payload == 64
    @test data.headers == true
    @test data.nonce == "abc123"
    @test data.proto == 1

    info = N.ServerInfo(; headers=true)
    N._merge_server_info!(info, N.ServerInfo(; connect_urls=["127.0.0.1:4222"]))
    @test info.headers == true
    N._merge_server_info!(info, N.ServerInfo(; headers=false, proto=1))
    @test info.headers == false
    @test info.proto == 1

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
    @test msg.headers isa N.RawHeaders
    @test N.header(msg, "Trace") == "abc"
    @test N.header(msg, "trace") == "abc"
    @test N.header(msg, "TRACE") == "abc"
    @test msg.header_bytes == length(hdr)
    @test N._msg_pending_bytes(msg) == length(payload)
    @test String(msg) == "body"

    status_field_hdr = N._headers_bytes(Headers("Status" => ["abc"], "Description" => ["plain"]))
    status_field_msg = N.Msg("events", nothing, UInt8[], N.LazyHeaders(status_field_hdr), 9, length(status_field_hdr))
    @test N.header(status_field_msg, "Status") == "abc"
    @test N.header(status_field_msg, "Description") == "plain"
    @test isnothing(status_field_msg.headers.parsed)

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

    parsed_headers = N._parse_headers(TestHelpers.bytes("NATS/1.0\r\nTrace:abc\r\nTabbed:\tvalue\r\nSpaced:   value\r\nEmpty:\r\n\r\n"))
    @test parsed_headers["Trace"] == ["abc"]
    @test parsed_headers["Tabbed"] == ["value"]
    @test parsed_headers["Spaced"] == ["value"]
    @test parsed_headers["Empty"] == [""]

    mixed_headers = Headers("Trace" => "abc", "trace" => ["def"])
    @test length(mixed_headers) == 1
    @test mixed_headers["Trace"] == ["abc", "def"]
    @test mixed_headers["trace"] == ["abc", "def"]
    @test mixed_headers["TRACE"] == ["abc", "def"]
    @test haskey(mixed_headers, "TRACE")
    @test only(collect(keys(mixed_headers))) == "Trace"
    N._delete_header!(mixed_headers, "TRACE")
    @test isempty(mixed_headers)

    parsed_mixed_headers = N._parse_headers(TestHelpers.bytes("NATS/1.0\r\nTrace:abc\r\ntrace:def\r\n\r\n"))
    @test length(parsed_mixed_headers) == 1
    @test parsed_mixed_headers["TRACE"] == ["abc", "def"]

    @test_throws ProtocolError N._parse_headers(TestHelpers.bytes("NATS/1.0\r\nMalformed\r\n\r\n"))
    @test_throws ProtocolError N._parse_headers(TestHelpers.bytes("NATS/1.0\r\nTrace: abc\r\n"))
    @test_throws ProtocolError N._parse_headers(TestHelpers.bytes("NATS/1.0\r\n: skipped\r\n\r\n"))
    @test_throws ProtocolError N._parse_headers(TestHelpers.bytes("NATS/1.0\r\nBad Key: x\r\n\r\n"))
    @test_throws ProtocolError N._parse_headers(TestHelpers.bytes("NATS/1.0\r\nGood: bad\rvalue\r\n\r\n"))
    @test_throws ProtocolError N._parse_headers(TestHelpers.bytes("NATS/1.0JUNK\r\n\r\n"))
    @test_throws ProtocolError N._parse_headers(TestHelpers.bytes("NATS/1.0 OK\r\n\r\n"))
    trailing_hdr = TestHelpers.bytes("NATS/1.0\r\nA: b\r\n\r\njunk")
    @test_throws ProtocolError N._parse_headers(trailing_hdr)
    @test_throws ArgumentError N.PublishFrame("foo", nothing, TestHelpers.bytes("hi"), trailing_hdr)

    malformed_hdr = TestHelpers.bytes("NATS/1.0\r\nMalformed\r\n\r\n")
    malformed_payload = vcat(malformed_hdr, TestHelpers.bytes("body"))
    malformed_raw = vcat(TestHelpers.bytes("HMSG events 9 $(length(malformed_hdr)) $(length(malformed_payload))\r\n"), malformed_payload, N.CRLF_BYTES)
    @test_throws ProtocolError N._read_control_or_msg(IOBuffer(malformed_raw))

    invalid_protocol_hdr = TestHelpers.bytes("NATS/1.0JUNK\r\n\r\n")
    invalid_protocol_payload = vcat(invalid_protocol_hdr, TestHelpers.bytes("body"))
    invalid_protocol_raw = vcat(TestHelpers.bytes("HMSG events 9 $(length(invalid_protocol_hdr)) $(length(invalid_protocol_payload))\r\n"), invalid_protocol_payload, N.CRLF_BYTES)
    @test_throws ProtocolError N._read_control_or_msg(IOBuffer(invalid_protocol_raw))
    @test_throws ArgumentError N.PublishFrame("foo", nothing, TestHelpers.bytes("hi"), invalid_protocol_hdr)

    trailing_payload = vcat(trailing_hdr, TestHelpers.bytes("body"))
    trailing_raw = vcat(TestHelpers.bytes("HMSG events 9 $(length(trailing_hdr)) $(length(trailing_payload))\r\n"), trailing_payload, N.CRLF_BYTES)
    @test_throws ProtocolError N._read_control_or_msg(IOBuffer(trailing_raw))

    frame = N._read_control_or_msg(IOBuffer(TestHelpers.bytes("-ERR 'Authorization Violation'\r\n")))
    @test frame.op == :ERR
    err = N._protocol_err(frame)
    @test err == "Authorization Violation"

    reader = N.ProtocolReader(IOBuffer(TestHelpers.bytes("PING\r\nPONG\r\n")))
    @test N._read_control_or_msg(reader).op == :PING
    @test N._read_control_or_msg(reader).op == :PONG
end

@testitem "protocol parser can borrow callback payloads" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    raw = TestHelpers.bytes("MSG foo 2 _INBOX.1 5\r\nhello\r\nPING\r\n")
    reader = N.ProtocolReader(IOBuffer(raw); read_size=length(raw))
    frame = N._read_control_or_msg(reader;
                                   borrow_payload=sid -> sid == 2)
    msg = N._protocol_msg(frame)
    @test msg isa BorrowedMsg
    @test msg.subject == "foo"
    @test msg.sid == 2
    @test msg.reply == "_INBOX.1"
    @test String(msg) == "hello"
    @test msg.data isa SubArray
    @test parent(msg.data) === reader.buffer
    @test N._read_control_or_msg(reader).op == :PING

    hdr = N._headers_bytes(Headers("Trace" => ["abc"]))
    payload = vcat(hdr, TestHelpers.bytes("body"))
    hraw = vcat(TestHelpers.bytes("HMSG events 9 $(length(hdr)) $(length(payload))\r\n"),
                payload, N.CRLF_BYTES)
    hreader = N.ProtocolReader(IOBuffer(hraw); read_size=length(hraw))
    hframe = N._read_control_or_msg(hreader; borrow_payload=_ -> true)
    hmsg = N._protocol_msg(hframe)
    @test hmsg isa BorrowedMsg
    @test hmsg.headers isa N.RawHeaders
    @test hmsg.headers.raw isa SubArray
    @test parent(hmsg.headers.raw) === hreader.buffer
    @test N.header(hmsg, "Trace") == "abc"
    @test String(hmsg) == "body"
    @test hmsg.data isa SubArray
    @test parent(hmsg.data) === hreader.buffer

    empty_raw = TestHelpers.bytes("MSG empty 11 0\r\n\r\n")
    empty_reader = N.ProtocolReader(IOBuffer(empty_raw); read_size=length(empty_raw))
    empty_frame = N._read_control_or_msg(empty_reader; borrow_payload=_ -> true)
    empty_msg = N._protocol_msg(empty_frame)
    @test empty_msg isa BorrowedMsg
    @test isempty(empty_msg.data)
    @test empty_msg.data isa N._BorrowedDispatchData
    @test parent(empty_msg.data) === empty_reader.buffer

    empty_hdr = N._headers_bytes(Headers("Trace" => ["empty"]))
    empty_hraw = vcat(TestHelpers.bytes("HMSG empty.headers 12 $(length(empty_hdr)) $(length(empty_hdr))\r\n"),
                      empty_hdr, N.CRLF_BYTES)
    empty_hreader = N.ProtocolReader(IOBuffer(empty_hraw); read_size=length(empty_hraw))
    empty_hframe = N._read_control_or_msg(empty_hreader; borrow_payload=_ -> true)
    empty_hmsg = N._protocol_msg(empty_hframe)
    @test empty_hmsg isa BorrowedMsg
    @test empty_hmsg.headers isa N.RawHeaders
    @test empty_hmsg.headers.raw isa N._BorrowedDispatchData
    @test empty_hmsg.data isa N._BorrowedDispatchData
    @test parent(empty_hmsg.headers.raw) === empty_hreader.buffer
    @test parent(empty_hmsg.data) === empty_hreader.buffer
    @test isempty(empty_hmsg.data)

    large = fill(UInt8('x'), 256)
    large_raw = vcat(TestHelpers.bytes("MSG large 4 $(length(large))\r\n"), large, N.CRLF_BYTES)
    large_reader = N.ProtocolReader(IOBuffer(large_raw); read_size=16, shrink_threshold=64)
    large_frame = N._read_control_or_msg(large_reader; borrow_payload=_ -> true)
    large_msg = N._protocol_msg(large_frame)
    borrowed_parent = parent(large_msg.data)
    @test borrowed_parent === large_reader.buffer
    @test length(large_reader.buffer) > large_reader.shrink_threshold
    N._drop_consumed!(large_reader)
    @test isempty(large_reader.buffer)
    @test large_reader.buffer !== borrowed_parent
end

@testitem "protocol parser boundary is inferred" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    ping = N._read_control_or_msg(IOBuffer(TestHelpers.bytes("PING\r\n")))
    @test ping isa N._ProtocolFrame
    @test ping isa N.PingFrame
    @test ping.op == :PING

    frame = N._read_control_or_msg(IOBuffer(TestHelpers.bytes("MSG foo 2 _INBOX.1 5\r\nhello\r\n")))
    @test frame isa N._ProtocolFrame
    @test frame isa N.MsgFrame{Msg}
    @test frame.op == :MSG
    msg = @inferred N._protocol_msg(frame)
    @test typeof(msg) === Msg
    @test msg.subject == "foo"
    @test msg.reply == "_INBOX.1"
    @test String(msg) == "hello"

    err = N._read_control_or_msg(IOBuffer(TestHelpers.bytes("-ERR 'Authorization Violation'\r\n")))
    @test err isa N.ErrFrame
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

@testitem "protocol reader fills TCP buffer tail directly" setup=[TestHelpers] begin
    using Natter
    using Sockets

    const N = Natter

    listener = listen(ip"127.0.0.1", 0)
    _, port = Sockets.getsockname(listener)
    server_task = @async begin
        accepted = accept(listener)
        try
            write(accepted, TestHelpers.bytes("PING\r\nPONG\r\n"))
            flush(accepted)
        finally
            close(accepted)
        end
    end

    let client_sock = nothing
        try
            client_sock = Sockets.connect(ip"127.0.0.1", Int(port))
            reader = N.ProtocolReader(client_sock; read_size=16)
            fill!(reader.scratch, 0xaa)

            @test N._read_control_or_msg(reader).op == :PING
            @test all(==(0xaa), reader.scratch)
            @test N._read_control_or_msg(reader).op == :PONG
            @test all(==(0xaa), reader.scratch)
        finally
            isnothing(client_sock) || close(client_sock)
            close(listener)
            wait(server_task)
        end
    end
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
    using Base64
    using JSON3
    using Natter

    const N = Natter

    @test String(N._pub_cmd(N._publish_frame("foo", nothing, TestHelpers.bytes("hi"), Headers()))) == "PUB foo 2\r\nhi\r\n"
    @test String(N._pub_cmd(N._publish_frame("foo", "bar", TestHelpers.bytes("hi"), Headers()))) == "PUB foo bar 2\r\nhi\r\n"
    @test N._unsub_cmd(7) == "UNSUB 7\r\n"
    @test N._unsub_cmd(7, 3) == "UNSUB 7 3\r\n"
    @test_throws ArgumentError N._unsub_cmd(7, -1)

    cmd = String(N._pub_cmd(N._publish_frame("foo", nothing, TestHelpers.bytes("hi"), Headers("A" => ["b"]))))
    @test startswith(cmd, "HPUB foo ")
    @test occursin("NATS/1.0\r\nA: b\r\n\r\nhi\r\n", cmd)

    function write_frame(frame)
        io = IOBuffer()
        N._write_pub_frame(io, frame)
        take!(io)
    end
    plain_payload = TestHelpers.bytes("hi")
    plain_frame = N.PublishFrame("foo", nothing, plain_payload, UInt8[])
    push!(plain_payload, UInt8('!'))
    @test collect(plain_frame.payload) == TestHelpers.bytes("hi")
    @test plain_frame.payload isa N.ImmutableBytes
    @test plain_frame.headers isa Base.CodeUnits{UInt8,String}
    @test_throws MethodError push!(plain_frame.payload, 0x41)

    raw_headers = N._headers_bytes(Headers("A" => ["b"]))
    header_snapshot = copy(raw_headers)
    copied_header_frame = N.PublishFrame("foo", nothing, UInt8[], raw_headers)
    raw_headers[1] = UInt8('X')
    @test collect(copied_header_frame.headers) == header_snapshot
    @test_throws ArgumentError N.PublishFrame("foo.*", nothing, TestHelpers.bytes("hi"), UInt8[])
    @test_throws ArgumentError N.PublishFrame("foo", "bar.*", TestHelpers.bytes("hi"), UInt8[])
    @test_throws ArgumentError N.PublishFrame("foo", nothing, TestHelpers.bytes("hi"), TestHelpers.bytes("bad\r\n\r\n"))
    for raw_header in (
            "NATS/1.0\r\n: skipped\r\n\r\n",
            "NATS/1.0\r\nBad Key: x\r\n\r\n",
            "NATS/1.0\r\nGood: bad\rvalue\r\n\r\n",
        )
        raw = TestHelpers.bytes(raw_header)
        @test_throws ArgumentError N.PublishFrame("foo", nothing, TestHelpers.bytes("hi"), raw)
        @test_throws ArgumentError prepare_publish("foo", "hi"; headers=raw)
    end

    reply_frame = N.PublishFrame("foo", "bar", TestHelpers.bytes("hi"), UInt8[])
    header_frame = N.PublishFrame("foo", nothing, TestHelpers.bytes("hi"), N._headers_bytes(Headers("A" => ["b"])))
    @test write_frame(plain_frame) == N._pub_cmd(plain_frame)
    @test write_frame(reply_frame) == N._pub_cmd(reply_frame)
    @test write_frame(header_frame) == N._pub_cmd(header_frame)
    @test_throws ArgumentError N.PublishFrame("foo", "bad.>", TestHelpers.bytes("hi"), UInt8[])
    @test_throws ArgumentError N.PublishFrame("foo", nothing, TestHelpers.bytes("hi"),
                                              TestHelpers.bytes("NATS/1.0\r\nMalformed\r\n\r\n"))

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

    server = N.Server("nats://example.test:4222")
    connect_command(client, info=N.ServerInfo(), url_user=nothing, url_pass=nothing) =
        N._connect_command(client, server, info, url_user, url_pass; attempt=1, reconnect=false)

    client = TestHelpers.fake_client()
    connect_cmd = connect_command(client)
    m = match(r"^CONNECT (.*)\r\n$", connect_cmd)
    @test m !== nothing
    body = JSON3.read(only(m.captures))
    @test body.headers == false
    @test body.no_responders == false
    @test body.lang == "julia"

    headers_cmd = connect_command(client, N.ServerInfo(; headers=true))
    headers_body = JSON3.read(only(match(r"^CONNECT (.*)\r\n$", headers_cmd).captures))
    @test headers_body.headers == true
    @test headers_body.no_responders == true

    no_echo_client = TestHelpers.fake_client(; opts=N.ConnectOptions(no_echo=true))
    @test_throws UnsupportedFeatureError connect_command(no_echo_client)
    @test_throws UnsupportedFeatureError connect_command(no_echo_client, N.ServerInfo(; proto=0))
    no_echo_cmd = connect_command(no_echo_client, N.ServerInfo(; proto=1))
    no_echo_body = JSON3.read(only(match(r"^CONNECT (.*)\r\n$", no_echo_cmd).captures))
    @test no_echo_body.echo == false

    token_client = TestHelpers.fake_client()
    token_cmd = connect_command(token_client, N.ServerInfo(), "secret", nothing)
    token_body = JSON3.read(only(match(r"^CONNECT (.*)\r\n$", token_cmd).captures))
    @test token_body.auth_token == "secret"

    userpass_cmd = connect_command(client, N.ServerInfo(), "user", "pass")
    userpass_body = JSON3.read(only(match(r"^CONNECT (.*)\r\n$", userpass_cmd).captures))
    @test userpass_body.user == "user"
    @test userpass_body.pass == "pass"
    @test !haskey(userpass_body, :auth_token)

    option_token_client = TestHelpers.fake_client(; opts=N.ConnectOptions(auth=N.TokenAuth("secret")))
    option_token_cmd = connect_command(option_token_client)
    option_token_body = JSON3.read(only(match(r"^CONNECT (.*)\r\n$", option_token_cmd).captures))
    @test option_token_body.auth_token == "secret"
    @test_throws ArgumentError connect_command(option_token_client, N.ServerInfo(), "user", "pass")
    @test_throws ArgumentError connect_command(option_token_client, N.ServerInfo(), "url-token", nothing)

    option_userpass_client = TestHelpers.fake_client(; opts=N.ConnectOptions(auth=N.UserPassAuth("user", "pass")))
    option_userpass_cmd = connect_command(option_userpass_client)
    option_userpass_body = JSON3.read(only(match(r"^CONNECT (.*)\r\n$", option_userpass_cmd).captures))
    @test option_userpass_body.user == "user"
    @test option_userpass_body.pass == "pass"
    @test_throws ArgumentError connect_command(option_userpass_client, N.ServerInfo(), "secret", nothing)
    @test_throws ArgumentError connect_command(option_userpass_client, N.ServerInfo(), "url-user", "url-pass")

    auth_request = Ref{Union{N.AuthRequest,Nothing}}(nothing)
    dynamic_auth_client = TestHelpers.fake_client(; opts=N.ConnectOptions(
        auth=N.CallbackAuth(req -> begin
            auth_request[] = req
            N.TokenAuth("dynamic")
        end),
    ))
    dynamic_auth_cmd = connect_command(dynamic_auth_client, N.ServerInfo(; nonce="nonce"))
    dynamic_auth_body = JSON3.read(only(match(r"^CONNECT (.*)\r\n$", dynamic_auth_cmd).captures))
    @test dynamic_auth_body.auth_token == "dynamic"
    @test auth_request[].server === server
    @test auth_request[].url == server.url
    @test auth_request[].nonce == "nonce"
    @test auth_request[].attempt == 1
    @test !auth_request[].reconnect

    invalid_callback_client = TestHelpers.fake_client(; opts=N.ConnectOptions(
        auth=N.CallbackAuth(_ -> "bad"),
    ))
    @test_throws ArgumentError connect_command(invalid_callback_client, N.ServerInfo())

    function encoded_seed(public_prefix::UInt8)
        data = Vector{UInt8}(undef, 34)
        data[1] = N._NKEY_PREFIX_SEED | (public_prefix >> 5)
        data[2] = (public_prefix & UInt8(0x1f)) << 3
        data[3:end] .= UInt8(0x42)
        raw = copy(data)
        crc = N._nkey_crc16(data)
        push!(raw, UInt8(crc & UInt16(0x00ff)))
        push!(raw, UInt8(crc >> 8))
        N._nkey_base32_encode(raw)
    end

    seed = "SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY"
    public_nkey = "UAT6BWCSCWLUKJT6K6MBJJOEOTXZ5AJDOYKNEVRFC7VNO6OA43N4TRNO"
    expected_sig = "m50It12aTgfbJwsQhucujqhXbsq7tLM-Mf_hSjBQsG_4onm8y2Vkw6JG1bbcDkdxXe-Ng0K-7X9ov4rZ4wFcDg"

    nkey_client = TestHelpers.fake_client(; opts=N.ConnectOptions(auth=N.NKeyAuth(; seed)))
    nkey_cmd = connect_command(nkey_client, N.ServerInfo(; nonce="nonce"))
    nkey_body = JSON3.read(only(match(r"^CONNECT (.*)\r\n$", nkey_cmd).captures))
    @test nkey_body.nkey == public_nkey
    @test nkey_body.sig == expected_sig
    @test !haskey(nkey_body, :jwt)
    @test !haskey(nkey_body, :auth_token)
    @test_throws UnsupportedFeatureError connect_command(nkey_client)

    mktemp() do path, io
        write(io, seed)
        close(io)
        nkey_path_client = TestHelpers.fake_client(; opts=N.ConnectOptions(auth=N.NKeyAuth(; seed_path=path)))
        nkey_path_cmd = connect_command(nkey_path_client, N.ServerInfo(; nonce="nonce"))
        nkey_path_body = JSON3.read(only(match(r"^CONNECT (.*)\r\n$", nkey_path_cmd).captures))
        @test nkey_path_body.nkey == public_nkey
        @test nkey_path_body.sig == expected_sig
    end

    callback_client = TestHelpers.fake_client(; opts=N.ConnectOptions(
        auth=N.NKeyAuth(; nkey=public_nkey, signature_cb=nonce -> fill(UInt8(0x01), 64)),
    ))
    callback_cmd = connect_command(callback_client, N.ServerInfo(; nonce="nonce"))
    callback_body = JSON3.read(only(match(r"^CONNECT (.*)\r\n$", callback_cmd).captures))
    callback_sig = replace(base64encode(fill(UInt8(0x01), 64)), '+' => '-', '/' => '_')
    callback_sig = replace(callback_sig, r"=+$" => "")
    @test callback_body.nkey == public_nkey
    @test callback_body.sig == callback_sig

    jwt_client = TestHelpers.fake_client(; opts=N.ConnectOptions(
        auth=N.JwtAuth(; jwt="header.payload.signature", seed),
    ))
    jwt_cmd = connect_command(jwt_client, N.ServerInfo(; nonce="nonce"))
    jwt_body = JSON3.read(only(match(r"^CONNECT (.*)\r\n$", jwt_cmd).captures))
    @test jwt_body.jwt == "header.payload.signature"
    @test jwt_body.sig == expected_sig
    @test !haskey(jwt_body, :nkey)

    for public_prefix in (
        N._NKEY_PREFIX_OPERATOR,
        N._NKEY_PREFIX_SERVER,
        N._NKEY_PREFIX_CLUSTER,
        N._NKEY_PREFIX_ACCOUNT,
    )
        non_user_public = N._nkey_encode_public(public_prefix, fill(UInt8(0x01), 32))
        non_user_seed = encoded_seed(public_prefix)

        non_user_public_client = TestHelpers.fake_client(; opts=N.ConnectOptions(
            auth=N.NKeyAuth(; nkey=non_user_public, signature_cb=nonce -> fill(UInt8(0x01), 64)),
        ))
        @test_throws ArgumentError connect_command(non_user_public_client,
                                                   N.ServerInfo(; nonce="nonce"))

        non_user_seed_client = TestHelpers.fake_client(;
            opts=N.ConnectOptions(auth=N.NKeyAuth(; seed=non_user_seed)),
        )
        @test_throws ArgumentError connect_command(non_user_seed_client,
                                                   N.ServerInfo(; nonce="nonce"))
    end

    creds = join([
        "-----BEGIN NATS USER JWT-----",
        "header.payload.signature",
        "------END NATS USER JWT------",
        "",
        "-----BEGIN USER NKEY SEED-----",
        seed,
        "------END USER NKEY SEED------",
    ], "\n")
    creds_client = TestHelpers.fake_client(; opts=N.ConnectOptions(auth=N.CredentialsAuth(creds)))
    creds_cmd = connect_command(creds_client, N.ServerInfo(; nonce="nonce"))
    creds_body = JSON3.read(only(match(r"^CONNECT (.*)\r\n$", creds_cmd).captures))
    @test creds_body.jwt == "header.payload.signature"
    @test creds_body.sig == expected_sig
    @test !haskey(creds_body, :nkey)

    mktemp() do path, io
        write(io, creds)
        close(io)
        creds_path_client = TestHelpers.fake_client(; opts=N.ConnectOptions(auth=N.CredentialsAuth(; path)))
        creds_path_cmd = connect_command(creds_path_client, N.ServerInfo(; nonce="nonce"))
        creds_path_body = JSON3.read(only(match(r"^CONNECT (.*)\r\n$", creds_path_cmd).captures))
        @test creds_path_body.jwt == "header.payload.signature"
        @test creds_path_body.sig == expected_sig
        @test !haskey(creds_path_body, :nkey)
    end

    for public_prefix in (N._NKEY_PREFIX_OPERATOR, N._NKEY_PREFIX_ACCOUNT)
        non_user_creds = join([
            "-----BEGIN NATS USER JWT-----",
            "header.payload.signature",
            "------END NATS USER JWT------",
            "",
            encoded_seed(public_prefix),
        ], "\n")
        non_user_creds_client = TestHelpers.fake_client(;
            opts=N.ConnectOptions(auth=N.CredentialsAuth(non_user_creds)),
        )
        @test_throws ArgumentError connect_command(non_user_creds_client,
                                                   N.ServerInfo(; nonce="nonce"))
    end
end
