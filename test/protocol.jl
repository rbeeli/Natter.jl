using TestItems

@testitem "protocol parser" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    op, data = N._read_control_or_msg(IOBuffer(TestHelpers.bytes("INFO {\"server_id\":\"srv\",\"max_payload\":64}\r\n")))
    @test op == :INFO
    @test data["server_id"] == "srv"
    @test data["max_payload"] == 64

    op, msg = N._read_control_or_msg(IOBuffer(TestHelpers.bytes("MSG foo 2 _INBOX.1 5\r\nhello\r\n")))
    @test op == :MSG
    @test msg.subject == "foo"
    @test msg.sid == 2
    @test msg.reply == "_INBOX.1"
    @test String(msg) == "hello"

    hdr = N._headers_bytes(Headers("Trace" => ["abc"]))
    payload = vcat(hdr, TestHelpers.bytes("body"))
    raw = vcat(TestHelpers.bytes("HMSG events 9 $(length(hdr)) $(length(payload))\r\n"), payload, N.CRLF_BYTES)
    op, msg = N._read_control_or_msg(IOBuffer(raw))
    @test op == :MSG
    @test msg.subject == "events"
    @test header(msg, "Trace") == "abc"
    @test String(msg) == "body"

    status_headers = N._parse_headers(TestHelpers.bytes("NATS/1.0 503 No Responders\r\n\r\n"))
    status_msg = Msg("reply", nothing, UInt8[]; headers=status_headers)
    @test N._status_header(status_msg) == 503
    @test N._status_description(status_msg) == "No Responders"

    op, err = N._read_control_or_msg(IOBuffer(TestHelpers.bytes("-ERR 'Authorization Violation'\r\n")))
    @test op == :ERR
    @test err == "Authorization Violation"
end

@testitem "protocol writer" setup=[TestHelpers] begin
    using JSON3
    using Natter

    const N = Natter

    @test String(N._pub_cmd("foo", nothing, TestHelpers.bytes("hi"), Headers())) == "PUB foo 2\r\nhi\r\n"
    @test String(N._pub_cmd("foo", "bar", TestHelpers.bytes("hi"), Headers())) == "PUB foo bar 2\r\nhi\r\n"

    cmd = String(N._pub_cmd("foo", nothing, TestHelpers.bytes("hi"), Headers("A" => ["b"])))
    @test startswith(cmd, "HPUB foo ")
    @test occursin("NATS/1.0\r\nA: b\r\n\r\nhi\r\n", cmd)
    @test_throws ArgumentError N._headers_bytes(Headers("Bad\r\nKey" => ["x"]))
    @test_throws ArgumentError N._headers_bytes(Headers("Good" => ["bad\r\nvalue"]))

    client = TestHelpers.fake_client()
    connect_cmd = N._connect_command(client, Dict{String,Any}(), nothing, nothing)
    m = match(r"^CONNECT (.*)\r\n$", connect_cmd)
    @test m !== nothing
    body = JSON3.read(only(m.captures))
    @test body.headers == true
    @test body.no_responders == true
    @test body.lang == "julia"

    token_client = TestHelpers.fake_client()
    token_cmd = N._connect_command(token_client, Dict{String,Any}(), "secret", nothing)
    token_body = JSON3.read(only(match(r"^CONNECT (.*)\r\n$", token_cmd).captures))
    @test token_body.auth_token == "secret"
end
