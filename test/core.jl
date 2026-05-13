using TestItems

@testitem "validation and buffering" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    @test N.ConnectOptions().allow_reconnect
    @test N.ConnectOptions().max_reconnect_attempts == -1
    @test N.ConnectOptions().sub_pending_msgs_limit == 1024

    @test N._validate_subject("foo.*.bar") == "foo.*.bar"
    @test N._validate_subject("foo.>") == "foo.>"
    @test_throws ArgumentError N._validate_subject("foo.>.bar")
    @test_throws ArgumentError N._validate_subject("foo.bad*")
    @test_throws ArgumentError N._validate_subject("foo..bar")
    @test_throws ArgumentError N._validate_publish_subject("foo.*")
    @test_throws ArgumentError N._validate_publish_subject("foo.>")
    @test_throws ArgumentError N._validate_publish_subject("foo..bar")

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    publish(client, "foo", "bar")
    @test client.pending_bytes == length("PUB foo 3\r\nbar\r\n")
    @test client.stats.out_msgs == 1

    headers = Headers("Trace" => [repeat("x", 16)])
    payload = TestHelpers.bytes("body")
    total = N._pub_payload_size(payload, N._headers_bytes(headers))
    max_payload = total - 1
    limited = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    limited.info.max_payload = max_payload
    try
        publish(limited, "foo", payload; headers)
        @test false
    catch err
        @test err isa MaxPayloadError
        @test err.limit == max_payload
        @test err.actual == total
    end
    @test limited.pending_bytes == 0

    small = TestHelpers.fake_client(; opts=N.ConnectOptions(pending_size=3), status=N.ConnectionStatus.RECONNECTING)
    @test_throws OutboundBufferLimitError N._send_raw(small, TestHelpers.bytes("abcd"); buffer_on_reconnect=true)

    closed = TestHelpers.fake_client(; status=N.ConnectionStatus.DISCONNECTED)
    @test_throws ConnectionClosedError publish(closed, "foo", "bar")
    @test_throws ConnectionClosedError subscribe(closed, "foo")
end

@testitem "reconnect local state does not enqueue subscription protocol" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    sub = subscribe(client, "foo")
    @test sub.sid in keys(client.subscriptions)
    @test !sub.server_active
    @test client.pending_bytes == 0
end

@testitem "pending buffer is restored after reconnect write failure" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    struct BadWriteIO <: IO end
    Base.write(::BadWriteIO, ::Vector{UInt8}) = throw(ErrorException("write failed"))
    Base.flush(::BadWriteIO) = nothing

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING, write_io=BadWriteIO())
    data = TestHelpers.bytes("PUB foo 3\r\nbar\r\n")
    N._enqueue_pending(client, data)
    @test_throws ErrorException N._flush_pending_buffer(client)
    @test client.pending_bytes == length(data)
end

@testitem "foreground publish write failure starts reconnect and buffers" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    struct BadWriteTransport <: IO end
    Base.write(::BadWriteTransport, ::Vector{UInt8}) = throw(ErrorException("write failed"))
    Base.flush(::BadWriteTransport) = nothing
    Base.close(::BadWriteTransport) = nothing

    transport = BadWriteTransport()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, read_io=transport, write_io=transport)

    publish(client, "foo", "bar")
    @test status(client) == N.ConnectionStatus.RECONNECTING
    @test client.pending_bytes == length("PUB foo 3\r\nbar\r\n")
    @test stats(client).out_msgs == 1
    close(client)
end

@testitem "request does not buffer while reconnecting" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)

    @test_throws ConnectionReconnectingError request(client, "foo", "bar"; timeout=0.001)
    @test isempty(client.subscriptions)
    @test client.sid == 0
    @test client.pending_bytes == 0
end

@testitem "request publish write failure starts reconnect without buffering" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct RequestPublishFailTransport <: IO
        writes::Vector{String}
        closed::Bool
    end
    function Base.write(t::RequestPublishFailTransport, data::Union{String,SubString{String}})
        s = String(data)
        push!(t.writes, s)
        startswith(s, "PUB ") && throw(ErrorException("write failed"))
        ncodeunits(s)
    end
    Base.write(t::RequestPublishFailTransport, data::Vector{UInt8}) = (push!(t.writes, String(data)); length(data))
    Base.flush(::RequestPublishFailTransport) = nothing
    Base.close(t::RequestPublishFailTransport) = (t.closed = true; nothing)

    transport = RequestPublishFailTransport(String[], false)
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, read_io=transport, write_io=transport)

    @test_throws ConnectionReconnectingError request(client, "foo", "bar"; timeout=0.001)
    @test status(client) == N.ConnectionStatus.RECONNECTING
    @test length(client.subscriptions) == 1
    @test !isnothing(client.request_mux)
    @test client.request_mux.sub.sid in keys(client.subscriptions)
    @test !client.request_mux.sub.server_active
    @test isempty(client.request_mux.waiters)
    @test client.pending_bytes == 0
    @test stats(client).out_msgs == 0
    @test any(write -> startswith(write, "SUB _INBOX.") && occursin("*", write), transport.writes)
    close(client)
end

@testitem "timed out request removes mux waiter but keeps shared inbox subscription" setup=[TestHelpers] begin
    using Natter

    client = TestHelpers.fake_client(; status=Natter.ConnectionStatus.CONNECTED, write_io=IOBuffer())

    @test_throws TimeoutError request(client, "foo", "bar"; timeout=0.001)
    @test length(client.subscriptions) == 1
    @test !isnothing(client.request_mux)
    @test client.request_mux.sub.sid in keys(client.subscriptions)
    @test isempty(client.request_mux.waiters)
    @test client.pending_bytes == 0
    close(client)
end

@testitem "concurrent requests share a mux and receive matching replies" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    function request_publishes(transport)
        lines = split(TestHelpers.capture_text(transport), "\r\n"; keepempty=false)
        publishes = Tuple{String,String}[]
        for i in eachindex(lines)
            line = lines[i]
            startswith(line, "PUB svc ") || continue
            parts = split(line)
            length(parts) == 4 || continue
            payload = i < lastindex(lines) ? String(lines[i + 1]) : ""
            push!(publishes, (String(parts[3]), payload))
        end
        publishes
    end

    function wait_for_publishes(transport, n)
        result = timedwait(1.0; pollint=0.001) do
            length(request_publishes(transport)) >= n
        end
        @test result != :timed_out
        request_publishes(transport)[1:n]
    end

    transport = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, read_io=transport, write_io=transport)

    tasks = [@async request(client, "svc", "req$i"; timeout=1.0) for i in 1:5]
    publishes = wait_for_publishes(transport, 5)
    mux = client.request_mux
    @test !isnothing(mux)
    @test length(client.subscriptions) == 1
    @test count(line -> startswith(line, "SUB _INBOX.") && occursin(".* ", line),
                split(TestHelpers.capture_text(transport), "\r\n"; keepempty=false)) == 1
    @test !occursin("UNSUB", TestHelpers.capture_text(transport))

    for (reply, payload) in reverse(publishes)
        N._dispatch_msg(client, Msg(reply, nothing, TestHelpers.bytes("resp-$payload"); client, sid=mux.sub.sid))
    end

    responses = String[ String(fetch(task)) for task in tasks ]
    @test sort(responses) == ["resp-req$i" for i in 1:5]
    @test isempty(mux.waiters)
    @test !occursin("UNSUB", TestHelpers.capture_text(transport))
    close(client)
end

@testitem "late request replies are dropped after timeout" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    function request_publishes(transport)
        lines = split(TestHelpers.capture_text(transport), "\r\n"; keepempty=false)
        publishes = Tuple{String,String}[]
        for i in eachindex(lines)
            line = lines[i]
            startswith(line, "PUB svc ") || continue
            parts = split(line)
            length(parts) == 4 || continue
            payload = i < lastindex(lines) ? String(lines[i + 1]) : ""
            push!(publishes, (String(parts[3]), payload))
        end
        publishes
    end

    function wait_for_publishes(transport, n)
        result = timedwait(1.0; pollint=0.001) do
            length(request_publishes(transport)) >= n
        end
        @test result != :timed_out
        request_publishes(transport)[1:n]
    end

    transport = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, read_io=transport, write_io=transport)

    @test_throws TimeoutError request(client, "svc", "first"; timeout=0.001)
    first_reply = only(request_publishes(transport))[1]
    mux = client.request_mux
    @test isempty(mux.waiters)

    before_drops = stats(client).dropped_msgs
    N._dispatch_msg(client, Msg(first_reply, nothing, TestHelpers.bytes("late"); client, sid=mux.sub.sid))
    @test stats(client).dropped_msgs == before_drops + 1

    task = @async request(client, "svc", "second"; timeout=1.0)
    publishes = wait_for_publishes(transport, 2)
    second_reply = publishes[2][1]
    @test second_reply != first_reply

    N._dispatch_msg(client, Msg(first_reply, nothing, TestHelpers.bytes("still-late"); client, sid=mux.sub.sid))
    sleep(0.02)
    @test !istaskdone(task)
    N._dispatch_msg(client, Msg(second_reply, nothing, TestHelpers.bytes("on-time"); client, sid=mux.sub.sid))
    @test String(fetch(task)) == "on-time"
    @test isempty(mux.waiters)
    close(client)
end

@testitem "request mux reconnect behavior clears waiters without replaying requests" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    function request_publishes(transport)
        lines = split(TestHelpers.capture_text(transport), "\r\n"; keepempty=false)
        publishes = Tuple{String,String}[]
        for i in eachindex(lines)
            line = lines[i]
            startswith(line, "PUB svc ") || continue
            parts = split(line)
            length(parts) == 4 || continue
            payload = i < lastindex(lines) ? String(lines[i + 1]) : ""
            push!(publishes, (String(parts[3]), payload))
        end
        publishes
    end

    transport = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, read_io=transport, write_io=transport)

    task = @async request(client, "svc", "pending"; timeout=1.0)
    result = timedwait(1.0; pollint=0.001) do
        length(request_publishes(transport)) == 1
    end
    @test result != :timed_out
    mux = client.request_mux
    @test length(mux.waiters) == 1

    N._trigger_reconnect(client, ErrorException("lost"))
    err = try
        fetch(task)
        nothing
    catch caught
        caught isa TaskFailedException ? first(Base.current_exceptions(task)).exception : caught
    end
    @test err isa ConnectionReconnectingError
    @test isempty(mux.waiters)
    @test client.pending_bytes == 0
    close(client)

    replay_transport = TestHelpers.WriteCapture()
    replay_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, read_io=replay_transport, write_io=replay_transport)
    @test_throws TimeoutError request(replay_client, "svc", "timed-out"; timeout=0.001)
    replay_mux = replay_client.request_mux
    @test isempty(replay_mux.waiters)
    TestHelpers.clear_capture!(replay_transport)
    replay_client.status = N.ConnectionStatus.RECONNECTING
    replay_mux.sub.server_active = false
    N._replay_subscriptions(replay_client)
    replayed = TestHelpers.capture_text(replay_transport)
    @test replayed == N._sub_cmd(replay_mux.sub.subject, nothing, replay_mux.sub.sid)
    @test !occursin("PUB svc", replayed)
    close(replay_client)
end

@testitem "foreground subscribe write failure keeps local subscription for replay" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    struct BadSubTransport <: IO end
    Base.write(::BadSubTransport, ::String) = throw(ErrorException("write failed"))
    Base.flush(::BadSubTransport) = nothing
    Base.close(::BadSubTransport) = nothing

    transport = BadSubTransport()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, read_io=transport, write_io=transport)

    sub = subscribe(client, "foo")
    @test status(client) == N.ConnectionStatus.RECONNECTING
    @test !sub.server_active
    @test sub.sid in keys(client.subscriptions)
    close(client)
end

@testitem "reconnect trigger only closes transport when it owns the transition" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct CountingTransport <: IO
        closes::Base.RefValue{Int}
    end
    Base.close(t::CountingTransport) = (t.closes[] += 1)

    closes = Ref(0)
    transport = CountingTransport(closes)
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING, read_io=transport, write_io=transport)

    N._trigger_reconnect(client, ErrorException("already reconnecting"))
    @test status(client) == N.ConnectionStatus.RECONNECTING
    @test closes[] == 0
end

@testitem "connect timeout covers protocol handshake" begin
    using Natter
    using Sockets

    const N = Natter

    listener = listen(ip"127.0.0.1", 0)
    port = Int(getsockname(listener)[2])
    accepted = Channel{Sockets.TCPSocket}(1)
    blocker = Channel{Bool}(1)
    server_task = @async begin
        try
            sock = accept(listener)
            put!(accepted, sock)
            try
                take!(blocker)
            finally
                close(sock)
            end
        catch err
            (err isa InvalidStateException || err isa InterruptException || err isa Base.IOError) || rethrow()
        end
    end
    try
        @test_throws TimeoutError N.connect("nats://127.0.0.1:$port"; connect_timeout=0.3)
    finally
        close(listener)
        if isready(accepted)
            close(take!(accepted))
        end
        isopen(blocker) && close(blocker)
        done = timedwait(0.5; pollint=0.01) do
            istaskdone(server_task)
        end
        done == :timed_out || wait(server_task)
    end
end

@testitem "close notifies pending flush waiters" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED)
    ch = Channel{Bool}(1)
    push!(client.pongs, N.PongWaiter(ch))
    close(client)
    @test isready(ch)
    @test take!(ch) == false
end

@testitem "timed out flush consumes its own late pong" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    struct FlushSink <: IO end
    Base.write(::FlushSink, data::Vector{UInt8}) = length(data)
    Base.write(::FlushSink, data::String) = ncodeunits(data)
    Base.flush(::FlushSink) = nothing
    Base.close(::FlushSink) = nothing

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=FlushSink())

    @test_throws TimeoutError flush(client; timeout=0.01)
    @test length(client.pongs) == 1
    timed_out_waiter = only(client.pongs)
    later_waiter = Channel{Bool}(1)
    push!(client.pongs, N.PongWaiter(later_waiter))

    N._notify_pong(client)
    @test isnothing(timed_out_waiter.channel)
    @test !isready(later_waiter)
end

@testitem "stale flush waiters are bounded" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    struct FlushSinkForStaleWaiters <: IO end
    Base.write(::FlushSinkForStaleWaiters, data::Vector{UInt8}) = length(data)
    Base.write(::FlushSinkForStaleWaiters, data::String) = ncodeunits(data)
    Base.flush(::FlushSinkForStaleWaiters) = nothing
    Base.close(::FlushSinkForStaleWaiters) = nothing

    client = TestHelpers.fake_client(; opts=N.ConnectOptions(max_stale_pong_waiters=2), status=N.ConnectionStatus.CONNECTED,
                                     write_io=FlushSinkForStaleWaiters())

    for _ in 1:4
        @test_throws TimeoutError flush(client; timeout=0.001)
    end
    @test count(waiter -> isnothing(waiter.channel), client.pongs) == 2
end

@testitem "transport close waits for in-flight writes" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct BlockingTransport <: IO
        closes::Base.RefValue{Int}
        entered::Channel{Bool}
        release::Channel{Bool}
    end
    function Base.write(t::BlockingTransport, data::Vector{UInt8})
        put!(t.entered, true)
        take!(t.release)
        length(data)
    end
    Base.flush(::BlockingTransport) = nothing
    Base.close(t::BlockingTransport) = (t.closes[] += 1)

    closes = Ref(0)
    transport = BlockingTransport(closes, Channel{Bool}(1), Channel{Bool}(1))
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, read_io=transport, write_io=transport)

    write_task = @async N._write_raw(client, TestHelpers.bytes("PING\r\n"))
    @test take!(transport.entered) == true
    close_task = @async N._close_transport!(client)
    sleep(0.05)
    @test closes[] == 0
    put!(transport.release, true)
    wait(write_task)
    wait(close_task)
    @test closes[] == 1
    @test isnothing(client.write_io)
end

@testitem "cleanup errors are reported and optionally thrown" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    struct BadCloseIO <: IO end
    Base.close(::BadCloseIO) = throw(ErrorException("close failed"))

    reported = Any[]
    client = TestHelpers.fake_client(; opts=N.ConnectOptions(error_cb=err -> push!(reported, err)),
                                     status=N.ConnectionStatus.CONNECTED, read_io=BadCloseIO())
    close(client)
    @test length(reported) == 1
    @test reported[1] isa CleanupError

    strict = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, read_io=BadCloseIO())
    @test_throws CleanupError close(strict; throw_errors=true)
end

@testitem "inbox generation" setup=[TestHelpers] begin
    using Natter

    client = TestHelpers.fake_client()
    inbox1 = new_inbox(client)
    inbox2 = new_inbox(client)
    @test startswith(inbox1, "_INBOX.")
    @test startswith(inbox2, "_INBOX.")
    @test inbox1 != inbox2
end

@testitem "discovered servers inherit connection URL context" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED)
    client.connected_url = "tls://user:pass@example.test:4222"
    push!(client.servers, N.Server(client.connected_url))

    N._merge_discovered_servers!(client, N.ServerInfo(; connect_urls=["10.0.0.2:4222", "nats://plain.test:4222"]))
    urls = [server.url for server in client.servers]
    @test "tls://user:pass@10.0.0.2:4222" in urls
    @test "nats://plain.test:4222" in urls
end

@testitem "TLS first handshake mode is explicit and overridable" begin
    using Natter

    const N = Natter

    @test N._tls_first_for_connection(ConnectOptions(), "nats") == false
    @test N._tls_first_for_connection(ConnectOptions(), "tls") == true
    @test N._tls_first_for_connection(ConnectOptions(tls_first=true), "nats") == true
    @test N._tls_first_for_connection(ConnectOptions(tls_first=false), "tls") == false
end

@testitem "TLS verification is enabled by default and can be disabled" begin
    using MbedTLS
    using Natter

    const N = Natter

    @test ConnectOptions().tls_verify == true
    @test N._tls_authmode(ConnectOptions()) == MbedTLS.MBEDTLS_SSL_VERIFY_REQUIRED
    @test N._tls_authmode(ConnectOptions(tls_verify=false)) == MbedTLS.MBEDTLS_SSL_VERIFY_NONE
end
