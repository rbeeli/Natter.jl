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
    @test_throws ArgumentError N._validate_publish_subject("foo.*")
    @test_throws ArgumentError N._validate_publish_subject("foo.>")

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    publish(client, "foo", "bar")
    @test client.pending_bytes == length("PUB foo 3\r\nbar\r\n")
    @test client.stats.out_msgs == 1

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

    struct BadWriteIO end
    Base.write(::BadWriteIO, ::Vector{UInt8}) = throw(ErrorException("write failed"))
    Base.flush(::BadWriteIO) = nothing

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    data = TestHelpers.bytes("PUB foo 3\r\nbar\r\n")
    N._enqueue_pending(client, data)
    client.write_io = BadWriteIO()
    @test_throws ErrorException N._flush_pending_buffer(client)
    @test client.pending_bytes == length(data)
end

@testitem "foreground publish write failure starts reconnect and buffers" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    struct BadWriteTransport end
    Base.write(::BadWriteTransport, ::Vector{UInt8}) = throw(ErrorException("write failed"))
    Base.flush(::BadWriteTransport) = nothing
    Base.close(::BadWriteTransport) = nothing

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED)
    transport = BadWriteTransport()
    client.read_io = transport
    client.write_io = transport

    publish(client, "foo", "bar")
    @test status(client) == N.ConnectionStatus.RECONNECTING
    @test client.pending_bytes == length("PUB foo 3\r\nbar\r\n")
    @test stats(client).out_msgs == 1
    close(client)
end

@testitem "foreground subscribe write failure keeps local subscription for replay" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    struct BadSubTransport end
    Base.write(::BadSubTransport, ::String) = throw(ErrorException("write failed"))
    Base.flush(::BadSubTransport) = nothing
    Base.close(::BadSubTransport) = nothing

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED)
    transport = BadSubTransport()
    client.read_io = transport
    client.write_io = transport

    sub = subscribe(client, "foo")
    @test status(client) == N.ConnectionStatus.RECONNECTING
    @test !sub.server_active
    @test sub.sid in keys(client.subscriptions)
    close(client)
end

@testitem "reconnect trigger only closes transport when it owns the transition" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct CountingTransport
        closes::Base.RefValue{Int}
    end
    Base.close(t::CountingTransport) = (t.closes[] += 1)

    closes = Ref(0)
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    transport = CountingTransport(closes)
    client.read_io = transport
    client.write_io = transport

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
    push!(client.pongs, ch)
    close(client)
    @test isready(ch)
    @test take!(ch) == false
end

@testitem "timed out flush consumes its own late pong" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    struct FlushSink end
    Base.write(::FlushSink, data::Vector{UInt8}) = length(data)
    Base.flush(::FlushSink) = nothing

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED)
    client.write_io = FlushSink()

    @test_throws TimeoutError flush(client; timeout=0.01)
    @test length(client.pongs) == 1
    timed_out_waiter = only(client.pongs)
    later_waiter = Channel{Bool}(1)
    push!(client.pongs, later_waiter)

    N._notify_pong(client)
    @test isready(timed_out_waiter)
    @test take!(timed_out_waiter) == true
    @test !isready(later_waiter)
end

@testitem "cleanup errors are reported and optionally thrown" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    struct BadCloseIO end
    Base.close(::BadCloseIO) = throw(ErrorException("close failed"))

    reported = Any[]
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED)
    client.options.error_cb = err -> push!(reported, err)
    client.read_io = BadCloseIO()
    close(client)
    @test length(reported) == 1
    @test reported[1] isa CleanupError

    strict = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED)
    strict.read_io = BadCloseIO()
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

    N._merge_discovered_servers!(client, Dict{String,Any}("connect_urls" => ["10.0.0.2:4222", "nats://plain.test:4222"]))
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
