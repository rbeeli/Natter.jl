using TestItems

@testitem "validation and buffering" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    @test N.ConnectOptions().allow_reconnect
    @test N.ConnectOptions().max_reconnect_attempts == -1
    @test N.ConnectOptions().sub_pending_msgs_limit == 1024

    @test N._validate_subject("foo.*.bar") == "foo.*.bar"
    @test N._validate_subject("foo.>") == "foo.>"
    @test_throws ArgumentError N._validate_subject(".foo")
    @test_throws ArgumentError N._validate_subject("foo.")
    @test_throws ArgumentError N._validate_subject("foo bar")
    @test_throws ArgumentError N._validate_subject("foo\tbar")
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

    ergonomic_headers = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    publish(ergonomic_headers, "foo", "bar"; headers=Dict("Trace" => "abc"))
    publish_frame = String(take!(ergonomic_headers.pending))
    @test startswith(publish_frame, "HPUB foo ")
    @test occursin("NATS/1.0\r\nTrace: abc\r\n\r\nbar\r\n", publish_frame)

    unsupported_headers = TestHelpers.fake_client(;
        status=N.ConnectionStatus.CONNECTED,
        info=N.ServerInfo(; headers=false),
        write_io=IOBuffer(),
    )
    @test_throws UnsupportedFeatureError publish(unsupported_headers, "foo", "bar"; headers=Dict("Trace" => "abc"))
    @test String(take!(unsupported_headers.write_io)) == ""
    @test unsupported_headers.pending_bytes == 0

    invalid_publish_headers = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    @test_throws ArgumentError publish(invalid_publish_headers, "foo", "bar"; headers=Dict("Bad Key" => "abc"))
    @test String(take!(invalid_publish_headers.write_io)) == ""
    @test invalid_publish_headers.pending_bytes == 0

    invalid_request_headers = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    @test_throws ArgumentError request(invalid_request_headers, "svc", "body"; timeout=0.001, headers=Dict("Bad Key" => "abc"))
    @test String(take!(invalid_request_headers.write_io)) == ""
    @test isempty(invalid_request_headers.subscriptions)
    @test invalid_request_headers.pending_bytes == 0

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

@testitem "core APIs accept abstract strings and callable objects" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct CoreCallable
        seen::Vector{String}
    end
    function (cb::CoreCallable)(msg)
        push!(cb.seen, String(msg))
        nothing
    end

    reply = SubString("reply.inbox.extra", 1, 11)
    publish_client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    publish(publish_client, "foo", "bar"; reply)
    @test String(take!(publish_client.pending)) == "PUB foo reply.inbox 3\r\nbar\r\n"

    queue = SubString("workers.extra", 1, 7)
    callback = CoreCallable(String[])
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    sub = subscribe(client, "events"; queue, callback)
    @test sub.queue == "workers"
    @test sub.has_callback

    N._dispatch_msg(client, Msg("events", nothing, TestHelpers.bytes("payload"); sid=sub.sid))
    for _ in 1:100
        isempty(callback.seen) || break
        yield()
    end
    @test callback.seen == ["payload"]
    close(sub)
    wait(sub.processor)

    positional = CoreCallable(String[])
    positional_sub = subscribe(positional, client, "events.positional")
    @test positional_sub.has_callback
    close(positional_sub)
    wait(positional_sub.processor)

    inbox = new_inbox(client; prefix=SubString("_INBOX.extra", 1, 6))
    @test startswith(inbox, "_INBOX.")
end

@testitem "ConnectOptions validates safety limits" begin
    using Natter

    const N = Natter

    opts = N.ConnectOptions(connect_timeout=1, ping_interval=2, max_outstanding_pings=1,
                            reconnect_jitter=0, write_buffer_size=0)
    @test opts.connect_timeout == 1.0
    @test opts.ping_interval == 2.0
    @test opts.write_buffer_size == 0
    @test !ismutable(opts)
    @test opts.servers isa Tuple{Vararg{String}}
    source_servers = ["nats://one.example:4222"]
    frozen = N.ConnectOptions(; servers=source_servers)
    source_servers[1] = "nats://two.example:4222"
    push!(source_servers, "nats://three.example:4222")
    @test frozen.servers == ("nats://one.example:4222",)
    @test N._parse_options(" nats://one.example:4222, nats://two.example:4223 , ").servers == (
        "nats://one.example:4222",
        "nats://two.example:4223",
    )
    @test N._parse_options([" nats://one.example:4222 "]).servers == ("nats://one.example:4222",)
    @test_throws ArgumentError N._parse_options(" , ")
    @test_throws ArgumentError N._parse_options(["nats://one.example:4222", " "])

    function rejects(; kwargs...)
        @test_throws ArgumentError N.ConnectOptions(; kwargs...)
    end

    function rejects_with(message; kwargs...)
        err = try
            N.ConnectOptions(; kwargs...)
            nothing
        catch err
            err
        end
        @test err isa ArgumentError
        @test occursin(message, sprint(showerror, err))
    end

    rejects(servers=String[])
    rejects(servers=[""])
    rejects_with("tls_cert_path and tls_key_path must be provided together"; tls_cert_path="client.pem")
    rejects_with("tls_cert_path and tls_key_path must be provided together"; tls_key_path="client-key.pem")
    rejects_with("token authentication cannot be combined with user/password authentication";
                 token="secret", user="user", password="pass")
    rejects_with("user and password must be provided together"; user="user")
    rejects_with("user and password must be provided together"; password="pass")
    rejects(connect_timeout=0)
    rejects(connect_timeout=Inf)
    rejects(ping_interval=0)
    rejects(ping_interval=true)
    rejects(max_outstanding_pings=0)
    rejects(max_outstanding_pings=true)
    rejects(reconnect_wait=0)
    rejects(reconnect_max_wait=0)
    rejects(reconnect_wait=2.0, reconnect_max_wait=1.0)
    rejects(reconnect_jitter=-0.1)
    rejects(max_reconnect_attempts=-2)
    rejects(pending_size=0)
    rejects(write_buffer_size=-1)
    rejects(max_control_line=0)
    rejects(max_inbound_payload=0)
    rejects(max_header_bytes=0)
    rejects(max_stale_pong_waiters=0)
    rejects(sub_pending_msgs_limit=0)
    rejects(sub_pending_bytes_limit=0)
    rejects(drain_timeout=0)
end

@testitem "connect rejects mixed URL and option authentication before transport IO" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; opts=N.ConnectOptions(token="secret"))
    err = TestHelpers.thrown_exception() do
        N._connect_once!(client, N.Server("nats://user:pass@example.invalid:4222"))
    end
    @test err isa ArgumentError
    @test occursin("token authentication cannot be combined with user/password authentication",
                   sprint(showerror, err))
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

@testitem "subscribe validates per-subscription limits before protocol writes" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    transport = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=transport)
    invalid_kwargs = (
        (; max_msgs=-1),
        (; pending_msgs_limit=0),
        (; pending_msgs_limit=-1),
        (; pending_bytes_limit=0),
        (; pending_bytes_limit=-1),
    )

    for kwargs in invalid_kwargs
        @test_throws ArgumentError subscribe(client, "events"; kwargs...)
        @test TestHelpers.capture_text(transport) == ""
        @test isempty(client.subscriptions)
        @test client.sid == 0
    end
end

@testitem "unsubscribe max_msgs uses additional messages and closes exhausted replay state" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    transport = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=transport)
    sub = subscribe(client, "foo")
    TestHelpers.clear_capture!(transport)

    sub.received = 3
    unsubscribe(sub; max_msgs=2)
    @test TestHelpers.capture_text(transport) == "UNSUB $(sub.sid) 5\r\n"
    @test sub.max_msgs == 5
    @test !sub.closed
    @test sub.sid in keys(client.subscriptions)

    N._dispatch_msg(client, Msg("foo", nothing, TestHelpers.bytes("four"); sid=sub.sid))
    @test !sub.closed
    N._dispatch_msg(client, Msg("foo", nothing, TestHelpers.bytes("five"); sid=sub.sid))
    @test sub.closed
    @test !(sub.sid in keys(client.subscriptions))

    negative = subscribe(client, "negative")
    TestHelpers.clear_capture!(transport)
    @test_throws ArgumentError unsubscribe(negative; max_msgs=-1)
    @test TestHelpers.capture_text(transport) == ""
    @test !negative.closed
    @test negative.sid in keys(client.subscriptions)
    @test_throws ArgumentError subscribe(client, "bad"; max_msgs=-1)

    replay_transport = TestHelpers.WriteCapture()
    replay_client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING,
                                            read_io=replay_transport, write_io=replay_transport)
    replay_sub = subscribe(replay_client, "replay"; max_msgs=5)
    replay_sub.received = 3
    N._replay_subscriptions(replay_client)
    @test TestHelpers.capture_text(replay_transport) ==
          "SUB replay $(replay_sub.sid)\r\nUNSUB $(replay_sub.sid) 2\r\n"
    @test replay_sub.server_active

    exhausted = subscribe(replay_client, "done"; max_msgs=2)
    exhausted.received = 2
    TestHelpers.clear_capture!(replay_transport)
    N._replay_subscriptions(replay_client)
    @test TestHelpers.capture_text(replay_transport) == ""
    @test exhausted.closed
    @test !(exhausted.sid in keys(replay_client.subscriptions))
    @test !isopen(exhausted.messages)
end

@testitem "subscription pending byte limits include HMSG headers" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    function parse_header_msg(client, sid, hdr)
        raw = vcat(TestHelpers.bytes("HMSG events $sid $(length(hdr)) $(length(hdr))\r\n"),
                   hdr,
                   N.CRLF_BYTES)
        frame = N._read_control_or_msg(IOBuffer(raw), client.options)
        @test frame.op == :MSG
        N._protocol_msg(frame)
    end

    hdr = N._headers_bytes(Headers("Trace" => [repeat("x", 64)]))

    accepted = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    accepted_sub = subscribe(accepted, "events"; pending_bytes_limit=length(hdr))
    accepted_msg = parse_header_msg(accepted, accepted_sub.sid, hdr)
    N._dispatch_msg(accepted, accepted_msg)
    @test isready(accepted_sub.messages)
    @test accepted_sub.pending_bytes == length(hdr)
    @test N._msg_pending_bytes(next(accepted_sub; timeout=0.1)) == length(hdr)
    @test accepted_sub.pending_bytes == 0

    reported = Any[]
    opts = N.ConnectOptions(error_cb=err -> push!(reported, err))
    rejected = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.RECONNECTING)
    rejected_sub = subscribe(rejected, "events"; pending_bytes_limit=length(hdr) - 1)
    rejected_msg = parse_header_msg(rejected, rejected_sub.sid, hdr)
    N._dispatch_msg(rejected, rejected_msg)
    @test !isready(rejected_sub.messages)
    @test rejected_sub.pending_bytes == 0
    @test stats(rejected).dropped_msgs == 1
    @test only(reported) isa SlowConsumerError
end

@testitem "next rejects callback subscriptions" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    sub = subscribe(client, "events") do _
        nothing
    end
    try
        err = TestHelpers.thrown_exception(() -> next(sub; timeout=0.1))
        @test err isa ArgumentError
        @test occursin("callback", err.msg)
    finally
        close(sub)
    end
end

@testitem "next timeout survives concurrent direct channel consumption" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    sub = subscribe(client, "events")
    @lock client.lock put!(sub.messages, Msg("events", nothing, TestHelpers.bytes("stolen"); sid=sub.sid))

    lock(client.lock)
    task = @async next(sub; timeout=0.05)
    try
        sleep(0.01)
        stolen = take!(sub.messages)
        @test String(stolen) == "stolen"
    finally
        unlock(client.lock)
    end

    result = timedwait(1.0; pollint=0.001) do
        istaskdone(task)
    end
    @test result != :timed_out
    err = try
        fetch(task)
        nothing
    catch caught
        caught isa TaskFailedException ? first(Base.current_exceptions(task)).exception : caught
    end
    @test err isa TimeoutError
    close(sub)
end

@testitem "publish writes use buffered flusher" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct BufferedTransport <: IO
        bytes::Vector{UInt8}
        flushes::Int
        closed::Bool
    end
    BufferedTransport() = BufferedTransport(UInt8[], 0, false)

    Base.write(t::BufferedTransport, data::Vector{UInt8}) = (append!(t.bytes, data); length(data))
    Base.write(t::BufferedTransport, data::Base.CodeUnits{UInt8}) = (append!(t.bytes, data); length(data))
    Base.write(t::BufferedTransport, data::AbstractString) = (append!(t.bytes, codeunits(data)); ncodeunits(data))
    Base.flush(t::BufferedTransport) = (t.flushes += 1; nothing)
    Base.close(t::BufferedTransport) = (t.closed = true; nothing)

    transport = BufferedTransport()
    client = TestHelpers.fake_client(; opts=N.ConnectOptions(write_buffer_size=1024 * 1024),
                                     status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport,
                                     write_io=N.BufferedWriteIO(transport))
    N._start_flusher_task!(client, client.generation)

    for i in 1:5
        publish(client, "foo", "bar-$i")
    end
    @test client.pending_bytes > 0
    @test isempty(transport.bytes)

    result = timedwait(1.0; pollint=0.001) do
        count(==('\n'), String(copy(transport.bytes))) >= 10
    end
    @test result != :timed_out
    @test client.pending_bytes == 0
    @test transport.flushes == 1
    @test count(line -> startswith(line, "PUB foo "), split(String(copy(transport.bytes)), "\r\n"; keepempty=false)) == 5

    close(client)
    @test transport.closed
end

@testitem "large publishes bypass buffered replay capture" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    opts = N.ConnectOptions(write_buffer_size=16)
    transport = TestHelpers.WriteCapture()
    write_io = N.BufferedWriteIO(transport)
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=write_io)

    payload = repeat("x", 64)
    publish(client, "foo", payload)

    expected = "PUB foo 64\r\n$payload\r\n"
    @test TestHelpers.capture_text(transport) == expected
    @test N._buffered_bytes(write_io) == 0
    @test client.pending_bytes == 0
    @test isempty(N._take_replayable_writes!(write_io))

    close(client)
end

@testitem "direct publish writes use coalesced frame chunks" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct ChunkCapture <: IO
        chunks::Vector{String}
    end
    ChunkCapture() = ChunkCapture(String[])

    Base.write(t::ChunkCapture, data::Vector{UInt8}) = (push!(t.chunks, String(copy(data))); length(data))
    Base.write(t::ChunkCapture, data::Base.CodeUnits{UInt8}) = (push!(t.chunks, String(data)); length(data))
    Base.write(t::ChunkCapture, data::Union{String,SubString{String}}) = (push!(t.chunks, String(data)); ncodeunits(data))
    Base.write(t::ChunkCapture, byte::UInt8) = (push!(t.chunks, String([byte])); 1)
    Base.flush(::ChunkCapture) = nothing
    Base.close(::ChunkCapture) = nothing

    transport = ChunkCapture()
    client = TestHelpers.fake_client(; opts=N.ConnectOptions(write_buffer_size=0),
                                     status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=transport)

    publish(client, "foo", "bar")

    @test transport.chunks == ["PUB foo 3\r\n", "bar", "\r\n"]
    close(client)
end

@testitem "request publish force flushes buffered writes" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct FlushCapture <: IO
        bytes::Vector{UInt8}
        flushes::Int
    end
    FlushCapture() = FlushCapture(UInt8[], 0)

    Base.write(t::FlushCapture, data::Vector{UInt8}) = (append!(t.bytes, data); length(data))
    Base.write(t::FlushCapture, data::Base.CodeUnits{UInt8}) = (append!(t.bytes, data); length(data))
    Base.write(t::FlushCapture, data::AbstractString) = (append!(t.bytes, codeunits(data)); ncodeunits(data))
    Base.flush(t::FlushCapture) = (t.flushes += 1; nothing)
    Base.close(::FlushCapture) = nothing

    transport = FlushCapture()
    write_io = N.BufferedWriteIO(transport)
    client = TestHelpers.fake_client(; opts=N.ConnectOptions(write_buffer_size=1024 * 1024),
                                     status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io)

    @test_throws TimeoutError request(client, "svc", "body"; timeout=0.001)
    written = String(copy(transport.bytes))

    @test transport.flushes >= 1
    @test occursin("SUB _INBOX.", written)
    @test occursin("PUB svc _INBOX.", written)
    @test N._buffered_bytes(write_io) == 0
    close(client)
end

@testitem "connected replayable publishes are bounded by pending_size" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    opts = N.ConnectOptions(pending_size=64, write_buffer_size=1024 * 1024)
    transport = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=N.BufferedWriteIO(transport))

    payload = repeat("x", 50)
    frame_size = ncodeunits("PUB foo 50\r\n$payload\r\n")
    @test frame_size == opts.pending_size

    publish(client, "foo", payload)
    @test client.pending_bytes == frame_size

    try
        publish(client, "foo", payload)
        @test false
    catch err
        @test err isa OutboundBufferLimitError
        @test err.limit == opts.pending_size
        @test err.actual == 2 * frame_size
    end

    @test status(client) == N.ConnectionStatus.CONNECTED
    @test client.pending_bytes == frame_size
    @test isempty(transport.bytes)
    @test stats(client).out_msgs == 1

    close(client)
end

@testitem "reconnect preserves replayable buffered publishes after flusher failure" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct FailingFlushTransport <: IO
        writes::Int
        closed::Bool
    end
    FailingFlushTransport() = FailingFlushTransport(0, false)

    Base.write(t::FailingFlushTransport, data::Vector{UInt8}) = (t.writes += 1; throw(ErrorException("write failed")))
    Base.flush(::FailingFlushTransport) = nothing
    Base.close(t::FailingFlushTransport) = (t.closed = true; nothing)

    reported = Any[]
    opts = N.ConnectOptions(write_buffer_size=1024 * 1024, max_reconnect_attempts=0,
                            error_cb=err -> push!(reported, err))
    transport = FailingFlushTransport()
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=N.BufferedWriteIO(transport))

    N._write_raw(client, "SUB foo 1\r\n")
    publish(client, "foo", "bar")
    expected = "PUB foo 3\r\nbar\r\n"
    @test client.pending_bytes == ncodeunits(expected)

    @test_throws ErrorException N._flush_buffered_writes(client)
    N._trigger_reconnect(client, ErrorException("flush failed"))

    @test client.pending_bytes == ncodeunits(expected)
    @test String(take!(client.pending)) == expected
    @test isnothing(client.write_io)
    @test transport.closed
    !isnothing(client.reconnect_task) && wait(client.reconnect_task)
end

@testitem "foreground buffered publish flush failure is pending once" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct FailingWriteTransport <: IO
        writes::Int
        closed::Bool
    end
    FailingWriteTransport() = FailingWriteTransport(0, false)

    Base.write(t::FailingWriteTransport, data::Vector{UInt8}) = (t.writes += 1; throw(ErrorException("write failed")))
    Base.flush(::FailingWriteTransport) = nothing
    Base.close(t::FailingWriteTransport) = (t.closed = true; nothing)

    opts = N.ConnectOptions(write_buffer_size=0, max_reconnect_attempts=0,
                            error_cb=err -> nothing)
    transport = FailingWriteTransport()
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=N.BufferedWriteIO(transport))

    publish(client, "foo", "bar")

    expected = "PUB foo 3\r\nbar\r\n"
    @test client.pending_bytes == ncodeunits(expected)
    @test String(take!(client.pending)) == expected
    @test stats(client).out_msgs == 1
    !isnothing(client.reconnect_task) && wait(client.reconnect_task)
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

@testitem "pending replay writes bounded batches" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct ReplayBatchTransport <: IO
        writes::Vector{String}
        flushes::Int
    end
    ReplayBatchTransport() = ReplayBatchTransport(String[], 0)

    Base.write(t::ReplayBatchTransport, data::Vector{UInt8}) = (push!(t.writes, String(copy(data))); length(data))
    Base.flush(t::ReplayBatchTransport) = (t.flushes += 1; nothing)
    Base.close(::ReplayBatchTransport) = nothing

    transport = ReplayBatchTransport()
    client = TestHelpers.fake_client(; opts=N.ConnectOptions(write_buffer_size=1024),
                                     status=N.ConnectionStatus.RECONNECTING,
                                     write_io=transport)
    first = TestHelpers.bytes("PUB foo 3\r\none\r\n")
    second = TestHelpers.bytes("PUB bar 3\r\ntwo\r\n")
    N._enqueue_pending(client, first)
    N._enqueue_pending(client, second)

    N._flush_pending_buffer(client)

    @test transport.writes == [String(copy(first)) * String(copy(second))]
    @test transport.flushes == 1
    @test client.pending_bytes == 0
end

@testitem "pending replay failure after close does not resurrect buffer" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct BlockingReplayTransport <: IO
        entered::Channel{Bool}
        release::Channel{Bool}
        closed::Bool
    end
    BlockingReplayTransport() = BlockingReplayTransport(Channel{Bool}(1), Channel{Bool}(1), false)

    function Base.write(t::BlockingReplayTransport, data::Vector{UInt8})
        put!(t.entered, true)
        take!(t.release)
        throw(ErrorException("write failed"))
    end
    Base.flush(::BlockingReplayTransport) = nothing
    Base.close(t::BlockingReplayTransport) = (t.closed = true; nothing)

    transport = BlockingReplayTransport()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING,
                                     read_io=transport, write_io=transport)
    data = TestHelpers.bytes("PUB foo 3\r\nbar\r\n")
    N._enqueue_pending(client, data)

    replay_task = @async try
        N._flush_pending_buffer(client)
        nothing
    catch err
        err
    end

    @test take!(transport.entered)
    close_task = @async close(client)
    @test timedwait(1.0; pollint=0.001) do
        status(client) == N.ConnectionStatus.CLOSED
    end != :timed_out
    put!(transport.release, true)

    @test fetch(replay_task) isa ErrorException
    wait(close_task)
    @test client.pending_bytes == 0
    @test isempty(take!(client.pending))
    @test transport.closed
end

@testitem "terminal disconnect and close clear pending buffer" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    data = TestHelpers.bytes(repeat("x", 100))

    terminal = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    N._enqueue_pending(terminal, data)
    @test terminal.pending_bytes == length(data)
    @test N._terminal_disconnect!(terminal, terminal.generation, N.NoServersError())
    @test status(terminal) == N.ConnectionStatus.DISCONNECTED
    @test terminal.pending_bytes == 0
    @test isempty(take!(terminal.pending))

    closing = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    N._enqueue_pending(closing, data)
    @test closing.pending_bytes == length(data)
    close(closing)
    @test status(closing) == N.ConnectionStatus.CLOSED
    @test closing.pending_bytes == 0
    @test isempty(take!(closing.pending))
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

@testitem "write recovery aborts when callbacks close client" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct CallbackCloseTransport <: IO
        closed::Bool
    end
    CallbackCloseTransport() = CallbackCloseTransport(false)

    Base.write(::CallbackCloseTransport, ::UInt8) = throw(ErrorException("write failed"))
    Base.write(::CallbackCloseTransport, ::Vector{UInt8}) = throw(ErrorException("write failed"))
    Base.write(::CallbackCloseTransport, ::Base.CodeUnits{UInt8}) = throw(ErrorException("write failed"))
    Base.write(::CallbackCloseTransport, ::Union{String,SubString{String}}) = throw(ErrorException("write failed"))
    Base.write(::CallbackCloseTransport, ::AbstractString) = throw(ErrorException("write failed"))
    Base.flush(::CallbackCloseTransport) = nothing
    Base.close(t::CallbackCloseTransport) = (t.closed = true; nothing)

    error_ref = Ref{Any}()
    error_opts = N.ConnectOptions(error_cb=err -> close(error_ref[]))
    error_transport = CallbackCloseTransport()
    error_client = TestHelpers.fake_client(; opts=error_opts, status=N.ConnectionStatus.CONNECTED,
                                           read_io=error_transport, write_io=error_transport)
    error_ref[] = error_client

    @test_throws ErrorException publish(error_client, "foo", "bar")
    @test status(error_client) == N.ConnectionStatus.CLOSED
    @test error_client.pending_bytes == 0
    @test isempty(take!(error_client.pending))
    @test stats(error_client).out_msgs == 0

    disconnected_ref = Ref{Any}()
    disconnected_opts = N.ConnectOptions(error_cb=err -> nothing,
                                         disconnected_cb=() -> close(disconnected_ref[]))
    disconnected_transport = CallbackCloseTransport()
    disconnected_client = TestHelpers.fake_client(; opts=disconnected_opts, status=N.ConnectionStatus.CONNECTED,
                                                  read_io=disconnected_transport, write_io=disconnected_transport)
    disconnected_ref[] = disconnected_client

    @test_throws ErrorException publish(disconnected_client, "foo", "bar")
    @test status(disconnected_client) == N.ConnectionStatus.CLOSED
    @test disconnected_client.pending_bytes == 0
    @test isempty(take!(disconnected_client.pending))
    @test stats(disconnected_client).out_msgs == 0

    raw_ref = Ref{Any}()
    raw_opts = N.ConnectOptions(error_cb=err -> close(raw_ref[]))
    raw_transport = CallbackCloseTransport()
    raw_client = TestHelpers.fake_client(; opts=raw_opts, status=N.ConnectionStatus.CONNECTED,
                                         read_io=raw_transport, write_io=raw_transport)
    raw_ref[] = raw_client

    @test_throws ErrorException N._send_raw(raw_client, TestHelpers.bytes("PING\r\n"); buffer_on_reconnect=true)
    @test status(raw_client) == N.ConnectionStatus.CLOSED
    @test raw_client.pending_bytes == 0
    @test isempty(take!(raw_client.pending))
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

@testitem "request accepts pair-style header input" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    transport = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, read_io=transport, write_io=transport)

    @test_throws TimeoutError request(client, "svc", "body"; timeout=0.001, headers=("Trace" => "abc", "Trace" => "def"))
    request_frame = TestHelpers.capture_text(transport)
    @test occursin("HPUB svc _INBOX.", request_frame)
    @test occursin("NATS/1.0\r\nTrace: abc\r\nTrace: def\r\n\r\nbody\r\n", request_frame)
    close(client)
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
    function Base.write(t::RequestPublishFailTransport, data::Vector{UInt8})
        s = String(copy(data))
        push!(t.writes, s)
        startswith(s, "PUB ") && throw(ErrorException("write failed"))
        length(data)
    end
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
        N._dispatch_msg(client, Msg(reply, nothing, TestHelpers.bytes("resp-$payload"); sid=mux.sub.sid))
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
    N._dispatch_msg(client, Msg(first_reply, nothing, TestHelpers.bytes("late"); sid=mux.sub.sid))
    @test stats(client).dropped_msgs == before_drops + 1

    task = @async request(client, "svc", "second"; timeout=1.0)
    publishes = wait_for_publishes(transport, 2)
    second_reply = publishes[2][1]
    @test second_reply != first_reply

    N._dispatch_msg(client, Msg(first_reply, nothing, TestHelpers.bytes("still-late"); sid=mux.sub.sid))
    sleep(0.02)
    @test !istaskdone(task)
    N._dispatch_msg(client, Msg(second_reply, nothing, TestHelpers.bytes("on-time"); sid=mux.sub.sid))
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

@testitem "reconnect exhaustion closes subscriptions and wakes consumers" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    reported = Any[]
    client = TestHelpers.fake_client(; opts=N.ConnectOptions(max_reconnect_attempts=0, error_cb=err -> push!(reported, err)),
                                     status=N.ConnectionStatus.RECONNECTING)
    sub = subscribe(client, "foo")
    next_task = @async next(sub; timeout=30.0)
    callback_sub = subscribe(client, "bar") do _
        nothing
    end
    processor = callback_sub.processor
    @test !isnothing(processor)

    sleep(0.01)
    N._reconnect_loop(client, client.generation)

    @test status(client) == N.ConnectionStatus.DISCONNECTED
    @test isempty(client.subscriptions)
    @test sub.closed
    @test callback_sub.closed
    @test !isopen(sub.messages)
    @test !isopen(callback_sub.messages)

    @test timedwait(1.0; pollint=0.001) do
        istaskdone(next_task)
    end != :timed_out
    err = try
        fetch(next_task)
        nothing
    catch caught
        caught isa TaskFailedException ? first(Base.current_exceptions(next_task)).exception : caught
    end
    @test err isa ConnectionClosedError
    @test timedwait(1.0; pollint=0.001) do
        istaskdone(processor)
    end != :timed_out
    @test only(reported) isa NoServersError
end

@testitem "terminal disconnect without reconnect closes subscriptions" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    reported = Any[]
    opts = N.ConnectOptions(allow_reconnect=false, error_cb=err -> push!(reported, err))
    transport = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=transport)
    sub = subscribe(client, "foo")
    next_task = @async next(sub; timeout=30.0)

    sleep(0.01)
    reason = ErrorException("lost")
    N._trigger_reconnect(client, reason)

    @test status(client) == N.ConnectionStatus.DISCONNECTED
    @test isempty(client.subscriptions)
    @test sub.closed
    @test !isopen(sub.messages)
    @test transport.closed
    @test timedwait(1.0; pollint=0.001) do
        istaskdone(next_task)
    end != :timed_out
    err = try
        fetch(next_task)
        nothing
    catch caught
        caught isa TaskFailedException ? first(Base.current_exceptions(next_task)).exception : caught
    end
    @test err isa ConnectionClosedError
    @test only(reported) === reason
end

@testitem "server permission errors are transient" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    reported = Any[]
    opts = N.ConnectOptions(error_cb=err -> push!(reported, err))
    transport = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=transport)
    generation = client.generation

    @test !N._handle_server_err!(client, generation, "Permissions Violation for Publish to \"secret\"")
    @test status(client) == N.ConnectionStatus.CONNECTED
    @test client.generation == generation
    @test !transport.closed
    @test only(reported) isa PermissionViolationError
end

@testitem "server auth errors keep auth-specific types" begin
    using Natter

    const N = Natter

    @test N._server_err("Authorization Violation") isa AuthorizationError
    @test N._server_err("User Authentication Expired") isa AuthenticationExpiredError
    @test N._server_err("User Authentication Revoked") isa AuthenticationRevokedError
    @test N._server_err("Account Authentication Expired") isa AccountAuthenticationExpiredError
    @test N._server_err("Permissions Violation for Subscription to \"foo\"") isa PermissionViolationError
    @test N._server_err("stale connection") isa ProtocolError
end

@testitem "reconnect aborts after repeated auth error on same server" setup=[TestHelpers] begin
    using Natter
    using Sockets

    const N = Natter

    listener = listen(ip"127.0.0.1", 0)
    port = Int(getsockname(listener)[2])
    attempts = Ref(0)
    server_task = @async begin
        try
            while attempts[] < 2
                sock = accept(listener)
                attempts[] += 1
                try
                    write(sock, "INFO {}\r\n")
                    flush(sock)
                    readline(sock)
                    readline(sock)
                    write(sock, "-ERR 'Authorization Violation'\r\n")
                    flush(sock)
                finally
                    close(sock)
                end
            end
        catch err
            (err isa InvalidStateException || err isa InterruptException || err isa Base.IOError) || rethrow()
        end
    end

    try
        reported = Any[]
        url = "nats://127.0.0.1:$port"
        opts = N.ConnectOptions(; servers=[url], connect_timeout=1.0, reconnect_wait=0.01,
                                reconnect_jitter=0.0, max_reconnect_attempts=-1,
                                error_cb=err -> push!(reported, err))
        client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.RECONNECTING)
        push!(client.servers, N.Server(url))

        N._reconnect_loop(client, client.generation)

        @test attempts[] == 2
        @test status(client) == N.ConnectionStatus.DISCONNECTED
        @test length(reported) == 2
        @test all(err -> err isa AuthorizationError, reported)
    finally
        close(listener)
        done = timedwait(0.5; pollint=0.01) do
            istaskdone(server_task)
        end
        done == :timed_out || wait(server_task)
    end
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

@testitem "timeout cleanup reports returned and thrown cleanup failures" begin
    using Natter

    const N = Natter

    returned_reported = Channel{Any}(1)
    returned_done = Channel{Bool}(1)
    returned_release = Channel{Bool}(1)
    returned_error = CleanupError("close timed-out transport", ErrorException("close failed"))
    returned_cleanup = () -> begin
        put!(returned_release, true)
        [returned_error]
    end
    returned_reporter = errors -> begin
        foreach(err -> put!(returned_reported, err), errors)
        put!(returned_done, true)
    end

    @test_throws TimeoutError N._run_with_timeout("returned cleanup", 0.01, returned_cleanup, returned_reporter) do
        take!(returned_release)
        nothing
    end
    @test timedwait(0.5; pollint=0.01) do
        isready(returned_done)
    end != :timed_out
    @test take!(returned_reported) === returned_error

    thrown_reported = Channel{Any}(1)
    thrown_done = Channel{Bool}(1)
    thrown_release = Channel{Bool}(1)
    cleanup_exception = ErrorException("cleanup failed")
    thrown_cleanup = () -> begin
        put!(thrown_release, true)
        throw(cleanup_exception)
    end
    thrown_reporter = errors -> begin
        foreach(err -> put!(thrown_reported, err), errors)
        put!(thrown_done, true)
    end

    @test_throws TimeoutError N._run_with_timeout("thrown cleanup", 0.01, thrown_cleanup, thrown_reporter) do
        take!(thrown_release)
        nothing
    end
    @test timedwait(0.5; pollint=0.01) do
        isready(thrown_done)
    end != :timed_out
    thrown_error = take!(thrown_reported)
    @test thrown_error isa CleanupError
    @test thrown_error.operation == "timeout cleanup after thrown cleanup"
    @test thrown_error.cause === cleanup_exception
end

@testitem "close notifies pending flush waiters" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED)
    waiter = N.PongWaiter(Base.Threads.Condition(client.lock))
    push!(client.pongs, waiter)
    close(client)
    @test waiter.ready
    @test waiter.value == false
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
    later_waiter = N.PongWaiter(Base.Threads.Condition(client.lock))
    push!(client.pongs, later_waiter)

    N._notify_pong(client)
    @test !timed_out_waiter.active
    @test !timed_out_waiter.ready
    @test !later_waiter.ready
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
    @test count(waiter -> !waiter.active && !waiter.ready, client.pongs) == 2
end

@testitem "drain uses one timeout budget" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct CoordinatedPongTransport <: IO
        client::Base.RefValue{Any}
        sub::Base.RefValue{Any}
        pong_delay::Float64
        release_delay::Float64
        released::Channel{Bool}
    end
    Base.write(::CoordinatedPongTransport, data::Vector{UInt8}) = length(data)
    Base.write(::CoordinatedPongTransport, data::String) = ncodeunits(data)
    Base.flush(t::CoordinatedPongTransport) = begin
        client = t.client[]
        sub = t.sub[]
        @async begin
            sleep(t.pong_delay)
            N._notify_pong(client)
            if !isnothing(sub)
                sleep(t.release_delay)
                isready(sub.messages) && take!(sub.messages)
                put!(t.released, true)
            end
        end
        nothing
    end
    Base.close(::CoordinatedPongTransport) = nothing

    warm_client_ref = Ref{Any}(nothing)
    warm_sub_ref = Ref{Any}(nothing)
    warm_released = Channel{Bool}(1)
    warm_transport = CoordinatedPongTransport(warm_client_ref, warm_sub_ref, 0.0, 0.01, warm_released)
    warm_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=warm_transport)
    warm_client_ref[] = warm_client
    warm_sub = subscribe(warm_client, "warmup")
    warm_sub_ref[] = warm_sub
    put!(warm_sub.messages, N.Msg("warmup", nothing, UInt8[]; sid=warm_sub.sid))
    @test_throws TimeoutError drain(warm_sub; timeout=0.001)
    warm_release_result = timedwait(1.0; pollint=0.01) do
        isready(warm_released)
    end
    @test warm_release_result != :timed_out
    close(warm_client)

    client_ref = Ref{Any}(nothing)
    sub_ref = Ref{Any}(nothing)
    released = Channel{Bool}(1)
    transport = CoordinatedPongTransport(client_ref, sub_ref, 0.12, 0.14, released)
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=transport)
    client_ref[] = client

    sub = subscribe(client, "foo")
    sub_ref[] = sub
    put!(sub.messages, N.Msg("foo", nothing, UInt8[]; sid=sub.sid))

    @test_throws TimeoutError drain(sub; timeout=0.2)
    release_result = timedwait(1.0; pollint=0.01) do
        isready(released)
    end
    @test release_result != :timed_out

    close(client)
end

@testitem "client drain shares timeout across subscriptions" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct ImmediatePongTransport <: IO
        client::Base.RefValue{Any}
        writes::Vector{String}
    end
    Base.write(t::ImmediatePongTransport, data::Vector{UInt8}) = (push!(t.writes, String(copy(data))); length(data))
    Base.write(t::ImmediatePongTransport, data::String) = (push!(t.writes, data); ncodeunits(data))
    Base.flush(t::ImmediatePongTransport) = (N._notify_pong(t.client[]); nothing)
    Base.close(::ImmediatePongTransport) = nothing

    reported = Any[]
    opts = N.ConnectOptions(error_cb=err -> push!(reported, err))
    client_ref = Ref{Any}(nothing)
    transport = ImmediatePongTransport(client_ref, String[])
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED, write_io=transport)
    client_ref[] = client

    sub1 = subscribe(client, "foo.1")
    sub2 = subscribe(client, "foo.2")
    put!(sub1.messages, N.Msg("foo.1", nothing, UInt8[]; sid=sub1.sid))
    put!(sub2.messages, N.Msg("foo.2", nothing, UInt8[]; sid=sub2.sid))
    empty!(transport.writes)

    @test_throws TimeoutError drain(client; timeout=0.12)
    @test count(startswith("UNSUB "), transport.writes) == 1
    @test count(==("PING\r\n"), transport.writes) == 1
    @test length(reported) == 1
    @test status(client) == N.ConnectionStatus.CLOSED
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

@testitem "default clients retain concrete protocol readers" setup=[TestHelpers] begin
    using Natter
    using Sockets

    const N = Natter

    client = TestHelpers.fake_client()
    sock = Sockets.TCPSocket()
    try
        reader = N.ProtocolReader(sock)
        client.read_io = sock
        client.reader = reader

        @test client.reader === reader
        @test typeof(client.reader) === N.ProtocolReader{Sockets.TCPSocket}
    finally
        close(sock)
    end
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

@testitem "discovered server merge prunes stale routes" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    callbacks = Ref(0)
    opts = N.ConnectOptions(; discovered_server_cb=() -> (callbacks[] += 1))
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED)

    seed = N.Server("nats://seed.test:4222")
    current = N.Server("nats://current.test:4222"; discovered=true)
    kept = N.Server("nats://kept.test:4222"; discovered=true)
    stale = N.Server("nats://stale.test:4222"; discovered=true)
    append!(client.servers, [seed, current, kept, stale])
    client.current_server = current
    client.connected_url = current.url

    N._merge_discovered_servers!(client, N.ServerInfo(; connect_urls=["kept.test:4222", "new.test:4222", "new.test:4222"]))
    @test [server.url for server in client.servers] == [
        "nats://seed.test:4222",
        "nats://current.test:4222",
        "nats://kept.test:4222",
        "nats://new.test:4222",
    ]
    @test callbacks[] == 1

    N._merge_discovered_servers!(client, N.ServerInfo(; connect_urls=String[]))
    @test [server.url for server in client.servers] == [
        "nats://seed.test:4222",
        "nats://current.test:4222",
    ]
    @test callbacks[] == 1
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

    conf = N._tls_config(ConnectOptions())
    @test isdefined(conf, :chain)
    @test conf.chain isa MbedTLS.CRT
end
