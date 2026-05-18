using TestItems

@testitem "deadline queue tracks earliest insertions" begin
    using Natter

    const N = Natter

    queue = N._DeadlineQueue{String}()
    @test N._deadline_queue_push!(queue, 1, 10.0, "ten")
    @test !N._deadline_queue_push!(queue, 2, 20.0, "twenty")
    @test N._deadline_queue_push!(queue, 3, 5.0, "five")

    first = N._deadline_queue_pop!(queue)
    @test first.token == 3
    @test first.deadline == 5.0
    @test first.value == "five"

    second = N._deadline_queue_pop!(queue)
    @test second.token == 1
    @test second.deadline == 10.0
    @test second.value == "ten"

    third = N._deadline_queue_pop!(queue)
    @test third.token == 2
    @test third.deadline == 20.0
    @test third.value == "twenty"
    @test isnothing(N._deadline_queue_pop!(queue))

    for i in 1:65
        N._deadline_queue_push!(queue, i, Float64(i), string(i))
    end
    @test N._deadline_queue_compaction_due(queue, 1)
    @test !N._deadline_queue_compaction_due(queue, 65)
end

@testitem "validation and buffering" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    @test N.ConnectOptions().allow_reconnect
    @test !N.ConnectOptions().retry_on_initial_connect
    @test N.ConnectOptions().max_reconnect_attempts == -1
    @test N.ConnectOptions(pending_size=0).pending_size == 0
    @test N.ConnectOptions().read_buffer_size == 64 * 1024
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
    @test N._validate_queue("workers.v1") == "workers.v1"
    @test_throws ArgumentError N._validate_queue("workers.*")
    @test_throws ArgumentError N._validate_queue("workers.>")
    @test_throws ArgumentError N._validate_queue("workers..v1")
    @test_throws ArgumentError N._validate_queue("workers\x7fv1")
    @test N.ConnectOptions(; inbox_prefix="CUSTOM.INBOX").inbox_prefix == "CUSTOM.INBOX"
    @test_throws ArgumentError N.ConnectOptions(; inbox_prefix="")
    @test_throws ArgumentError N.ConnectOptions(; inbox_prefix=".INBOX")
    @test_throws ArgumentError N.ConnectOptions(; inbox_prefix="INBOX.")
    @test_throws ArgumentError N.ConnectOptions(; inbox_prefix="INBOX.*")
    @test_throws ArgumentError N.ConnectOptions(; inbox_prefix=123)

    stats_opts = N.ConnectOptions(record_stats=true)
    client = TestHelpers.fake_client(; opts=stats_opts, status=N.ConnectionStatus.RECONNECTING)
    publish(client, "foo", "bar"; buffer_on_reconnect=true)
    @test client.pending_bytes == length("PUB foo 3\r\nbar\r\n")
    @test stats(client).out_msgs == 1

    binary_payload = TestHelpers.bytes("bin")
    binary_pending = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    publish(binary_pending, "foo", binary_payload; buffer_on_reconnect=true)
    binary_payload[1] = UInt8('B')
    @test String(take!(binary_pending.pending)) == "PUB foo 3\r\nbin\r\n"

    no_replay = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    @test_throws ConnectionReconnectingError publish(no_replay, "foo", "bar";
                                                     buffer_on_reconnect=false)
    @test no_replay.pending_bytes == 0

    hot_payload = fill(UInt8('x'), 64 * 1024)
    direct_frame = N._publish_frame("foo", nothing, hot_payload, nothing)
    @test direct_frame.payload === hot_payload
    direct_frame = nothing
    GC.gc()
    @test (@allocated N._publish_frame("foo", nothing, hot_payload, nothing)) < 1024
    hot_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=devnull)
    publish(hot_client, "foo", hot_payload)
    GC.gc()
    @test (@allocated publish(hot_client, "foo", hot_payload)) < 1024

    ergonomic_headers = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    publish(ergonomic_headers, "foo", "bar"; headers=Dict("Trace" => "abc"),
            buffer_on_reconnect=true)
    publish_frame = String(take!(ergonomic_headers.pending))
    @test startswith(publish_frame, "HPUB foo ")
    @test occursin("NATS/1.0\r\nTrace: abc\r\n\r\nbar\r\n", publish_frame)

    prepared_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    prepared = prepare_publish("prepared.subject", "body"; headers=Headers("Trace" => "abc"))
    @test prepared isa PublishFrame
    publish(prepared_client, prepared)
    prepared_frame = String(take!(prepared_client.write_io))
    @test startswith(prepared_frame, "HPUB prepared.subject ")
    @test occursin("NATS/1.0\r\nTrace: abc\r\n\r\nbody\r\n", prepared_frame)
    empty_prepared = prepare_publish("prepared.empty")
    @test isempty(empty_prepared.payload)
    @test_throws MethodError push!(empty_prepared.payload, 0x41)
    @test isempty(prepare_publish("prepared.empty.again").payload)

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

    invalid_raw_publish_headers = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    invalid_raw_headers = TestHelpers.bytes("NATS/1.0\r\nBad Key: abc\r\n\r\n")
    @test_throws ArgumentError publish(invalid_raw_publish_headers, "foo", "bar"; headers=invalid_raw_headers)
    @test String(take!(invalid_raw_publish_headers.write_io)) == ""
    @test invalid_raw_publish_headers.pending_bytes == 0

    structured_payload = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    @test_throws ArgumentError publish(structured_payload, "foo", (; id=1))
    @test String(take!(structured_payload.write_io)) == ""
    @test structured_payload.pending_bytes == 0
    @test_throws ArgumentError prepare_publish("prepared.structured", (; id=1))

    invalid_request_headers = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    @test_throws ArgumentError request(invalid_request_headers, "svc", "body"; timeout=0.001, headers=Dict("Bad Key" => "abc"))
    @test String(take!(invalid_request_headers.write_io)) == ""
    @test isempty(invalid_request_headers.subscriptions)
    @test invalid_request_headers.pending_bytes == 0

    headers = Headers("Trace" => [repeat("x", 16)])
    payload = TestHelpers.bytes("body")
    total = N._pub_payload_size(payload, N._headers_bytes(headers))
    max_payload = total - 1
    limited = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING,
                                      info=N.ServerInfo(; headers=true, max_payload))
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

    disabled = TestHelpers.fake_client(; opts=N.ConnectOptions(pending_size=0),
                                       status=N.ConnectionStatus.RECONNECTING)
    @test_throws ConnectionReconnectingError publish(disabled, "foo", "bar")
    @test_throws ConnectionReconnectingError N._send_raw(disabled, TestHelpers.bytes("PING\r\n");
                                                        buffer_on_reconnect=true)
    @test disabled.pending_bytes == 0
    @test isempty(take!(disabled.pending))

    closed = TestHelpers.fake_client(; status=N.ConnectionStatus.DISCONNECTED)
    @test_throws ConnectionClosedError publish(closed, "foo", "bar")
    @test_throws ConnectionClosedError subscribe(closed, "foo")
end

@testitem "stats byte counters include headers" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    opts = N.ConnectOptions(record_stats=true)

    outbound = TestHelpers.fake_client(; opts,
                                       status=N.ConnectionStatus.CONNECTED,
                                       info=N.ServerInfo(; headers=true),
                                       write_io=IOBuffer())
    hdrs = Headers("Trace" => "abc")
    hdr_bytes = N._headers_bytes(hdrs)
    payload = TestHelpers.bytes("body")
    publish(outbound, "events", payload; headers=hdrs)
    @test stats(outbound).out_msgs == 1
    @test stats(outbound).out_bytes == length(hdr_bytes) + length(payload)

    inbound = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.RECONNECTING)
    sub = subscribe(inbound, "events")
    raw = vcat(TestHelpers.bytes("HMSG events $(sub.sid) $(length(hdr_bytes)) $(length(hdr_bytes) + length(payload))\r\n"),
               hdr_bytes,
               payload,
               N.CRLF_BYTES)
    frame = N._read_control_or_msg(IOBuffer(raw), inbound.options)
    @test frame.op == :MSG
    N._dispatch_msg(inbound, N._protocol_msg(frame))
    @test stats(inbound).in_msgs == 1
    @test stats(inbound).in_bytes == length(hdr_bytes) + length(payload)
end

@testitem "hot path allocation behavior" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    function discard_buffered_replay!(client, write_io)
        replayable = write_io.replayable_bytes
        N._take_replayable_bytes!(write_io)
        N._release_pending_bytes!(client, replayable)
        nothing
    end

    function queue_put_take_alloc()
        q = N.MsgQueue{Msg}(8)
        msg = Msg("foo", nothing, TestHelpers.bytes("x"); sid=1)
        for _ in 1:1000
            put!(q, msg)
            take!(q)
        end
        GC.gc()
        put_alloc = @allocated put!(q, msg)
        take_alloc = @allocated take!(q)
        put_alloc + take_alloc
    end

    function dispatch_take_alloc()
        client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=devnull)
        sub = subscribe(client, "foo")
        msg = Msg("foo", nothing, TestHelpers.bytes("x"); sid=sub.sid)
        for _ in 1:1000
            N._dispatch_msg(client, msg)
            ready, _ = N._take_subscription_msg_ready!(sub)
            ready || throw(AssertionError("expected ready message"))
        end
        GC.gc()
        @allocated begin
            for _ in 1:1000
                N._dispatch_msg(client, msg)
                ready, _ = N._take_subscription_msg_ready!(sub)
                ready || throw(AssertionError("expected ready message"))
            end
        end
    end

    mutable struct BorrowedCounter
        bytes::Int
    end

    function (counter::BorrowedCounter)(msg::BorrowedMsg)
        counter.bytes += length(msg.data)
        nothing
    end

    function borrowed_dispatch_alloc()
        client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=devnull)
        counter = BorrowedCounter(0)
        sub = subscribe(client, "foo"; callback=counter, borrowed=true)
        data = TestHelpers.bytes("x")
        msg = BorrowedMsg("foo", nothing, @view(data[1:1]), nothing, sub.sid, 0)
        for _ in 1:1000
            N._dispatch_msg(client, msg)
        end
        GC.gc()
        @allocated begin
            for _ in 1:1000
                N._dispatch_msg(client, msg)
            end
        end
    end

    mutable struct BorrowedReaderCounter
        count::Int
        bytes::Int
        dispatch_views::Bool
    end

    function (counter::BorrowedReaderCounter)(msg::BorrowedMsg)
        counter.count += 1
        counter.bytes += length(msg.data)
        counter.dispatch_views &= msg.data isa N._BorrowedDispatchData
        nothing
    end

    function borrowed_reader_dispatch_alloc(payload::AbstractString)
        client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=devnull)
        counter = BorrowedReaderCounter(0, 0, true)
        sub = subscribe(client, "events"; callback=counter, borrowed=true)
        n = 1000
        raw = UInt8[]
        for _ in 1:n
            append!(raw, codeunits("MSG events $(sub.sid) $(ncodeunits(payload))\r\n"))
            append!(raw, codeunits(payload))
            append!(raw, N.CRLF_BYTES)
        end
        reader = N.ProtocolReader(IOBuffer(UInt8[]))
        sizehint!(reader.buffer, length(raw))
        route_resolver = N._ReaderMsgRouteResolver(client)
        handler = N._ReaderMsgDispatcher(client)

        function prefill!()
            empty!(reader.buffer)
            append!(reader.buffer, raw)
            reader.first = 1
            reader.last = length(raw)
            reader.subject_cache[sub.sid] = "events"
            nothing
        end

        function drain!()
            for _ in 1:n
                frame = N._read_control_or_msg_dispatch(reader, client.options,
                                                        route_resolver, handler)
                isnothing(frame) || throw(AssertionError("unexpected control frame"))
            end
            nothing
        end

        prefill!()
        drain!()
        counter.count = 0
        counter.bytes = 0
        counter.dispatch_views = true

        prefill!()
        allocated = @allocated drain!()
        (allocated, counter.count, counter.bytes, counter.dispatch_views)
    end

    function buffered_publish_alloc()
        opts = N.ConnectOptions(write_buffer_size=1024)
        write_io = N.BufferedWriteIO(devnull)
        client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED, write_io)
        payload = fill(UInt8('x'), 64)
        for _ in 1:1000
            publish(client, "foo", payload)
            discard_buffered_replay!(client, write_io)
        end
        GC.gc()
        @allocated publish(client, "foo", payload)
    end

    function buffered_replay_snapshot()
        opts = N.ConnectOptions(write_buffer_size=1024)
        write_io = N.BufferedWriteIO(devnull)
        client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED, write_io)
        payload = TestHelpers.bytes("bar")
        publish(client, "foo", payload; buffer_on_reconnect=true)
        payload[1] = UInt8('B')
        entries = N._take_replayable_writes!(write_io)
        only(entries).is_publish || throw(AssertionError("expected publish replay entry"))
        String(N._pending_entries_write_bytes(entries))
    end

    @test queue_put_take_alloc() == 0
    @test !(:callback in fieldnames(N.Subscription))
    @test !(Any in fieldtypes(N.Subscription{N.Client}))
    @test dispatch_take_alloc() == 0
    @test borrowed_dispatch_alloc() == 0
    nonempty_reader_alloc, nonempty_count, nonempty_bytes, nonempty_views = borrowed_reader_dispatch_alloc("x")
    @test nonempty_reader_alloc <= 1024
    @test nonempty_count == 1000
    @test nonempty_bytes == 1000
    @test nonempty_views
    empty_reader_alloc, empty_count, empty_bytes, empty_views = borrowed_reader_dispatch_alloc("")
    @test empty_reader_alloc <= 1024
    @test empty_count == 1000
    @test empty_bytes == 0
    @test empty_views
    @test buffered_publish_alloc() == 0
    @test buffered_replay_snapshot() == "PUB foo 3\r\nbar\r\n"
end

@testitem "reader dispatch resolves subscription route once per message" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct CountingRouteResolver{R}
        inner::R
        count::Int
    end

    function (resolver::CountingRouteResolver)(sid::Int)
        resolver.count += 1
        N._resolve_message_route(resolver.inner, sid)
    end

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=devnull)
    sub = subscribe(client, "route.once"; pending_msgs_limit=4)
    hdr = N._headers_bytes(Headers("Trace" => ["abc"]))
    payload = vcat(hdr, TestHelpers.bytes("two"))
    raw = UInt8[]
    append!(raw, TestHelpers.bytes("MSG route.once $(sub.sid) 3\r\none\r\n"))
    append!(raw, TestHelpers.bytes("HMSG route.once $(sub.sid) $(length(hdr)) $(length(payload))\r\n"))
    append!(raw, payload)
    append!(raw, N.CRLF_BYTES)

    reader = N.ProtocolReader(IOBuffer(raw); read_size=length(raw))
    resolver = CountingRouteResolver(N._ReaderMsgRouteResolver(client), 0)
    handler = N._ReaderMsgDispatcher(client)

    @test isnothing(N._read_control_or_msg_dispatch(reader, client.options, resolver, handler))
    @test isnothing(N._read_control_or_msg_dispatch(reader, client.options, resolver, handler))
    @test resolver.count == 2
    @test String(N.next(sub; timeout=0.1)) == "one"
    hmsg = N.next(sub; timeout=0.1)
    @test String(hmsg) == "two"
    @test header(hmsg, "Trace") == "abc"
    close(sub)
end

@testitem "empty subscription ready helper returns a typed empty result" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=devnull)
    sub = subscribe(client, "empty.ready")

    ready, msg = N._take_subscription_msg_ready!(sub)
    @test ready == false
    @test msg === N.EMPTY_MSG
end

@testitem "core timeout arguments are validated before protocol writes" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    for invalid_timeout in (-1.0, 0.0, Inf, NaN, true)
        request_capture = TestHelpers.WriteCapture()
        request_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED,
                                                 write_io=request_capture)
        @test_throws ArgumentError request(request_client, "svc", "body"; timeout=invalid_timeout)
        @test TestHelpers.capture_text(request_capture) == ""
        @test isempty(request_client.subscriptions)
        @test request_client.pending_bytes == 0

        flush_capture = TestHelpers.WriteCapture()
        flush_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED,
                                               write_io=flush_capture)
        @test_throws ArgumentError flush(flush_client; timeout=invalid_timeout)
        @test TestHelpers.capture_text(flush_capture) == ""
        @test isempty(flush_client.pongs)

        sub_drain_capture = TestHelpers.WriteCapture()
        sub_drain_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED,
                                                   write_io=sub_drain_capture)
        sub = subscribe(sub_drain_client, "foo")
        TestHelpers.clear_capture!(sub_drain_capture)
        @test_throws ArgumentError drain(sub; timeout=invalid_timeout)
        @test TestHelpers.capture_text(sub_drain_capture) == ""
        close(sub_drain_client)

        client_drain_capture = TestHelpers.WriteCapture()
        client_drain_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED,
                                                      write_io=client_drain_capture)
        subscribe(client_drain_client, "bar")
        TestHelpers.clear_capture!(client_drain_capture)
        @test_throws ArgumentError drain(client_drain_client; timeout=invalid_timeout)
        @test TestHelpers.capture_text(client_drain_capture) == ""
        @test N.status(client_drain_client) == N.ConnectionStatus.CONNECTED
        close(client_drain_client)
    end
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
    publish(publish_client, "foo", "bar"; reply, buffer_on_reconnect=true)
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

    borrowed_seen = Ref{Any}(nothing)
    borrowed_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED,
                                              write_io=IOBuffer())
    borrowed_sub = subscribe(borrowed_client, "events.borrowed"; borrowed=true) do msg
        borrowed_seen[] = msg
        @test msg isa BorrowedMsg
        @test String(msg) == "borrowed"
    end
    @test borrowed_sub.has_callback
    @test borrowed_sub.borrowed_callback
    @test isnothing(borrowed_sub.processor)

    raw = TestHelpers.bytes("MSG events.borrowed $(borrowed_sub.sid) 8\r\nborrowed\r\n")
    reader = N.ProtocolReader(IOBuffer(raw); read_size=length(raw))
    frame = N._read_control_or_msg(reader, borrowed_client.options,
                                   N._ReaderMsgRouteResolver(borrowed_client))
    borrowed_msg = N._protocol_msg(frame)
    @test borrowed_msg isa BorrowedMsg
    @test parent(borrowed_msg.data) === reader.buffer
    N._dispatch_msg(borrowed_client, borrowed_msg)
    @test borrowed_seen[] === borrowed_msg
    @test borrowed_sub.received == 1
    @test borrowed_sub.processing == 0
    @test !isready(borrowed_sub.messages)
    close(borrowed_sub)

    @test_throws ArgumentError subscribe(borrowed_client, "events.no-callback"; borrowed=true)

    positional = CoreCallable(String[])
    positional_sub = subscribe(positional, client, "events.positional")
    @test positional_sub.has_callback
    close(positional_sub)
    wait(positional_sub.processor)

    inbox = new_inbox(client; prefix=SubString("_INBOX.extra", 1, 6))
    @test startswith(inbox, "_INBOX.")
    @test_throws ArgumentError new_inbox(client; prefix="bad.*")
end

@testitem "ConnectOptions validates safety limits" begin
    using Natter

    const N = Natter

    opts = N.ConnectOptions(connect_timeout=1, ping_interval=2, max_outstanding_pings=1,
                            reconnect_jitter=0, read_buffer_size=8192, write_buffer_size=0,
                            direct_write_threshold=4096, write_timeout=3,
                            close_callback_timeout=4)
    @test opts.connect_timeout == 1.0
    @test opts.ping_interval == 2.0
    @test opts.read_buffer_size == 8192
    @test opts.write_buffer_size == 0
    @test opts.direct_write_threshold == 4096
    @test opts.write_timeout == 3.0
    @test opts.close_callback_timeout == 4.0
    @test opts.randomize_servers
    @test N.CLIENT_VERSION == string(pkgversion(N))
    @test N.ConnectOptions(write_timeout=Inf).write_timeout == Inf
    @test N.ConnectOptions(close_callback_timeout=0).close_callback_timeout == 0.0
    @test !ismutable(opts)
    cold_opts = N.ConnectOptions(name=SubString("client-extra", 1, 6),
                                 verbose=true, pedantic=true, no_echo=true,
                                 tls_required=true, tls_first=false, tls_verify=false,
                                 tls_ca_path=SubString("ca.pem.extra", 1, 6),
                                 tls_cert_path=SubString("client.pem.extra", 1, 10),
                                 tls_key_path=SubString("key.pem.extra", 1, 7),
                                 allow_reconnect=false, retry_on_initial_connect=true,
                                 randomize_servers=false, record_stats=true)
    @test cold_opts.name == "client"
    @test cold_opts.verbose
    @test cold_opts.pedantic
    @test cold_opts.no_echo
    @test cold_opts.tls_required
    @test cold_opts.tls_first == false
    @test cold_opts.tls_verify == false
    @test cold_opts.tls_ca_path == "ca.pem"
    @test cold_opts.tls_cert_path == "client.pem"
    @test cold_opts.tls_key_path == "key.pem"
    @test !cold_opts.allow_reconnect
    @test cold_opts.retry_on_initial_connect
    @test !cold_opts.randomize_servers
    @test cold_opts.record_stats
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
    tuple_opts = N._parse_options((" nats://one.example:4222 ", "nats://two.example:4222"))
    @test tuple_opts.servers == (
        "nats://one.example:4222",
        "nats://two.example:4222",
    )
    @test_throws ArgumentError N._parse_options(" , ")
    @test_throws ArgumentError N._parse_options(["nats://one.example:4222", " "])
    @test_throws ArgumentError N._parse_options(("nats://one.example:4222", " "))

    seed = "SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY"
    creds = join([
        "-----BEGIN NATS USER JWT-----",
        "header.payload.signature",
        "------END NATS USER JWT------",
        "",
        "-----BEGIN USER NKEY SEED-----",
        seed,
        "------END USER NKEY SEED------",
    ], "\n")
    seed_opts = N.ConnectOptions(; auth=N.NKeyAuth(; seed))
    @test seed_opts.auth.seed isa N.SecretBytes
    jwt_opts = N.ConnectOptions(; auth=N.JwtAuth(; jwt="header.payload.signature", seed))
    @test jwt_opts.auth.jwt isa N.SecretBytes
    token_opts = N.ConnectOptions(; auth=N.TokenAuth("token-secret-123"))
    @test token_opts.auth.token isa N.SecretBytes
    @test !hasfield(N.SecretBytes, :bytes)
    @test_throws CanonicalIndexError setindex!(token_opts.auth.token, UInt8('x'), 1)
    @test N._secret_to_string(token_opts.auth.token) == "token-secret-123"
    @test !occursin("token-secret-123", sprint(show, token_opts))
    @test occursin("auth=TokenAuth(<redacted>)", sprint(show, token_opts))
    @test !occursin("token-secret-123", sprint(show, MIME("text/plain"), token_opts))
    userpass_opts = N.ConnectOptions(; auth=N.UserPassAuth("auth-user-123", "password-secret-123"))
    @test userpass_opts.auth.password isa N.SecretBytes
    userpass_show = sprint(show, userpass_opts)
    @test !occursin("auth-user-123", userpass_show)
    @test !occursin("password-secret-123", userpass_show)
    @test occursin("auth=UserPassAuth(<redacted>)", userpass_show)
    url_auth_opts = N.ConnectOptions(; servers=(
        "nats://url-token-123@nats.example:4222",
        "tls://url-user-123:url-pass-123@nats.example:4223",
        "nats://url-user-raw:url-pass-raw@extra@nats.example:4224",
    ))
    url_auth_show = sprint(show, url_auth_opts)
    @test !occursin("url-token-123", url_auth_show)
    @test !occursin("url-user-123", url_auth_show)
    @test !occursin("url-pass-123", url_auth_show)
    @test !occursin("url-user-raw", url_auth_show)
    @test !occursin("url-pass-raw", url_auth_show)
    @test !occursin("extra", url_auth_show)
    @test occursin("nats://<redacted>@nats.example:4222", url_auth_show)
    @test occursin("tls://<redacted>@nats.example:4223", url_auth_show)
    @test occursin("nats://<redacted>@nats.example:4224", url_auth_show)
    secret_opts = N.ConnectOptions(; auth=N.CredentialsAuth(creds))
    @test secret_opts.auth.credentials isa N.SecretBytes
    @test !occursin(seed, sprint(show, secret_opts))
    @test !occursin("header.payload.signature", sprint(show, secret_opts))
    @test occursin("auth=CredentialsAuth(<redacted>)", sprint(show, secret_opts))

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

    function rejects_expr(message, f)
        err = try
            f()
            nothing
        catch err
            err
        end
        @test err isa ArgumentError
        @test occursin(message, sprint(showerror, err))
    end

    rejects(servers=String[])
    rejects(servers=[""])
    rejects(randomize_servers=1)
    rejects_with("tls_cert_path and tls_key_path must be provided together"; tls_cert_path="client.pem")
    rejects_with("tls_cert_path and tls_key_path must be provided together"; tls_key_path="client-key.pem")
    rejects_with("auth must be an AbstractAuth"; auth="secret")
    rejects_expr("user is required", () -> N.UserPassAuth(nothing, "pass"))
    rejects_expr("password is required", () -> N.UserPassAuth("user", nothing))
    rejects_expr("JwtAuth requires exactly one of jwt or jwt_path", () -> N.JwtAuth(; seed))
    rejects_expr("JwtAuth requires exactly one of seed, seed_path, or signature_cb", () -> N.JwtAuth(; jwt="jwt"))
    rejects_expr("NKeyAuth requires exactly one of seed, seed_path, or signature_cb", () -> N.NKeyAuth(; nkey="UABC"))
    rejects_expr("NKeyAuth with signature_cb requires nkey", () -> N.NKeyAuth(; signature_cb=nonce -> fill(UInt8(0), 64)))
    rejects_expr("NKeyAuth must use either seed or seed_path, not both", () -> N.NKeyAuth(; seed="seed", seed_path="seed.nk"))
    rejects_expr("JwtAuth requires exactly one of jwt or jwt_path", () -> N.JwtAuth(; jwt="jwt", jwt_path="user.jwt", seed))
    rejects_expr("CredentialsAuth requires exactly one of credentials or path", () -> N.CredentialsAuth(; credentials="creds", path="user.creds"))
    rejects(name="")
    rejects(name=:client)
    rejects(verbose=1)
    rejects(pedantic=nothing)
    rejects(no_echo=1)
    rejects(tls_required=1)
    rejects(tls_first=1)
    rejects(tls_verify=1)
    rejects(tls_ca_path="")
    rejects(tls_ca_path=:ca)
    rejects(tls_cert_path="", tls_key_path="client-key.pem")
    rejects(tls_cert_path="client.pem", tls_key_path=:key)
    rejects(allow_reconnect=1)
    rejects(retry_on_initial_connect=1)
    rejects(record_stats=1)
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
    rejects(pending_size=-1)
    rejects(pending_size=true)
    rejects(read_buffer_size=0)
    rejects(read_buffer_size=true)
    rejects(write_buffer_size=-1)
    rejects(direct_write_threshold=-1)
    rejects(read_buffer_shrink_threshold=1024, read_buffer_size=2048)
    rejects(write_timeout=0)
    rejects(close_callback_timeout=-1)
    rejects(close_callback_timeout=Inf)
    rejects(max_control_line=0)
    rejects(max_inbound_payload=0)
    rejects(max_header_bytes=0)
    rejects(max_stale_pong_waiters=0)
    rejects(sub_pending_msgs_limit=0)
    rejects(sub_pending_bytes_limit=0)
    rejects(drain_timeout=0)
end

@testitem "server attempt order randomizes with opt-out" setup=[TestHelpers] begin
    using Natter
    using Random

    const N = Natter

    urls = [
        "nats://one.example:4222",
        "nats://two.example:4222",
        "nats://three.example:4222",
        "nats://four.example:4222",
    ]

    ordered_opts = N.ConnectOptions(; servers=urls, randomize_servers=false)
    ordered_client = TestHelpers.fake_client(; opts=ordered_opts)
    append!(ordered_client.servers, N.Server.(urls))
    @test [server.url for server in N._server_attempt_order!(ordered_client)] == urls

    randomized_opts = N.ConnectOptions(; servers=urls)
    randomized_client = TestHelpers.fake_client(; opts=randomized_opts)
    append!(randomized_client.servers, N.Server.(urls))
    expected = copy(urls)
    shuffle!(MersenneTwister(1), expected)
    randomized = [server.url for server in N._server_attempt_order!(randomized_client)]
    @test randomized == expected
    @test Set(randomized) == Set(urls)
end

@testitem "server URL userinfo is percent-decoded" begin
    using Natter

    const N = Natter

    @test N._server_parts("nats://u:p%40ss@example.test") ==
        ("nats", "example.test", 4222, "u", "p@ss")
    @test N._server_parts("nats://tok%40n@example.test") ==
        ("nats", "example.test", 4222, "tok@n", nothing)
    @test N._server_parts("nats://u%3Aname:p%3Aword@example.test") == (
        "nats",
        "example.test",
        4222,
        "u:name",
        "p:word",
    )
end

@testitem "connect rejects mixed URL and option authentication" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; opts=N.ConnectOptions(auth=N.TokenAuth("secret")))
    server = N.Server("nats://user:pass@example.invalid:4222")
    err = TestHelpers.thrown_exception() do
        N._connect_command(client, server, N.ServerInfo(), "user", "pass"; attempt=1, reconnect=false)
    end
    @test err isa ArgumentError
    @test occursin("URL userinfo cannot be combined with TokenAuth",
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
        (; max_msgs=true),
        (; max_msgs=big(typemax(Int)) + 1),
        (; pending_msgs_limit=0),
        (; pending_msgs_limit=-1),
        (; pending_msgs_limit=true),
        (; pending_msgs_limit=big(typemax(Int)) + 1),
        (; pending_bytes_limit=0),
        (; pending_bytes_limit=-1),
        (; pending_bytes_limit=true),
        (; pending_bytes_limit=big(typemax(Int)) + 1),
    )

    for kwargs in invalid_kwargs
        @test_throws ArgumentError subscribe(client, "events"; kwargs...)
        @test TestHelpers.capture_text(transport) == ""
        @test isempty(client.subscriptions)
        @test client.sid == 0
    end

    reconnecting = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    sub = subscribe(reconnecting, "events";
                    max_msgs=big(2), pending_msgs_limit=big(3), pending_bytes_limit=big(128))
    @test sub.max_msgs == 2
    @test sub.pending_msgs_limit == 3
    @test sub.pending_bytes_limit == 128
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
    @test_throws ArgumentError unsubscribe(negative; max_msgs=true)
    @test_throws ArgumentError unsubscribe(negative; max_msgs=big(typemax(Int)) + 1)
    @test TestHelpers.capture_text(transport) == ""
    @test !negative.closed
    @test negative.sid in keys(client.subscriptions)
    @test_throws ArgumentError subscribe(client, "bad"; max_msgs=-1)

    replay_transport = TestHelpers.WriteCapture()
    replay_client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING,
                                            read_io=replay_transport, write_io=replay_transport)
    replay_sub = subscribe(replay_client, "replay"; max_msgs=5)
    replay_sub.received = 3
    N._replay_subscriptions(replay_client; reconnect_replay=true)
    @test TestHelpers.capture_text(replay_transport) ==
          "SUB replay $(replay_sub.sid)\r\nUNSUB $(replay_sub.sid) 2\r\n"
    @test replay_sub.server_active

    exhausted = subscribe(replay_client, "done"; max_msgs=2)
    exhausted.received = 2
    TestHelpers.clear_capture!(replay_transport)
    N._replay_subscriptions(replay_client; reconnect_replay=true)
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
    @test N._msg_pending_bytes(N.next(accepted_sub; timeout=0.1)) == length(hdr)
    @test accepted_sub.pending_bytes == 0

    reported = Any[]
    opts = N.ConnectOptions(record_stats=true, error_cb=err -> push!(reported, err))
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
        err = TestHelpers.thrown_exception(() -> N.next(sub; timeout=0.1))
        @test err isa ArgumentError
        @test occursin("callback", err.msg)
    finally
        close(sub)
    end
end

@testitem "subscription snapshot tracks active sids sparsely" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    sub1 = subscribe(client, "events.1")
    sub2 = subscribe(client, "events.2")
    sub3 = subscribe(client, "events.3")

    @test N._lookup_subscription(client, sub3.sid) === sub3
    @test length(@atomic client.subscription_snapshot) == 3

    close(sub3)
    @test isnothing(N._lookup_subscription(client, sub3.sid))
    @test length(@atomic client.subscription_snapshot) == 2

    close(sub2)
    @test length(@atomic client.subscription_snapshot) == 1

    close(client)
end

@testitem "fast control snapshot only marks pre-payload control subscriptions" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    ordinary = subscribe(client, "events")
    push_control = N._subscribe(client, "push";
                                _control_handler=N._JetStreamPushControlHandler())
    request_mux = N._subscribe(client, "_INBOX.request.*";
                               _control_handler=N._RequestMuxControlHandler())
    async_publish = N._subscribe(client, "_INBOX.publish.*";
                                 _control_handler=N._JetStreamAsyncPublishControlHandler(nothing))

    @test !N._fast_control_subscription_for_sid(client, ordinary.sid)
    @test !N._fast_control_subscription_for_sid(client, push_control.sid)
    @test N._fast_control_subscription_for_sid(client, request_mux.sid)
    @test N._fast_control_subscription_for_sid(client, async_publish.sid)

    close(request_mux)
    @test !N._fast_control_subscription_for_sid(client, request_mux.sid)
    @test N._fast_control_subscription_for_sid(client, async_publish.sid)

    close(client)
end

@testitem "next timeout survives concurrent direct channel consumption" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    sub = subscribe(client, "events")
    @lock sub.lock put!(sub.messages, Msg("events", nothing, TestHelpers.bytes("stolen"); sid=sub.sid))

    lock(sub.lock)
    task = @async N.next(sub; timeout=0.05)
    try
        sleep(0.01)
        stolen = take!(sub.messages)
        @test String(stolen) == "stolen"
    finally
        unlock(sub.lock)
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

@testitem "signal flusher does not wait for write lock" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED)
    entered = Channel{Bool}(1)
    release = Channel{Bool}(1)
    holder = @async begin
        lock(client.write_lock)
        put!(entered, true)
        try
            take!(release)
        finally
            unlock(client.write_lock)
        end
    end
    take!(entered)

    signal_task = @async N._signal_flusher(client)
    try
        @test timedwait(0.2; pollint=0.001) do
            istaskdone(signal_task)
        end != :timed_out
        @test isready(client.flush_signal)
    finally
        put!(release, true)
        wait(holder)
        wait(signal_task)
    end
end

@testitem "buffered publish does not wait for client lock" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    transport = TestHelpers.WriteCapture()
    write_io = N.BufferedWriteIO(transport)
    opts = N.ConnectOptions(write_buffer_size=1024 * 1024)
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io)

    lock(client.lock)
    task = @async publish(client, "foo", "bar")
    try
        @test timedwait(0.2; pollint=0.001) do
            istaskdone(task)
        end != :timed_out
    finally
        unlock(client.lock)
    end
    fetch(task)
    @test N._buffered_bytes(write_io) > 0
    @test client.pending_bytes == 0
    @test isready(client.flush_signal)

    close(client)
end

@testitem "flusher signals are scoped to connection generation" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct FlushRaceTransport <: IO
        bytes::Vector{UInt8}
        flushes::Int
        closed::Bool
    end
    FlushRaceTransport() = FlushRaceTransport(UInt8[], 0, false)

    Base.write(t::FlushRaceTransport, data::Vector{UInt8}) = (append!(t.bytes, data); length(data))
    Base.write(t::FlushRaceTransport, data::Base.CodeUnits{UInt8}) = (append!(t.bytes, data); length(data))
    Base.write(t::FlushRaceTransport, data::AbstractString) = (append!(t.bytes, codeunits(data)); ncodeunits(data))
    Base.flush(t::FlushRaceTransport) = (t.flushes += 1; nothing)
    Base.close(t::FlushRaceTransport) = (t.closed = true; nothing)
    Base.isopen(t::FlushRaceTransport) = !t.closed

    transport = FlushRaceTransport()
    write_io = N.BufferedWriteIO(transport)
    opts = N.ConnectOptions(write_buffer_size=1024 * 1024, write_buffer_latency=0)
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io)

    old_signal = client.flush_signal
    waiting = Channel{Bool}(1)
    stale_result = Channel{Bool}(1)
    stale_waiter = @async begin
        put!(waiting, true)
        try
            take!(old_signal)
            put!(stale_result, true)
        catch err
            err isa InvalidStateException || rethrow()
            put!(stale_result, false)
        end
    end
    take!(waiting)

    @lock client.lock N._bump_generation_locked!(client)
    @test client.flush_signal !== old_signal
    @test !isopen(old_signal)
    @test timedwait(1.0; pollint=0.001) do
        isready(stale_result)
    end != :timed_out
    @test take!(stale_result) === false
    wait(stale_waiter)

    N._start_flusher_task!(client, client.generation)
    publish(client, "foo", "bar")
    @test N._buffered_bytes(write_io) > 0

    result = timedwait(1.0; pollint=0.001) do
        occursin("PUB foo 3\r\nbar\r\n", String(copy(transport.bytes))) &&
            N._buffered_bytes(write_io) == 0
    end
    @test result != :timed_out
    @test transport.flushes == 1

    close(client)
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
    @test client.pending_bytes == 0
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
    publish(client, "foo", payload; buffer_on_reconnect=true)

    expected = "PUB foo 64\r\n$payload\r\n"
    @test TestHelpers.capture_text(transport) == expected
    @test N._buffered_bytes(write_io) == 0
    @test client.pending_bytes == 0
    @test isempty(N._take_replayable_writes!(write_io))

    close(client)
end

@testitem "raw writes recheck connection status after write lock" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    function queued_raw_result(new_status)
        transport = TestHelpers.WriteCapture()
        client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED,
                                         read_io=transport, write_io=transport)
        lock(client.write_lock)
        task = @async try
            N._write_raw(client, TestHelpers.bytes("PING\r\n"))
            nothing
        catch err
            err
        end
        try
            for _ in 1:10
                yield()
            end
            @lock client.lock N._store_status_locked!(client, new_status)
        finally
            unlock(client.write_lock)
        end
        fetch(task), TestHelpers.capture_text(transport)
    end

    err, written = queued_raw_result(N.ConnectionStatus.RECONNECTING)
    @test err isa ConnectionReconnectingError
    @test written == ""

    err, written = queued_raw_result(N.ConnectionStatus.DRAINING)
    @test err isa ConnectionDrainingError
    @test written == ""

    err, written = queued_raw_result(N.ConnectionStatus.CLOSED)
    @test err isa ConnectionClosedError
    @test written == ""

    drain_transport = TestHelpers.WriteCapture()
    drain_client = TestHelpers.fake_client(; status=N.ConnectionStatus.DRAINING,
                                           read_io=drain_transport, write_io=drain_transport)
    N._write_raw(drain_client, TestHelpers.bytes("PONG\r\n"); write_mode=N._RAW_WRITE_DRAIN)
    @test TestHelpers.capture_text(drain_transport) == "PONG\r\n"

    replay_transport = TestHelpers.WriteCapture()
    replay_client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING,
                                            read_io=replay_transport, write_io=replay_transport)
    N._write_raw(replay_client, TestHelpers.bytes("SUB foo 1\r\n");
                 write_mode=N._RAW_WRITE_RECONNECT_REPLAY)
    @test TestHelpers.capture_text(replay_transport) == "SUB foo 1\r\n"
end

@testitem "raw transport writes time out and close active transport" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct RawTimeoutTransport <: IO
        entered::Channel{Bool}
        release::Channel{Bool}
        closes::Base.RefValue{Int}
        closed::Base.RefValue{Bool}
    end
    RawTimeoutTransport() = RawTimeoutTransport(Channel{Bool}(1), Channel{Bool}(1), Ref(0), Ref(false))

    function Base.write(t::RawTimeoutTransport, data::Vector{UInt8})
        put!(t.entered, true)
        take!(t.release)
        t.closed[] && throw(ErrorException("transport closed"))
        length(data)
    end
    Base.flush(::RawTimeoutTransport) = nothing
    function Base.close(t::RawTimeoutTransport)
        t.closes[] += 1
        t.closed[] = true
        isready(t.release) || put!(t.release, true)
        nothing
    end

    transport = RawTimeoutTransport()
    client = TestHelpers.fake_client(; opts=N.ConnectOptions(write_buffer_size=0, write_timeout=0.02),
                                     status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=transport)

    task = @async try
        N._write_raw(client, TestHelpers.bytes("PING\r\n"))
        nothing
    catch err
        err
    end
    @test timedwait(1.0; pollint=0.001) do
        isready(transport.entered) || istaskdone(task)
    end != :timed_out
    entered_ready = isready(transport.entered)
    @test entered_ready
    entered_ready && (@test take!(transport.entered))
    result = timedwait(1.0; pollint=0.001) do
        istaskdone(task)
    end
    result == :timed_out && close(transport)

    @test result != :timed_out
    @test fetch(task) isa N.TimeoutError
    @test transport.closes[] >= 1

    close(client)
end

@testitem "large direct publish writes time out under write deadline" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct PublishTimeoutTransport <: IO
        entered::Channel{Bool}
        release::Channel{Bool}
        closes::Base.RefValue{Int}
        closed::Base.RefValue{Bool}
    end
    PublishTimeoutTransport() = PublishTimeoutTransport(Channel{Bool}(1), Channel{Bool}(1), Ref(0), Ref(false))

    function Base.write(t::PublishTimeoutTransport, data::Vector{UInt8})
        put!(t.entered, true)
        take!(t.release)
        t.closed[] && throw(ErrorException("transport closed"))
        length(data)
    end
    Base.write(t::PublishTimeoutTransport, data::Base.CodeUnits{UInt8}) = (length(data))
    Base.write(t::PublishTimeoutTransport, data::AbstractString) = ncodeunits(data)
    Base.flush(::PublishTimeoutTransport) = nothing
    function Base.close(t::PublishTimeoutTransport)
        t.closes[] += 1
        t.closed[] = true
        isready(t.release) || put!(t.release, true)
        nothing
    end

    transport = PublishTimeoutTransport()
    opts = N.ConnectOptions(write_buffer_size=16, write_timeout=0.02, allow_reconnect=false,
                            error_cb=err -> nothing)
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=N.BufferedWriteIO(transport))

    task = @async try
        publish(client, "foo", repeat("x", 64))
        nothing
    catch err
        err
    end
    @test timedwait(1.0; pollint=0.001) do
        isready(transport.entered) || istaskdone(task)
    end != :timed_out
    entered_ready = isready(transport.entered)
    @test entered_ready
    entered_ready && (@test take!(transport.entered))
    result = timedwait(1.0; pollint=0.001) do
        istaskdone(task)
    end
    result == :timed_out && close(transport)

    @test result != :timed_out
    @test fetch(task) isa N.TimeoutError
    @test transport.closes[] >= 1

    close_task = @async close(client)
    @test timedwait(1.0; pollint=0.001) do
        istaskdone(close_task)
    end != :timed_out
    wait(close_task)
end

@testitem "disabled and memory write timeouts skip watchdog state" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    disabled_transport = TestHelpers.WriteCapture()
    disabled = TestHelpers.fake_client(; opts=N.ConnectOptions(write_buffer_size=0, write_timeout=Inf),
                                       status=N.ConnectionStatus.CONNECTED,
                                       write_io=disabled_transport)
    N._write_raw(disabled, TestHelpers.bytes("PING\r\n"))

    @test TestHelpers.capture_text(disabled_transport) == "PING\r\n"
    @test disabled.write_epoch[] == 0
    @test disabled.write_deadline[] == Inf
    @test isnothing(disabled.write_timeout_task)
    close(disabled)

    memory_io = IOBuffer()
    memory = TestHelpers.fake_client(; opts=N.ConnectOptions(write_buffer_size=0, write_timeout=0.02),
                                     status=N.ConnectionStatus.CONNECTED,
                                     write_io=memory_io)
    N._write_raw(memory, TestHelpers.bytes("PONG\r\n"))

    @test String(take!(memory_io)) == "PONG\r\n"
    @test memory.write_epoch[] == 0
    @test memory.write_deadline[] == Inf
    @test isnothing(memory.write_timeout_task)
    close(memory)
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

    @test transport.chunks == ["PUB foo 3\r\nbar\r\n"]
    close(client)

    buffered_transport = ChunkCapture()
    buffered_write = N.BufferedWriteIO(buffered_transport)
    buffered_client = TestHelpers.fake_client(; opts=N.ConnectOptions(write_buffer_size=1024),
                                             status=N.ConnectionStatus.CONNECTED,
                                             read_io=buffered_transport,
                                             write_io=buffered_write)

    publish(buffered_client, "foo", "bar"; direct_write=true, buffer_on_reconnect=false)

    @test buffered_transport.chunks == ["PUB foo 3\r\nbar\r\n"]
    @test N._buffered_bytes(buffered_write) == 0
    close(buffered_client)

    large_transport = ChunkCapture()
    large_client = TestHelpers.fake_client(; opts=N.ConnectOptions(write_buffer_size=0,
                                                                   direct_write_threshold=8),
                                           status=N.ConnectionStatus.CONNECTED,
                                           read_io=large_transport, write_io=large_transport)
    publish(large_client, "foo", "0123456789abcdef")
    @test large_transport.chunks == ["PUB foo 16\r\n", "0123456789abcdef", "\r\n"]
    close(large_client)
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

@testitem "cancellation tokens cancel blocking core waits" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    source = CancellationSource()
    token = cancellation_token(source)
    @test !iscancelled(token)
    @test cancel!(source)
    @test iscancelled(token)
    @test !cancel!(source)

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    sub = subscribe(client, "updates")
    already_cancelled = TestHelpers.thrown_exception(() -> N.next(sub; timeout=1.0, cancel_token=token))
    @test already_cancelled isa CancelledError

    source = CancellationSource()
    token = cancellation_token(source)
    next_task = @async TestHelpers.thrown_exception(() -> N.next(sub; timeout=30.0, cancel_token=token))
    sleep(0.02)
    @test cancel!(source)
    @test fetch(next_task) isa CancelledError

    async_source = CancellationSource()
    async_token = cancellation_token(async_source)
    handle = next_async(sub; timeout=30.0, cancel_token=async_token)
    sleep(0.02)
    cancel!(async_source)
    async_err = TestHelpers.thrown_exception(() -> fetch(handle))
    @test async_err isa CancelledError
end

@testitem "cancelled flush leaves a stale waiter tombstone" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    struct CancellableFlushSink <: IO end
    Base.write(::CancellableFlushSink, data::Vector{UInt8}) = length(data)
    Base.write(::CancellableFlushSink, data::String) = ncodeunits(data)
    Base.flush(::CancellableFlushSink) = nothing
    Base.close(::CancellableFlushSink) = nothing

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED,
                                     write_io=CancellableFlushSink())
    source = CancellationSource()
    token = cancellation_token(source)
    task = @async TestHelpers.thrown_exception(() -> flush(client; timeout=30.0, cancel_token=token))

    @test timedwait(1.0; pollint=0.005) do
        length(client.pongs) == 1
    end == :ok
    cancel!(source)

    @test fetch(task) isa CancelledError
    @test length(client.pongs) == 1
    waiter = only(client.pongs)
    @test !waiter.active
    @test !waiter.ready
end

@testitem "cancelled request removes mux waiter" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct RequestCancelTransport <: IO
        bytes::Vector{UInt8}
    end
    RequestCancelTransport() = RequestCancelTransport(UInt8[])
    Base.write(t::RequestCancelTransport, data::Vector{UInt8}) = (append!(t.bytes, data); length(data))
    Base.write(t::RequestCancelTransport, data::Base.CodeUnits{UInt8}) = (append!(t.bytes, data); length(data))
    Base.write(t::RequestCancelTransport, data::Union{String,SubString{String}}) = (append!(t.bytes, codeunits(data)); ncodeunits(data))
    Base.write(t::RequestCancelTransport, data::AbstractString) = (append!(t.bytes, codeunits(data)); ncodeunits(data))
    Base.flush(::RequestCancelTransport) = nothing
    Base.close(::RequestCancelTransport) = nothing

    transport = RequestCancelTransport()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=transport)
    source = CancellationSource()
    token = cancellation_token(source)
    task = @async TestHelpers.thrown_exception(() -> request(client, "svc", "body";
                                                             timeout=30.0, cancel_token=token))

    @test timedwait(1.0; pollint=0.005) do
        mux = @atomic client.request_mux
        !isnothing(mux) && (@lock mux.condition !isempty(mux.waiters))
    end == :ok
    cancel!(source)

    @test fetch(task) isa CancelledError
    mux = @atomic client.request_mux
    @test !isnothing(mux)
    @test @lock mux.condition isempty(mux.waiters)
end

@testitem "connected replayable publishes are bounded by pending_size" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    opts = N.ConnectOptions(pending_size=64, write_buffer_size=1024 * 1024,
                            record_stats=true)
    transport = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=N.BufferedWriteIO(transport))

    payload = repeat("x", 50)
    frame_size = ncodeunits("PUB foo 50\r\n$payload\r\n")
    @test frame_size == opts.pending_size

    publish(client, "foo", payload; buffer_on_reconnect=true)
    @test client.pending_bytes == frame_size

    try
        publish(client, "foo", payload; buffer_on_reconnect=true)
        @test false
    catch err
        @test err isa OutboundBufferLimitError
        @test err.limit == opts.pending_size
        @test err.actual == 2 * frame_size
    end

    @test N.status(client) == N.ConnectionStatus.CONNECTED
    @test client.pending_bytes == frame_size
    @test isempty(transport.bytes)
    @test stats(client).out_msgs == 1

    close(client)
end

@testitem "pending_size zero disables reconnect publish replay" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    opts = N.ConnectOptions(pending_size=0, write_buffer_size=1024 * 1024)
    transport = TestHelpers.WriteCapture()
    write_io = N.BufferedWriteIO(transport)
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io)

    publish(client, "foo", "bar"; buffer_on_reconnect=true)

    expected = "PUB foo 3\r\nbar\r\n"
    @test N._buffered_bytes(write_io) == ncodeunits(expected)
    @test isempty(write_io.replayable_entries)
    @test client.pending_bytes == 0

    N._take_transport!(client; preserve_replayable=true)

    @test N._buffered_bytes(write_io) == 0
    @test client.pending_bytes == 0
    @test isempty(take!(client.pending))

    close(transport)
end

@testitem "flusher failure discards ambiguous buffered publishes" setup=[TestHelpers] begin
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
    publish(client, "foo", "bar"; buffer_on_reconnect=true)
    expected = "PUB foo 3\r\nbar\r\n"
    @test client.pending_bytes == ncodeunits(expected)

    @test_throws ErrorException N._flush_buffered_writes(client)
    N._trigger_reconnect(client, ErrorException("flush failed"))

    @test client.pending_bytes == 0
    @test isempty(take!(client.pending))
    @test isnothing(client.write_io)
    @test transport.closed
    !isnothing(client.reconnect_task) && wait(client.reconnect_task)
end

@testitem "foreground direct publish write failure is not replayed" setup=[TestHelpers] begin
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
                            record_stats=true,
                            error_cb=err -> nothing)
    transport = FailingWriteTransport()
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=N.BufferedWriteIO(transport))

    err = TestHelpers.thrown_exception(() -> publish(client, "foo", "bar"))

    @test err isa ConnectionReconnectingError
    @test client.pending_bytes == 0
    @test isempty(take!(client.pending))
    @test stats(client).out_msgs == 0
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
    @test_throws ErrorException N._flush_pending_buffer(client; reconnect_replay=true)
    @test client.pending_bytes == length(data)
end

@testitem "pending publish replay revalidates server capabilities" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    function set_info!(client, info)
        @lock client.lock begin
            client.info = info
            N._sync_server_info_cache_locked!(client)
        end
    end

    header_transport = TestHelpers.WriteCapture()
    header_client = TestHelpers.fake_client(;
        status=N.ConnectionStatus.RECONNECTING,
        info=N.ServerInfo(; headers=true, max_payload=1024),
        write_io=header_transport,
    )
    publish(header_client, "foo", "bar"; headers=Headers("Trace" => "abc"),
            buffer_on_reconnect=true)
    header_pending_bytes = header_client.pending_bytes

    set_info!(header_client, N.ServerInfo(; headers=false, max_payload=1024))
    header_err = TestHelpers.thrown_exception() do
        N._flush_pending_buffer(header_client; reconnect_replay=true)
    end
    @test header_err isa UnsupportedFeatureError
    @test TestHelpers.capture_text(header_transport) == ""
    @test header_client.pending_bytes == header_pending_bytes
    restored_header = String(take!(header_client.pending))
    @test startswith(restored_header, "HPUB foo ")
    @test occursin("Trace: abc", restored_header)

    payload = repeat("x", 8)
    opts = N.ConnectOptions(write_buffer_size=1024 * 1024)
    original_transport = TestHelpers.WriteCapture()
    write_io = N.BufferedWriteIO(original_transport)
    payload_client = TestHelpers.fake_client(;
        opts,
        status=N.ConnectionStatus.CONNECTED,
        info=N.ServerInfo(; headers=true, max_payload=1024),
        read_io=original_transport,
        write_io,
    )
    publish(payload_client, "foo", payload; buffer_on_reconnect=true)
    payload_pending_bytes = payload_client.pending_bytes
    @test N._buffered_bytes(write_io) > 0

    N._take_transport!(payload_client; preserve_replayable=true)
    replay_transport = TestHelpers.WriteCapture()
    @lock payload_client.lock begin
        N._store_status_locked!(payload_client, N.ConnectionStatus.RECONNECTING)
        @atomic payload_client.write_io = N.BufferedWriteIO(replay_transport)
        payload_client.info = N.ServerInfo(; headers=true, max_payload=4)
        N._sync_server_info_cache_locked!(payload_client)
    end

    payload_err = TestHelpers.thrown_exception() do
        N._flush_pending_buffer(payload_client; reconnect_replay=true)
    end
    @test payload_err isa MaxPayloadError
    @test payload_err.limit == 4
    @test payload_err.actual == ncodeunits(payload)
    @test TestHelpers.capture_text(replay_transport) == ""
    @test payload_client.pending_bytes == payload_pending_bytes
    @test String(take!(payload_client.pending)) == "PUB foo 8\r\n$payload\r\n"
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

    N._flush_pending_buffer(client; reconnect_replay=true)

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
        N._flush_pending_buffer(client; reconnect_replay=true)
        nothing
    catch err
        err
    end

    @test take!(transport.entered)
    close_task = @async close(client)
    @test timedwait(1.0; pollint=0.001) do
        N.status(client) == N.ConnectionStatus.CLOSED
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
    @test N.status(terminal) == N.ConnectionStatus.DISCONNECTED
    @test terminal.pending_bytes == 0
    @test isempty(take!(terminal.pending))

    closing = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    N._enqueue_pending(closing, data)
    @test closing.pending_bytes == length(data)
    close(closing)
    @test N.status(closing) == N.ConnectionStatus.CLOSED
    @test closing.pending_bytes == 0
    @test isempty(take!(closing.pending))
end

@testitem "reconnect with no servers terminates" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED)
    N._trigger_reconnect(client, ErrorException("lost"))

    @test timedwait(1.0; pollint=0.01) do
        N.status(client) == N.ConnectionStatus.DISCONNECTED
    end != :timed_out
    @test isnothing(client.reconnect_task) || istaskdone(client.reconnect_task)
end

@testitem "foreground publish write failure starts reconnect without replaying ambiguous data" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    struct BadWriteTransport <: IO end
    Base.write(::BadWriteTransport, ::Vector{UInt8}) = throw(ErrorException("write failed"))
    Base.flush(::BadWriteTransport) = nothing
    Base.close(::BadWriteTransport) = nothing

    transport = BadWriteTransport()
    client = TestHelpers.fake_client(; opts=N.ConnectOptions(record_stats=true),
                                     status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=transport)

    @test_throws ConnectionReconnectingError publish(client, "foo", "bar")
    @test N.status(client) == N.ConnectionStatus.RECONNECTING
    @test client.pending_bytes == 0
    @test isempty(take!(client.pending))
    @test stats(client).out_msgs == 0
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
    @test N.status(error_client) == N.ConnectionStatus.CLOSED
    @test error_client.pending_bytes == 0
    @test isempty(take!(error_client.pending))
    @test stats(error_client).out_msgs == 0

    disconnected_ref = Ref{Any}()
    disconnected_opts = N.ConnectOptions(error_cb=err -> nothing,
                                         event_cb=event -> begin
                                             if event.kind == N.ConnectionEventKind.DISCONNECTED
                                                 close(disconnected_ref[])
                                             end
                                         end)
    disconnected_transport = CallbackCloseTransport()
    disconnected_client = TestHelpers.fake_client(; opts=disconnected_opts, status=N.ConnectionStatus.CONNECTED,
                                                  read_io=disconnected_transport, write_io=disconnected_transport)
    disconnected_ref[] = disconnected_client

    @test_throws ErrorException publish(disconnected_client, "foo", "bar")
    @test N.status(disconnected_client) == N.ConnectionStatus.CLOSED
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
    @test N.status(raw_client) == N.ConnectionStatus.CLOSED
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
    @test isnothing(client.request_mux)
    @test isempty(take!(client.pending))
    @test client.pending_bytes == 0

    disabled = TestHelpers.fake_client(; opts=N.ConnectOptions(pending_size=0),
                                       status=N.ConnectionStatus.RECONNECTING)
    @test_throws ConnectionReconnectingError request(disabled, "foo", "bar"; timeout=0.001)
    @test isempty(disabled.subscriptions)
    @test disabled.sid == 0
    @test disabled.pending_bytes == 0
end

@testitem "request accepts pair-style header input" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    transport = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; opts=N.ConnectOptions(record_stats=true),
                                     status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=transport)

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
    client = TestHelpers.fake_client(; opts=N.ConnectOptions(record_stats=true),
                                     status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=transport)

    @test_throws ConnectionReconnectingError request(client, "foo", "bar"; timeout=0.001)
    @test N.status(client) == N.ConnectionStatus.RECONNECTING
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

@testitem "request mux reply tokens use numeric suffixes after rng prefix" setup=[TestHelpers] begin
    using Natter
    using Random

    const N = Natter

    expected_rng = MersenneTwister(1)
    expected_prefix = "_INBOX.$(randstring(expected_rng, N.NUID_ALPHABET, 22))"

    transport = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, read_io=transport, write_io=transport)
    try
        @test_throws TimeoutError request(client, "svc", "body"; timeout=0.001)

        lines = split(TestHelpers.capture_text(transport), "\r\n"; keepempty=false)
        publish_line = only(line for line in lines if startswith(line, "PUB svc "))
        @test split(publish_line)[3] == "$expected_prefix.1"
        @test N._request_mux_token(expected_prefix, "$expected_prefix.1") == 1
        @test isnothing(N._request_mux_token(expected_prefix, "$expected_prefix.01"))

        reply_bytes = TestHelpers.bytes("$expected_prefix.1")
        alias_bytes = TestHelpers.bytes("$expected_prefix.01")
        @test N._request_mux_token(expected_prefix, reply_bytes, firstindex(reply_bytes), lastindex(reply_bytes)) == 1
        @test isnothing(N._request_mux_token(expected_prefix, alias_bytes, firstindex(alias_bytes), lastindex(alias_bytes)))
    finally
        close(client)
    end
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
    client = TestHelpers.fake_client(; opts=N.ConnectOptions(record_stats=true),
                                     status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=transport)

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

@testitem "request mux reconnect keeps in-flight waiters and rejects queued requests" setup=[TestHelpers] begin
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
    opts = N.ConnectOptions(; connect_timeout=0.05, reconnect_wait=1.0,
                            reconnect_max_wait=1.0, reconnect_jitter=0.0)
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=transport)
    push!(client.servers, N.Server("nats://127.0.0.1:1"))

    task = @async request(client, "svc", "pending"; timeout=1.0)
    result = timedwait(1.0; pollint=0.001) do
        length(request_publishes(transport)) == 1
    end
    @test result != :timed_out
    reply = only(request_publishes(transport))[1]
    mux = client.request_mux
    @test length(mux.waiters) == 1

    N._trigger_reconnect(client, ErrorException("lost"))
    @test N.status(client) == N.ConnectionStatus.RECONNECTING
    @test length(mux.waiters) == 1
    @test client.pending_bytes == 0
    @test !istaskdone(task)

    N._dispatch_msg(client, Msg(reply, nothing, TestHelpers.bytes("after-reconnect"); sid=mux.sub.sid))
    @test String(fetch(task)) == "after-reconnect"
    @test isempty(mux.waiters)
    close(client)

    replay_client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    @test_throws ConnectionReconnectingError request(replay_client, "svc", "queued"; timeout=1.0)
    @test replay_client.pending_bytes == 0
    @test isnothing(replay_client.request_mux)
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
    @test N.status(client) == N.ConnectionStatus.RECONNECTING
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
    @test N.status(client) == N.ConnectionStatus.RECONNECTING
    @test closes[] == 0
end

@testitem "reconnect intent blocks foreground writes before transport swap" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    transport = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=transport)
    client.write_reconnect_pending[] = true

    publish(client, "foo", "bar"; buffer_on_reconnect=true)
    expected = "PUB foo 3\r\nbar\r\n"
    @test TestHelpers.capture_text(transport) == ""
    @test client.pending_bytes == ncodeunits(expected)
    @test String(take!(client.pending)) == expected

    client.write_reconnect_pending[] = true
    @test_throws ConnectionReconnectingError publish(client, "foo", "bar";
                                                     buffer_on_reconnect=false)
    @test TestHelpers.capture_text(transport) == ""
end

@testitem "reconnect exhaustion closes subscriptions and wakes consumers" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    reported = Any[]
    client = TestHelpers.fake_client(; opts=N.ConnectOptions(max_reconnect_attempts=0, error_cb=err -> push!(reported, err)),
                                     status=N.ConnectionStatus.RECONNECTING)
    sub = subscribe(client, "foo")
    next_task = @async N.next(sub; timeout=30.0)
    callback_sub = subscribe(client, "bar") do _
        nothing
    end
    processor = callback_sub.processor
    @test !isnothing(processor)

    sleep(0.01)
    N._reconnect_loop(client, client.generation)

    @test N.status(client) == N.ConnectionStatus.DISCONNECTED
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
    next_task = @async N.next(sub; timeout=30.0)

    sleep(0.01)
    reason = ErrorException("lost")
    N._trigger_reconnect(client, reason)

    @test N.status(client) == N.ConnectionStatus.DISCONNECTED
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
    @test N.status(client) == N.ConnectionStatus.CONNECTED
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
        @test N.status(client) == N.ConnectionStatus.DISCONNECTED
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

@testitem "initial connect retry is opt-in and bounded" setup=[TestHelpers] begin
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
                close(sock)
            end
        finally
            isopen(listener) && close(listener)
        end
    end

    reported = Any[]
    events = N.ConnectionEvent[]
    try
        err = TestHelpers.thrown_exception() do
            N.connect("nats://127.0.0.1:$port";
                      retry_on_initial_connect=true,
                      connect_timeout=0.5,
                      reconnect_wait=0.01,
                      reconnect_jitter=0.0,
                      max_reconnect_attempts=1,
                      error_cb=err -> push!(reported, err),
                      event_cb=event -> push!(events, event))
        end

        @test !(err isa N.CancelledError)
        @test attempts[] == 2
        @test length(reported) == 2
        delay_events = filter(event -> event.kind == N.ConnectionEventKind.RECONNECT_DELAY, events)
        @test length(delay_events) == 1
        @test only(delay_events).status == N.ConnectionStatus.CONNECTING
        @test only(delay_events).attempt == 1
    finally
        isopen(listener) && close(listener)
        timedwait(1.0; pollint=0.01) do
            istaskdone(server_task)
        end == :timed_out || wait(server_task)
    end
end

@testitem "initial connect retry succeeds when server appears" begin
    using Natter
    using Sockets

    const N = Natter

    listener = listen(ip"127.0.0.1", 0)
    port = Int(getsockname(listener)[2])
    attempts = Ref(0)
    reported = Ref(0)
    release = Channel{Bool}(1)
    handshake = Channel{Tuple{String,String}}(1)
    server_task = @async begin
        try
            while true
                sock = accept(listener)
                attempts[] += 1
                if attempts[] == 1
                    close(sock)
                    continue
                end
                try
                    write(sock, "INFO {\"headers\":true,\"proto\":1,\"max_payload\":1048576}\r\n")
                    flush(sock)
                    connect_line = rstrip(readline(sock), '\r')
                    ping_line = rstrip(readline(sock), '\r')
                    put!(handshake, (connect_line, ping_line))
                    write(sock, "PONG\r\n")
                    flush(sock)
                    take!(release)
                finally
                    close(sock)
                end
                break
            end
        finally
            isopen(listener) && close(listener)
        end
    end

    client_ref = Ref{Any}(nothing)
    try
        client_ref[] = N.connect("nats://127.0.0.1:$port";
                                 retry_on_initial_connect=true,
                                 connect_timeout=0.5,
                                 reconnect_wait=0.01,
                                 reconnect_jitter=0.0,
                                 max_reconnect_attempts=5,
                                 error_cb=_ -> (reported[] += 1; nothing))
        client = client_ref[]
        @test N.status(client) == N.ConnectionStatus.CONNECTED
        @test attempts[] == 2
        @test reported[] >= 1
        connect_line, ping_line = take!(handshake)
        @test startswith(connect_line, "CONNECT ")
        @test ping_line == "PING"
    finally
        client = client_ref[]
        isnothing(client) || close(client)
        isready(release) || put!(release, true)
        isopen(listener) && close(listener)
        timedwait(1.0; pollint=0.01) do
            istaskdone(server_task)
        end == :timed_out || wait(server_task)
    end
end

@testitem "connect uses configured read buffer size" begin
    using Natter
    using Sockets

    const N = Natter

    listener = listen(ip"127.0.0.1", 0)
    port = Int(getsockname(listener)[2])
    release = Channel{Bool}(1)
    server_task = @async begin
        try
            sock = accept(listener)
            try
                write(sock, "INFO {\"headers\":true,\"proto\":1,\"max_payload\":1048576}\r\n")
                flush(sock)
                readline(sock)
                readline(sock)
                write(sock, "PONG\r\n")
                flush(sock)
                take!(release)
            finally
                close(sock)
            end
        finally
            isopen(listener) && close(listener)
        end
    end

    client_ref = Ref{Any}(nothing)
    try
        client_ref[] = N.connect("nats://127.0.0.1:$port";
                                 connect_timeout=0.5,
                                 read_buffer_size=8192)
        client = client_ref[]
        @test N.status(client) == N.ConnectionStatus.CONNECTED
        @test length(client.reader.scratch) == 8192
    finally
        client = client_ref[]
        isnothing(client) || close(client)
        isready(release) || put!(release, true)
        isopen(listener) && close(listener)
        timedwait(1.0; pollint=0.01) do
            istaskdone(server_task)
        end == :timed_out || wait(server_task)
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

@testitem "connect timeout covers DNS resolution" setup=[TestHelpers] begin
    using Natter
    using Sockets

    const N = Natter

    started = Channel{Bool}(1)
    release = Channel{Bool}(1)
    resolver = _host -> begin
        put!(started, true)
        take!(release)
        ip"127.0.0.1"
    end

    start = time()
    err = TestHelpers.thrown_exception() do
        N._resolve_connect_address("blocked.invalid", 4222, 0.05; resolver)
    end
    elapsed = time() - start

    @test err isa TimeoutError
    @test elapsed < 1.0
    @test isready(started)
    isready(release) || put!(release, true)
end

@testitem "connect timeout bounds auth callbacks" setup=[TestHelpers] begin
    using Natter
    using Sockets

    const N = Natter

    listener = listen(ip"127.0.0.1", 0)
    port = Int(getsockname(listener)[2])
    accepted = Channel{Sockets.TCPSocket}(1)
    server_task = @async begin
        sock = try
            accept(listener)
        catch err
            (err isa InvalidStateException || err isa InterruptException || err isa Base.IOError) || rethrow()
            return nothing
        end
        put!(accepted, sock)
        try
            write(sock, "INFO {\"headers\":true,\"nonce\":\"nonce\"}\r\n")
            flush(sock)
            try
                readline(sock)
            catch err
                (err isa EOFError || err isa Base.IOError || err isa InvalidStateException) || rethrow()
            end
        catch err
            (err isa InvalidStateException || err isa InterruptException || err isa Base.IOError) || rethrow()
        finally
            close(sock)
        end
    end

    started = Channel{Bool}(1)
    blocker = Channel{Bool}(0)
    try
        err = TestHelpers.thrown_exception() do
            N.connect("nats://127.0.0.1:$port";
                      connect_timeout=1.0,
                      auth=N.CallbackAuth(_ -> begin
                          put!(started, true)
                          take!(blocker)
                          N.NoAuth()
                      end))
        end
        @test err isa TimeoutError
        @test isready(started)
    finally
        close(listener)
        isopen(blocker) && close(blocker)
        if isready(accepted)
            sock = take!(accepted)
            isopen(sock) && close(sock)
        end
        done = timedwait(1.0; pollint=0.01) do
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

@testitem "interruptible IO timeout runs operation in caller task" begin
    using Natter

    const N = Natter

    caller = current_task()
    ran_inline = Ref(false)
    cleanup_called = Ref(false)

    result = N._run_interruptible_io_with_timeout("inline operation", 1.0, () -> (cleanup_called[] = true)) do
        ran_inline[] = current_task() === caller
        :ok
    end

    @test result == :ok
    @test ran_inline[]
    @test !cleanup_called[]
end

@testitem "timed operation timeout interrupts worker task" begin
    using Natter

    const N = Natter

    worker_done = Channel{Bool}(1)
    cleanup_done = Channel{Bool}(1)
    blocker = Channel{Bool}(0)
    reported = Channel{Any}(1)
    cleanup = () -> begin
        put!(cleanup_done, true)
        nothing
    end
    reporter = errors -> foreach(err -> put!(reported, err), errors)

    @test_throws TimeoutError N._run_with_timeout("blocked operation", 0.01, cleanup, reporter) do
        try
            take!(blocker)
        finally
            put!(worker_done, true)
        end
    end
    @test timedwait(0.5; pollint=0.01) do
        isready(cleanup_done)
    end != :timed_out
    @test timedwait(0.5; pollint=0.01) do
        isready(worker_done)
    end != :timed_out
    @test !isready(reported)
end

@testitem "_wait_task! interrupts blocked task after grace timeout" begin
    using Natter

    const N = Natter

    started = Channel{Bool}(1)
    stopped = Channel{Bool}(1)
    blocker = Channel{Bool}(0)
    task = @async begin
        put!(started, true)
        try
            take!(blocker)
        finally
            put!(stopped, true)
        end
    end
    take!(started)

    errors = Any[]
    N._wait_task!(errors, "stop blocked test task", task; timeout=0.2, interrupt=true)

    @test timedwait(0.5; pollint=0.01) do
        isready(stopped)
    end != :timed_out
    @test istaskdone(task)
    @test isempty(errors)
end

@testitem "_wait_task! leaves blocked task running when interruption is disabled" begin
    using Natter

    const N = Natter

    started = Channel{Bool}(1)
    stopped = Channel{Bool}(1)
    blocker = Channel{Bool}(1)
    task = @async begin
        put!(started, true)
        try
            take!(blocker)
        finally
            put!(stopped, true)
        end
    end
    take!(started)

    errors = Any[]
    N._wait_task!(errors, "wait blocked test task", task; timeout=0.05)

    @test !istaskdone(task)
    @test !isready(stopped)
    @test length(errors) == 1
    @test errors[1] isa CleanupError
    @test errors[1].cause isa TimeoutError

    put!(blocker, true)
    wait(task)
    @test isready(stopped)
end

@testitem "client close does not interrupt subscription callbacks" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    reported = Any[]
    opts = N.ConnectOptions(close_callback_timeout=0.05, error_cb=err -> push!(reported, err))
    transport = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED,
                                     read_io=transport, write_io=transport)
    entered = Channel{Bool}(1)
    release = Channel{Bool}(1)
    stopped = Channel{Bool}(1)
    interrupted = Ref(false)

    sub = subscribe(client, "events") do _
        put!(entered, true)
        try
            take!(release)
        catch err
            interrupted[] = err isa InterruptException
            rethrow()
        finally
            put!(stopped, true)
        end
    end
    N._dispatch_msg(client, Msg("events", nothing, UInt8[]; sid=sub.sid))
    take!(entered)

    close(client)

    @test !interrupted[]
    @test !istaskdone(sub.processor)
    @test length(reported) == 1
    @test reported[1] isa CleanupError
    @test reported[1].cause isa TimeoutError

    put!(release, true)
    @test timedwait(1.0; pollint=0.01) do
        istaskdone(sub.processor)
    end != :timed_out
    @test isready(stopped)
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

@testitem "flush reports disconnected clients as closed" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    disconnected = TestHelpers.fake_client(; status=N.ConnectionStatus.DISCONNECTED)
    err = TestHelpers.thrown_exception(() -> flush(disconnected; timeout=1.0))
    @test err isa ConnectionClosedError
    @test sprint(showerror, err) == "Natter.ConnectionClosedError: connection is disconnected"
    @test isempty(disconnected.pongs)

    mutable struct DisconnectingFlushTransport <: IO
        client::Base.RefValue{Any}
    end
    Base.write(::DisconnectingFlushTransport, data::Vector{UInt8}) = length(data)
    Base.write(::DisconnectingFlushTransport, data::String) = ncodeunits(data)
    Base.flush(t::DisconnectingFlushTransport) = begin
        client = t.client[]
        @lock client.lock N._store_status_locked!(client, N.ConnectionStatus.DISCONNECTED)
        N._notify_pong_waiters!(client, false)
        nothing
    end
    Base.close(::DisconnectingFlushTransport) = nothing

    client_ref = Ref{Any}(nothing)
    transport = DisconnectingFlushTransport(client_ref)
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=transport)
    client_ref[] = client

    err = TestHelpers.thrown_exception(() -> flush(client; timeout=1.0))
    @test err isa ConnectionClosedError
    @test N.status(client) == N.ConnectionStatus.DISCONNECTED
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

@testitem "keepalive pong does not satisfy later flush" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct KeepaliveFlushTransport <: IO
        writes::Vector{String}
        flushed::Channel{Bool}
    end
    Base.write(t::KeepaliveFlushTransport, data::Vector{UInt8}) = (push!(t.writes, String(copy(data))); length(data))
    Base.write(t::KeepaliveFlushTransport, data::String) = (push!(t.writes, data); ncodeunits(data))
    Base.flush(t::KeepaliveFlushTransport) = (put!(t.flushed, true); nothing)
    Base.close(::KeepaliveFlushTransport) = nothing

    transport = KeepaliveFlushTransport(String[], Channel{Bool}(8))
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=transport)
    @lock client.lock begin
        client.pings_out += 1
        N._queue_ping_marker_locked!(client)
    end
    N._send_raw(client, "PING\r\n"; force_flush=true)
    @test timedwait(() -> isready(transport.flushed), 1.0; pollint=0.01) != :timed_out
    take!(transport.flushed)

    @test length(client.pongs) == 1
    keepalive_marker = only(client.pongs)
    @test !keepalive_marker.active
    @test keepalive_marker.ready

    flush_task = @async flush(client; timeout=1.0)
    @test timedwait(() -> length(client.pongs) == 2, 1.0; pollint=0.01) != :timed_out
    @test count(==("PING\r\n"), transport.writes) == 2

    N._notify_pong(client)
    @test timedwait(() -> istaskdone(flush_task), 0.05; pollint=0.01) == :timed_out
    @test length(client.pongs) == 1

    N._notify_pong(client)
    @test fetch(flush_task) === nothing
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

@testitem "public flush does not expose internal deadline keyword" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED)

    err = TestHelpers.thrown_exception(() -> flush(client; deadline=time() + 1.0))
    @test err isa MethodError
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

    err = TestHelpers.thrown_exception() do
        drain(client; timeout=0.12)
    end
    @test N._drain_timed_out(err)
    if err isa CompositeException
        @test any(e -> e isa TimeoutError, err.exceptions)
    else
        @test err isa TimeoutError
    end
    @test count(startswith("UNSUB "), transport.writes) == 1
    @test count(==("PING\r\n"), transport.writes) == 1
    @test length(reported) == 1
    @test N.status(client) == N.ConnectionStatus.CLOSED
end

@testitem "client drain close respects remaining timeout budget" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct ImmediatePongTransportForCallbacks <: IO
        client::Base.RefValue{Any}
    end
    Base.write(::ImmediatePongTransportForCallbacks, data::Vector{UInt8}) = length(data)
    Base.write(::ImmediatePongTransportForCallbacks, data::String) = ncodeunits(data)
    Base.flush(t::ImmediatePongTransportForCallbacks) = (N._notify_pong(t.client[]); nothing)
    Base.close(::ImmediatePongTransportForCallbacks) = nothing

    reported = Any[]
    opts = N.ConnectOptions(; close_callback_timeout=5.0, error_cb=err -> push!(reported, err))
    client_ref = Ref{Any}(nothing)
    transport = ImmediatePongTransportForCallbacks(client_ref)
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED, write_io=transport)
    client_ref[] = client

    entered = Channel{Bool}(1)
    release = Channel{Bool}(1)
    sub = subscribe(client, "callback.work") do _
        put!(entered, true)
        take!(release)
    end
    N._dispatch_msg(client, N.Msg("callback.work", nothing, UInt8[]; sid=sub.sid))
    take!(entered)

    start = time()
    err = TestHelpers.thrown_exception(() -> drain(client; timeout=0.05))
    elapsed = time() - start

    @test N._drain_timed_out(err)
    @test elapsed < 3.0
    @test N.status(client) == N.ConnectionStatus.CLOSED

    put!(release, true)
    wait(sub.processor)
end

@testitem "client drain close shares deadline across background task waits" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct ImmediatePongTransportForBackgroundClose <: IO
        client::Base.RefValue{Any}
    end
    Base.write(::ImmediatePongTransportForBackgroundClose, data::Vector{UInt8}) = length(data)
    Base.write(::ImmediatePongTransportForBackgroundClose, data::String) = ncodeunits(data)
    Base.flush(t::ImmediatePongTransportForBackgroundClose) = (N._notify_pong(t.client[]); nothing)
    Base.close(::ImmediatePongTransportForBackgroundClose) = nothing

    opts = N.ConnectOptions(; connect_timeout=2.0)
    client_ref = Ref{Any}(nothing)
    transport = ImmediatePongTransportForBackgroundClose(client_ref)
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED, write_io=transport)
    client_ref[] = client

    started = Channel{Bool}(1)
    stopped = Channel{Bool}(1)
    blocker = Channel{Bool}(0)
    reader_task = @async begin
        put!(started, true)
        try
            take!(blocker)
        finally
            put!(stopped, true)
        end
    end
    take!(started)
    @lock client.lock client.reader_task = reader_task

    start = time()
    err = TestHelpers.thrown_exception(() -> drain(client; timeout=0.02))
    elapsed = time() - start

    @test N._drain_timed_out(err)
    @test elapsed < 3.0
    @test N.status(client) == N.ConnectionStatus.CLOSED
    @test timedwait(1.0; pollint=0.01) do
        isready(stopped)
    end != :timed_out
end

@testitem "subscription drain deadline covers active UNSUB write lock wait" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct ActiveDrainWriteLockTransport <: IO
        writes::Vector{String}
    end
    Base.write(t::ActiveDrainWriteLockTransport, data::Vector{UInt8}) = (push!(t.writes, String(copy(data))); length(data))
    Base.write(t::ActiveDrainWriteLockTransport, data::String) = (push!(t.writes, data); ncodeunits(data))
    Base.flush(::ActiveDrainWriteLockTransport) = nothing
    Base.close(::ActiveDrainWriteLockTransport) = nothing

    opts = N.ConnectOptions(error_cb=err -> nothing)
    transport = ActiveDrainWriteLockTransport(String[])
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED, write_io=transport)
    sub = subscribe(client, "drain.active")
    @test (@lock sub.lock sub.server_active)

    entered = Channel{Bool}(1)
    release = Channel{Bool}(1)
    holder = @async begin
        lock(client.write_lock)
        put!(entered, true)
        try
            take!(release)
        finally
            unlock(client.write_lock)
        end
    end
    take!(entered)

    drain_task = @async TestHelpers.thrown_exception(() -> drain(sub; timeout=0.02))
    try
        finished = timedwait(2.0; pollint=0.01) do
            istaskdone(drain_task)
        end
        @test finished != :timed_out
        if finished != :timed_out
            err = fetch(drain_task)
            @test err isa TimeoutError
            @test N.status(client) == N.ConnectionStatus.CONNECTED
        end
    finally
        put!(release, true)
        wait(holder)
        istaskdone(drain_task) || wait(drain_task)
        close(client)
    end
end

@testitem "client drain deadline covers active subscription UNSUB write lock wait" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct ActiveClientDrainWriteLockTransport <: IO
        writes::Vector{String}
    end
    Base.write(t::ActiveClientDrainWriteLockTransport, data::Vector{UInt8}) = (push!(t.writes, String(copy(data))); length(data))
    Base.write(t::ActiveClientDrainWriteLockTransport, data::String) = (push!(t.writes, data); ncodeunits(data))
    Base.flush(::ActiveClientDrainWriteLockTransport) = nothing
    Base.close(::ActiveClientDrainWriteLockTransport) = nothing

    opts = N.ConnectOptions(error_cb=err -> nothing)
    transport = ActiveClientDrainWriteLockTransport(String[])
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED, write_io=transport)
    sub = subscribe(client, "drain.client.active")
    @test (@lock sub.lock sub.server_active)

    entered = Channel{Bool}(1)
    release = Channel{Bool}(1)
    holder = @async begin
        lock(client.write_lock)
        put!(entered, true)
        try
            take!(release)
        finally
            unlock(client.write_lock)
        end
    end
    take!(entered)

    drain_task = @async TestHelpers.thrown_exception(() -> drain(client; timeout=0.02))
    try
        finished = timedwait(2.0; pollint=0.01) do
            istaskdone(drain_task)
        end
        @test finished != :timed_out
        if finished != :timed_out
            err = fetch(drain_task)
            @test N._drain_timed_out(err)
            @test N.status(client) == N.ConnectionStatus.CLOSED
        end
    finally
        put!(release, true)
        wait(holder)
        istaskdone(drain_task) || wait(drain_task)
    end
end

@testitem "client drain deadline covers write lock waits" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct DrainWriteLockTransport <: IO
        client::Base.RefValue{Any}
    end
    Base.write(::DrainWriteLockTransport, data::Vector{UInt8}) = length(data)
    Base.write(::DrainWriteLockTransport, data::String) = ncodeunits(data)
    Base.flush(t::DrainWriteLockTransport) = (N._notify_pong(t.client[]); nothing)
    Base.close(::DrainWriteLockTransport) = nothing

    opts = N.ConnectOptions(error_cb=err -> nothing)
    client_ref = Ref{Any}(nothing)
    transport = DrainWriteLockTransport(client_ref)
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED, write_io=transport)
    client_ref[] = client

    entered = Channel{Bool}(1)
    release = Channel{Bool}(1)
    holder = @async begin
        lock(client.write_lock)
        put!(entered, true)
        try
            take!(release)
        finally
            unlock(client.write_lock)
        end
    end
    take!(entered)

    drain_task = @async TestHelpers.thrown_exception(() -> drain(client; timeout=0.02))
    try
        finished = timedwait(2.0; pollint=0.01) do
            istaskdone(drain_task)
        end
        @test finished != :timed_out
        if finished != :timed_out
            err = fetch(drain_task)
            @test N._drain_timed_out(err)
            @test N.status(client) == N.ConnectionStatus.CLOSED
            detached = @lock client.lock begin
                isnothing(client.read_io) &&
                    isnothing(client.reader) &&
                    isnothing(client.write_io) &&
                    isnothing(client.socket)
            end
            @test detached
        end
    finally
        put!(release, true)
        wait(holder)
        istaskdone(drain_task) || wait(drain_task)
    end
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

        opts = N.ConnectOptions(read_buffer_size=8192)
        configured = TestHelpers.fake_client(; opts, read_io=sock)
        @test typeof(configured.reader) === N.ProtocolReader{Sockets.TCPSocket}
        @test length(configured.reader.scratch) == 8192
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
    current = N.Server(client.connected_url)
    push!(client.servers, current)
    client.current_server = current

    info = N.ServerInfo(; connect_urls=["10.0.0.2:4222", "nats://plain.test:4222"])
    N._merge_discovered_servers!(client, info)
    urls = [server.url for server in client.servers]
    @test "tls://user:pass@10.0.0.2:4222" in urls
    @test "nats://plain.test:4222" in urls
    ip_server = client.servers[findfirst(server -> server.url == "tls://user:pass@10.0.0.2:4222",
                                         client.servers)]
    plain_server = client.servers[findfirst(server -> server.url == "nats://plain.test:4222",
                                            client.servers)]
    @test ip_server.tls_name == "example.test"
    @test N._tls_hostname(ip_server, "10.0.0.2") == "example.test"
    @test isnothing(plain_server.tls_name)
end

@testitem "discovered server merge prunes stale routes" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    callbacks = Ref(0)
    opts = N.ConnectOptions(; event_cb=event -> begin
        event.kind == N.ConnectionEventKind.DISCOVERED_SERVERS && (callbacks[] += 1)
    end)
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

@testitem "connection events and reconnect delay callbacks are typed" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    events = N.ConnectionEvent[]
    errors = Any[]
    opts = N.ConnectOptions(;
        error_cb=err -> push!(errors, err),
        event_cb=event -> push!(events, event),
        reconnect_delay_cb=event -> event.kind == N.ConnectionEventKind.RECONNECT_DELAY ? 0.25 : nothing,
    )
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.RECONNECTING)
    server = N.Server("nats://reconnect.test:4222")
    @lock client.lock begin
        client.current_server = server
        client.connected_url = server.url
    end

    event = N._emit_connection_event(client, N.ConnectionEventKind.RECONNECT_ATTEMPT;
                                     server, url=server.url, attempt=3, generation=client.generation)
    @test event.kind == N.ConnectionEventKind.RECONNECT_ATTEMPT
    @test event.status == N.ConnectionStatus.RECONNECTING
    @test event.server === server
    @test event.url == server.url
    @test event.attempt == 3
    @test event.generation == client.generation
    @test only(events) === event

    delay_event = N._connection_event(client, N.ConnectionEventKind.RECONNECT_DELAY;
                                      attempt=3, delay=1.0, generation=client.generation)
    @test N._resolve_reconnect_delay(client, delay_event, 1.0) == 0.25
    @test isempty(errors)

    bad_opts = N.ConnectOptions(;
        error_cb=err -> push!(errors, err),
        reconnect_delay_cb=_ -> -1,
    )
    bad_client = TestHelpers.fake_client(; opts=bad_opts, status=N.ConnectionStatus.RECONNECTING)
    @test N._resolve_reconnect_delay(bad_client, delay_event, 1.0) == 1.0
    @test last(errors) isa ArgumentError
end

@testitem "TLS first handshake mode is explicit and overridable" begin
    using Natter

    const N = Natter

    @test N._tls_first_for_connection(ConnectOptions(), "nats") == false
    @test N._tls_first_for_connection(ConnectOptions(), "tls") == true
    @test N._tls_first_for_connection(ConnectOptions(tls_first=true), "nats") == true
    @test N._tls_first_for_connection(ConnectOptions(tls_first=false), "tls") == false
end

@testitem "TLS server name override and IP SAN matching" begin
    using Base64
    using MbedTLS
    using Natter
    using Sockets

    const N = Natter

    opts = ConnectOptions(tls_server_name="nats.internal")
    @test opts.tls_server_name == "nats.internal"
    @test_throws ArgumentError ConnectOptions(tls_server_name="")
    @test_throws ArgumentError ConnectOptions(tls_server_name=:nats)

    server = N.Server("tls://127.0.0.1:4222"; tls_name="discovered.example")
    @test N._tls_server_name(ConnectOptions(), server, "127.0.0.1") == "discovered.example"
    @test N._tls_server_name(opts, server, "127.0.0.1") == "nats.internal"

    cert_der = base64decode(join((
        "MIIDLDCCAhSgAwIBAgIUFd4E14pGmodfCwBFy7FGLd6cYSowDQYJKoZIhvcNAQEL",
        "BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDUxNTEyNDg0NVoXDTI3",
        "MDUxNTEyNDg0NVowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG",
        "9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtCHx7n802x9FJkzVrHLaFLkDOMZPWrg/",
        "fnaUDaht5rO9/XgH5/aVR6bmGnqUgfDpQJgGPaBgB0shUhJfCAZpPTyxfnk",
        "QzYL5V2X4MWSkwURcap1+40f5AK6DsCsa6LOm+gDCdeuEs3Xy+U1OVD03fo",
        "eXXpZtNyznSbQWoh6w5L1sRnqovmx+m1zlFmiPCLi3TmIeZgJ6+hOWmK2W18",
        "DeCU9ImpJCXSi5A39m7ePwwpqHmbnCMhBub2ihUVMmJ6uipxBdhmBQ4lJBZ",
        "3UxW65VYmZyCXg5NU+aKbR5wDLg2mcR+pwKrghcPNbbkJfNeUviYK86Aazwg",
        "sU2yIzdJZp0TQIDAQABo3YwdDAdBgNVHQ4EFgQUlocppOpVmJZpI4XBtPJW",
        "6oYUXbowHwYDVR0jBBgwFoAUlocppOpVmJZpI4XBtPJW6oYUXbowDwYDVR0T",
        "AQH/BAUwAwEB/zAhBgNVHREEGjAYhwR/AAABhxAAAAAAAAAAAAAAAAAAAAAB",
        "MA0GCSqGSIb3DQEBCwUAA4IBAQAfNoBAjn6f6SDSBlMAsyD48LExw+GapZr",
        "OGk/Iell8tj+PjmDyueybQDW4H6T7nBX/fvAt1iiD2sh42+qviT1MtAYif9",
        "+lujVZEzbUDm9kUW0iApGQVc2fixqaqYEvAaWG589oVbExAa5vAC1EP5zww",
        "OXj5+hqDdjRr6US5qECwAN0DlnPCkJRM7zxRAr01UadQD54rcP6uZeoA3qZ",
        "kiUL7nvClIn/RcPCJtIIr+yfs8R3o7k3PdayZNnLEUChDhNRFCiE73ul4fK",
        "vgT3zeDyZBU6lL3ElFLfzaUfCNLz0mpfonXdNXtJ21JoqblmtI4W/olUz6R",
        "NtON28U4mpyRzQ",
    )))
    @test N._tls_certificate_has_ip_san(cert_der, "127.0.0.1")
    @test N._tls_certificate_has_ip_san(cert_der, "::1")
    @test N._tls_certificate_has_ip_san(cert_der, "[::1]")
    @test !N._tls_certificate_has_ip_san(cert_der, "127.0.0.2")
    @test !N._tls_certificate_has_ip_san(cert_der, "localhost")

    openssl = Sys.which("openssl")
    if isnothing(openssl)
        @test_skip "openssl unavailable for local TLS handshake check"
    else
        mktempdir() do dir
            cert_path = joinpath(dir, "cert.pem")
            key_path = joinpath(dir, "key.pem")
            generated = success(pipeline(
                `$openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost -keyout $key_path -out $cert_path -addext subjectAltName=IP:127.0.0.1`;
                stdout=devnull, stderr=devnull,
            ))
            if !generated
                @test_skip "openssl could not generate an IP SAN certificate"
                return
            end

            listener = listen(ip"127.0.0.1", 0)
            _ip, port = getsockname(listener)
            server_task = @async begin
                sock = accept(listener)
                ctx = nothing
                try
                    entropy = MbedTLS.Entropy()
                    rng = MbedTLS.CtrDrbg()
                    MbedTLS.seed!(rng, entropy)
                    conf = MbedTLS.SSLConfig()
                    MbedTLS.config_defaults!(conf; endpoint=MbedTLS.MBEDTLS_SSL_IS_SERVER)
                    MbedTLS.rng!(conf, rng)
                    MbedTLS.own_cert!(conf, MbedTLS.crt_parse_file(cert_path), MbedTLS.parse_keyfile(key_path))
                    ctx = MbedTLS.SSLContext()
                    MbedTLS.setup!(ctx, conf)
                    MbedTLS.set_bio!(ctx, sock)
                    MbedTLS.handshake(ctx)
                    nothing
                catch err
                    err
                finally
                    isnothing(ctx) ? close(sock) : close(ctx)
                end
            end

            client_sock = Sockets.connect(ip"127.0.0.1", port)
            client_ctx = nothing
            client_err = nothing
            try
                client_ctx = N._tls_wrap(client_sock, ConnectOptions(tls_ca_path=cert_path), "127.0.0.1")
                @test client_ctx isa MbedTLS.SSLContext
            catch err
                client_err = err
            finally
                close(listener)
                isnothing(client_ctx) ? close(client_sock) : close(client_ctx)
            end
            server_err = fetch(server_task)
            isnothing(client_err) || throw(client_err)
            @test isnothing(server_err)
        end
    end
end

@testitem "TLS verification is enabled by default and can be disabled" begin
    using MbedTLS
    using Natter

    const N = Natter

    @test ConnectOptions().tls_verify == true
    @test isnothing(ConnectOptions().tls_server_name)
    @test N._tls_authmode(ConnectOptions()) == MbedTLS.MBEDTLS_SSL_VERIFY_REQUIRED
    @test N._tls_authmode(ConnectOptions(tls_verify=false)) == MbedTLS.MBEDTLS_SSL_VERIFY_NONE

    conf = N._tls_config(ConnectOptions())
    @test isdefined(conf, :chain)
    @test conf.chain isa MbedTLS.CRT
end
