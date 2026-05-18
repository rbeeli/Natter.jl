using TestItems

@testitem "core async wrappers return task results and errors" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)

    publish_task = publish_async(client, "foo", "bar")
    @test publish_task isa NatterTask
    @test isnothing(fetch(publish_task))
    @test client.pending_bytes == length("PUB foo 3\r\nbar\r\n")

    sub = fetch(subscribe_async(client, "foo"))
    @test sub isa Subscription
    put!(sub.messages, Msg("foo", nothing, TestHelpers.bytes("hello"); sid=sub.sid))
    @test String(fetch(next_async(sub; timeout=0.1))) == "hello"
    @test isnothing(fetch(close_async(sub)))

    callback_sub = fetch(subscribe_async(_ -> nothing, client, "foo.callback"))
    @test callback_sub.has_callback
    next_err = TestHelpers.thrown_exception(() -> fetch(next_async(callback_sub; timeout=0.1)))
    @test next_err isa ArgumentError
    @test isnothing(fetch(close_async(callback_sub)))

    closed = TestHelpers.fake_client(; status=N.ConnectionStatus.DISCONNECTED)
    publish_err = TestHelpers.thrown_exception(() -> fetch(publish_async(closed, "foo", "bar")))
    @test publish_err isa ConnectionClosedError

    subscribe_err = TestHelpers.thrown_exception(() -> fetch(subscribe_async(closed, "foo")))
    @test subscribe_err isa ConnectionClosedError
end

@testitem "KeyValue watcher async close returns task and closes watcher" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    js = jetstream(client)
    push_sub = subscribe(client, "_INBOX.kv")
    push = N.PushSubscription(js, push_sub, "KV_bucket", "watcher", ReentrantLock(), false, false)
    watcher_state = N._kv_watcher_state(nothing, 1, true)
    watcher = KeyValueWatcher(push, watcher_state.updates, watcher_state)

    close_task = close_async(watcher; timeout=0.1)

    @test close_task isa NatterTask
    @test isnothing(fetch(close_task))
    @test push.closed
    @test push_sub.closed
    @test N._kv_watcher_closed(watcher_state)
    @test !isopen(watcher.updates)
end

@testitem "JetStream subscription async close accepts timeout" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    js = jetstream(client)

    pull = N.PullSubscription(js, "S", "C", ReentrantLock(), ReentrantLock(), false, false)
    pull_task = close_async(pull; timeout=0.1)
    @test pull_task isa NatterTask
    @test isnothing(fetch(pull_task))
    @test pull.closed

    push_core = subscribe(client, "_INBOX.push")
    push = N.PushSubscription(js, push_core, "S", "C", ReentrantLock(), false, false)
    push_task = close_async(push; timeout=0.1)
    @test push_task isa NatterTask
    @test isnothing(fetch(push_task))
    @test push.closed
    @test push_core.closed
end

@testitem "async wrappers preserve synchronous validation failures" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    js = jetstream(client)
    kv = KeyValue(js, "bucket", "KV_bucket", "\$KV.bucket.")

    get_err = TestHelpers.thrown_exception(() -> fetch(stream_message_get_async(js, "ORDERS"; seq=0)))
    @test get_err isa ArgumentError

    delete_err = TestHelpers.thrown_exception(() -> fetch(stream_message_delete_async(js, "ORDERS", 0)))
    @test delete_err isa ArgumentError

    ordered_err = TestHelpers.thrown_exception(() -> fetch(push_subscribe_async(
        js,
        "orders.created";
        stream="ORDERS",
        ordered=true,
        queue="workers",
    )))
    @test ordered_err isa ArgumentError

    ack_msg = JetStreamMsg(Msg("s", nothing, UInt8[]), client)
    ack_err = TestHelpers.thrown_exception(() -> fetch(ack_async(ack_msg)))
    @test ack_err isa JetStreamError

    put_err = TestHelpers.thrown_exception(() -> fetch(kv_put_async(kv, "bad.*", "value")))
    @test put_err isa ArgumentError
end

@testitem "JetStream ack async helpers mirror ack signatures" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)

    function borrowed(reply="ACK.REPLY")
        data = TestHelpers.bytes("work")
        BorrowedJetStreamMsg(BorrowedMsg("orders.created", reply, @view(data[1:4]), nothing, 1, 0),
                             client)
    end

    source = CancellationSource()
    cancel!(source)
    cancelled = TestHelpers.thrown_exception() do
        fetch(ack_async(JetStreamMsg(Msg("orders.created", "ACK.REPLY", TestHelpers.bytes("work")),
                                     client);
                        cancel_token=cancellation_token(source)))
    end
    @test cancelled isa CancelledError
    @test TestHelpers.capture_text(capture) == ""

    @test isnothing(fetch(ack_async(borrowed())))
    @test TestHelpers.capture_text(capture) == "PUB ACK.REPLY 0\r\n\r\n"

    TestHelpers.clear_capture!(capture)
    @test isnothing(fetch(nak_async(borrowed(); delay=0)))
    @test TestHelpers.capture_text(capture) == "PUB ACK.REPLY 16\r\n-NAK {\"delay\":0}\r\n"

    TestHelpers.clear_capture!(capture)
    @test isnothing(fetch(in_progress_async(borrowed())))
    @test TestHelpers.capture_text(capture) == "PUB ACK.REPLY 4\r\n+WPI\r\n"

    TestHelpers.clear_capture!(capture)
    @test isnothing(fetch(term_async(borrowed())))
    @test TestHelpers.capture_text(capture) == "PUB ACK.REPLY 5\r\n+TERM\r\n"

    sync_err = TestHelpers.thrown_exception() do
        fetch(ack_sync_async(borrowed(nothing); timeout=0.1))
    end
    @test sync_err isa JetStreamError
end

@testitem "NatterTask failures rethrow original operation errors" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    mutable struct NatterAsyncMarkerError <: Exception
        msg::String
    end

    marker = NatterAsyncMarkerError("async marker")

    function natter_async_marker()
        throw(marker)
    end

    err = TestHelpers.thrown_exception(() -> fetch(N._natter_async(natter_async_marker)))
    @test err === marker

    function natter_nested_async_failure()
        try
            throw(ArgumentError("inner"))
        catch
            throw(ArgumentError("outer"))
        end
    end

    nested_err = TestHelpers.thrown_exception(() -> fetch(N._natter_async(natter_nested_async_failure)))
    @test nested_err isa ArgumentError
    @test nested_err.msg == "outer"
end
