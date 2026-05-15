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

    close_task = close_async(watcher)

    @test close_task isa NatterTask
    @test isnothing(fetch(close_task))
    @test push.closed
    @test push_sub.closed
    @test N._kv_watcher_closed(watcher_state)
    @test !isopen(watcher.updates)
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

    ack_msg = JetStreamMsg(Msg("s", nothing, UInt8[]), client)
    ack_err = TestHelpers.thrown_exception(() -> fetch(ack_async(ack_msg)))
    @test ack_err isa JetStreamError

    put_err = TestHelpers.thrown_exception(() -> fetch(kv_put_async(kv, "bad.*", "value")))
    @test put_err isa ArgumentError
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
