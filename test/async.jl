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
    put!(sub.messages, Msg("foo", nothing, TestHelpers.bytes("hello"); client, sid=sub.sid))
    @test String(fetch(next_async(sub; timeout=0.1))) == "hello"
    @test isnothing(fetch(close_async(sub)))

    callback_sub = fetch(subscribe_async(_ -> nothing, client, "foo.callback"))
    @test callback_sub.has_callback
    @test isnothing(fetch(close_async(callback_sub)))

    closed = TestHelpers.fake_client(; status=N.ConnectionStatus.DISCONNECTED)
    @test_throws ConnectionClosedError fetch(publish_async(closed, "foo", "bar"))
    @test_throws ConnectionClosedError fetch(subscribe_async(closed, "foo"))
end

@testitem "async wrappers preserve synchronous validation failures" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    js = jetstream(client)
    kv = KeyValue(js, "bucket", "KV_bucket", "\$KV.bucket.")

    @test_throws ArgumentError fetch(stream_message_get_async(js, "ORDERS"; seq=0))
    @test_throws ArgumentError fetch(stream_message_delete_async(js, "ORDERS", 0))
    @test_throws JetStreamError fetch(ack_async(Msg("s", nothing, UInt8[]; client)))
    @test_throws ArgumentError fetch(kv_put_async(kv, "bad.*", "value"))
end
