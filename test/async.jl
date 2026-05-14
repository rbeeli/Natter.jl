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
    @test isnothing(fetch(close_async(callback_sub)))

    closed = TestHelpers.fake_client(; status=N.ConnectionStatus.DISCONNECTED)
    publish_err = TestHelpers.thrown_exception(() -> fetch(publish_async(closed, "foo", "bar")))
    @test publish_err isa CapturedException
    @test publish_err.ex isa ConnectionClosedError

    subscribe_err = TestHelpers.thrown_exception(() -> fetch(subscribe_async(closed, "foo")))
    @test subscribe_err isa CapturedException
    @test subscribe_err.ex isa ConnectionClosedError
end

@testitem "async wrappers preserve synchronous validation failures" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    js = jetstream(client)
    kv = KeyValue(js, "bucket", "KV_bucket", "\$KV.bucket.")

    get_err = TestHelpers.thrown_exception(() -> fetch(stream_message_get_async(js, "ORDERS"; seq=0)))
    @test get_err isa CapturedException
    @test get_err.ex isa ArgumentError

    delete_err = TestHelpers.thrown_exception(() -> fetch(stream_message_delete_async(js, "ORDERS", 0)))
    @test delete_err isa CapturedException
    @test delete_err.ex isa ArgumentError

    ack_msg = JetStreamMsg(Msg("s", nothing, UInt8[]), client)
    ack_err = TestHelpers.thrown_exception(() -> fetch(ack_async(ack_msg)))
    @test ack_err isa CapturedException
    @test ack_err.ex isa JetStreamError

    put_err = TestHelpers.thrown_exception(() -> fetch(kv_put_async(kv, "bad.*", "value")))
    @test put_err isa CapturedException
    @test put_err.ex isa ArgumentError
end

@testitem "NatterTask failures preserve task backtraces" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    function natter_async_backtrace_marker()
        throw(ArgumentError("async marker"))
    end

    err = TestHelpers.thrown_exception(() -> fetch(N._natter_async(natter_async_backtrace_marker)))
    @test err isa CapturedException
    @test err.ex isa ArgumentError
    @test err.ex.msg == "async marker"

    shown = sprint(showerror, err)
    @test occursin("natter_async_backtrace_marker", shown)
    @test !occursin("_rethrow_task_failure", shown)

    function natter_nested_async_failure()
        try
            throw(ArgumentError("inner"))
        catch
            throw(ArgumentError("outer"))
        end
    end

    nested_err = TestHelpers.thrown_exception(() -> fetch(N._natter_async(natter_nested_async_failure)))
    @test nested_err isa CapturedException
    @test nested_err.ex isa ArgumentError
    @test nested_err.ex.msg == "outer"
    @test occursin("natter_nested_async_failure", sprint(showerror, nested_err))
end
