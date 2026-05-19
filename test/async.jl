using TestItems

@testitem "public async API uses Base tasks and domain handles" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream
    using Natter.KeyValue

    const N = Natter

    removed = (
        :NatterTask,
        :connect_async,
        :publish_async,
        :respond_async,
        :subscribe_async,
        :unsubscribe_async,
        :next_async,
        :request_async,
        :flush_async,
        :ping_async,
        :drain_async,
        :close_async,
        :fetch_async,
        :ack_async,
        :ack_sync_async,
        :nak_async,
        :in_progress_async,
        :term_async,
        :kv_get_async,
    )
    @test all(name -> !isdefined(N, name), removed)

    @test :JetStream in names(N)
    @test :KeyValue in names(N)
    @test :js_publish in names(N.JetStream)
    @test :kv_get in names(N.KeyValue)
    @test !(:js_publish in names(N))
    @test !(:kv_get in names(N))
end

@testitem "internal task helpers use non-sticky Julia tasks" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    control = N._spawn_control(:control_probe) do
        (task_local_storage(:natter_task_name), Threads.threadpool())
    end
    work = N._spawn_work(:work_probe) do
        (task_local_storage(:natter_task_name), Threads.threadpool())
    end
    sticky = N._spawn_sticky(:sticky_probe) do
        task_local_storage(:natter_task_name)
    end

    @test control isa Task
    @test work isa Task
    @test sticky isa Task
    @test control.sticky == false
    @test work.sticky == false
    @test sticky.sticky == true
    control_name, control_pool = fetch(control)
    @test control_name == :control_probe
    @test control_pool in (:interactive, :default)
    @test fetch(work) == (:work_probe, :default)
    @test fetch(sticky) == :sticky_probe

    failed = N._spawn_work(:failure_probe) do
        error("task failed")
    end
    @test failed.sticky == false
    @test_throws TaskFailedException fetch(failed)
end

@testitem "source uses private task helpers instead of raw async macro" setup=[TestHelpers] begin
    using Natter

    srcdir = joinpath(pkgdir(Natter), "src")
    needle = string('@', "async")
    matches = String[]

    for (root, _dirs, files) in walkdir(srcdir)
        for file in files
            endswith(file, ".jl") || continue
            path = joinpath(root, file)
            occursin(needle, read(path, String)) && push!(matches, relpath(path, srcdir))
        end
    end

    @test isempty(matches)
end

@testitem "Base task composition runs direct operations" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)

    publish_task = Threads.@spawn publish(client, "foo", "bar"; buffer_on_reconnect=true)
    @test publish_task isa Task
    @test isnothing(fetch(publish_task))
    @test client.pending_bytes == length("PUB foo 3\r\nbar\r\n")

    response_client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    respond_task = Threads.@spawn respond(response_client, Msg("svc", "_INBOX.reply", UInt8[]), "ok";
                                  buffer_on_reconnect=true)
    @test isnothing(fetch(respond_task))
    @test String(take!(response_client.pending)) == "PUB _INBOX.reply 2\r\nok\r\n"

    sub = fetch(Threads.@spawn subscribe(client, "foo"))
    put!(sub.messages, Msg("foo", nothing, TestHelpers.bytes("hello"); sid=sub.sid))
    @test String(fetch(Threads.@spawn N.next(sub; timeout=0.1))) == "hello"
    @test isnothing(fetch(Threads.@spawn close(sub)))
end

@testitem "Base task composition preserves direct validation and cancellation behavior" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream
    using Natter.KeyValue

    const N = Natter

    closed = TestHelpers.fake_client(; status=N.ConnectionStatus.DISCONNECTED)
    publish_task = Threads.@spawn TestHelpers.thrown_exception(() -> publish(closed, "foo", "bar"))
    @test fetch(publish_task) isa ConnectionClosedError

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    sub = subscribe(client, "foo")
    source = CancellationSource()
    token = cancellation_token(source)
    next_task = Threads.@spawn TestHelpers.thrown_exception(() -> N.next(sub; timeout=30.0, cancel_token=token))
    sleep(0.02)
    cancel!(source)
    @test fetch(next_task) isa CancelledError

    js = jetstream(client)
    kv = KeyValueBucket(js, "bucket", "KV_bucket", "\$KV.bucket.")
    put_task = Threads.@spawn TestHelpers.thrown_exception(() -> kv_put(kv, "bad.*", "value"))
    @test fetch(put_task) isa ArgumentError

    ack_msg = JetStreamMsg(Msg("s", nothing, UInt8[]), client)
    ack_task = Threads.@spawn TestHelpers.thrown_exception(() -> ack(ack_msg))
    @test fetch(ack_task) isa JetStreamError
end

@testitem "JetStream publish futures remain protocol-level handles" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)

    future = js_publish_future(js, "orders.created", "payload"; timeout=1.0)
    @test future isa JetStreamPublishFuture
    @test !(future isa Task)
    @test js_publish_future_pending(js) == 1
end
