using TestItems

@testmodule ChaosTestHelpers begin
    using Natter
    using Natter.JetStream
    using Natter.KeyValue

    chaos_enabled() =
        get(ENV, "NATTER_RUN_INTEGRATION", "false") == "true" &&
        get(ENV, "NATTER_RUN_CHAOS", "false") == "true"

    stress_enabled() =
        get(ENV, "NATTER_RUN_INTEGRATION", "false") == "true" &&
        get(ENV, "NATTER_RUN_STRESS", "false") == "true"

    function wait_reconnecting(client; timeout::Real)
        timedwait(timeout; pollint=0.01) do
            status(client) == ConnectionStatus.RECONNECTING
        end != :timed_out
    end

    function wait_reconnected(client, expected_reconnects::Int; timeout::Real)
        timedwait(timeout; pollint=0.02) do
            status(client) == ConnectionStatus.CONNECTED &&
                stats(client).reconnects >= expected_reconnects
        end != :timed_out
    end

    function task_error(task::Task)
        try
            fetch(task)
            return nothing
        catch err
            err isa TaskFailedException && return first(Base.current_exceptions(task)).exception
            return err
        end
    end
end

@testitem "real nats-server chaos reconnect publish replay" setup=[IntegrationHelpers, ChaosTestHelpers] begin
    using Natter
    using Natter.JetStream
    using Natter.KeyValue
    using Random

    const N = Natter

    if ChaosTestHelpers.chaos_enabled()
        url = get(ENV, "NATTER_URL", "nats://127.0.0.1:4222")
        scheme, host, port, _, _ = N._server_parts(url)
        io_timeout = IntegrationHelpers.integration_timeout()

        if scheme == "nats"
            proxy = IntegrationHelpers.start_tcp_proxy(host, port)
            client = connect(proxy.url;
                             connect_timeout=IntegrationHelpers.integration_connect_timeout(),
                             ping_interval=0.2,
                             max_outstanding_pings=1,
                             reconnect_wait=0.05,
                             reconnect_jitter=0.0,
                             max_reconnect_attempts=80,
                             record_stats=true)
            service = Ref{Any}(nothing)
            sub = Ref{Any}(nothing)
            try
                subject = "natter.chaos.core.$(randstring(10))"
                sub[] = subscribe(client, subject)
                service[] = subscribe(client, "$subject.req") do req
                    publish(client, req.reply, "reply-$(String(req.data))")
                end

                IntegrationHelpers.publish_and_flush(client, subject, "before"; timeout=io_timeout)
                @test String(take!(sub[]; timeout=io_timeout)) == "before"

                for i in 1:IntegrationHelpers.chaos_iterations()
                    expected_reconnects = stats(client).reconnects + 1
                    proxy.pause_new_connections()
                    proxy.drop_connections()
                    @test ChaosTestHelpers.wait_reconnecting(client; timeout=max(io_timeout, 2.0))

                    publish(client, subject, "during-$i"; mode=PublishMode.REPLAYABLE)
                    proxy.release()
                    @test ChaosTestHelpers.wait_reconnected(client, expected_reconnects;
                                                            timeout=max(io_timeout, 5.0))

                    flush(client; timeout=io_timeout)
                    @test String(take!(sub[]; timeout=io_timeout)) == "during-$i"
                    @test String(request(client, "$subject.req", "$i"; timeout=io_timeout)) == "reply-$i"
                end
            finally
                try
                    try
                        isnothing(service[]) || close(service[])
                        isnothing(sub[]) || close(sub[])
                    finally
                        close(client)
                    end
                finally
                    proxy.stop()
                end
            end
        else
            @info "Skipping chaos reconnect publish replay test; NATTER_URL must use nats:// for the local proxy."
        end
    else
        @info "Skipping chaos reconnect publish replay test; set NATTER_RUN_INTEGRATION=true and NATTER_RUN_CHAOS=true to enable it."
    end
end

@testitem "real nats-server chaos slow consumer reports backpressure" setup=[IntegrationHelpers, ChaosTestHelpers] begin
    using Natter
    using Natter.JetStream
    using Natter.KeyValue
    using Random

    if ChaosTestHelpers.chaos_enabled()
        url = get(ENV, "NATTER_URL", "nats://127.0.0.1:4222")
        io_timeout = IntegrationHelpers.integration_timeout()
        reported = Channel{Any}(16)
        client = connect(url;
                         connect_timeout=IntegrationHelpers.integration_connect_timeout(),
                         ping_interval=0.2,
                         max_outstanding_pings=1,
                         record_stats=true,
                         error_cb=err -> put!(reported, err))
        try
            subject = "natter.chaos.slow.$(randstring(10))"
            sub = subscribe(client, subject; pending_msgs_limit=1, pending_bytes_limit=1024)
            try
                for i in 1:8
                    publish(client, subject, "msg-$i")
                end
                flush(client; timeout=io_timeout)

                @test timedwait(io_timeout; pollint=0.01) do
                    isready(reported)
                end != :timed_out

                saw_slow = false
                while isready(reported)
                    saw_slow |= take!(reported) isa SlowConsumerError
                end
                @test saw_slow
                @test stats(client).dropped_msgs > 0

                @test startswith(String(take!(sub; timeout=io_timeout)), "msg-")

                healthy_subject = "$subject.healthy"
                healthy = subscribe(client, healthy_subject)
                try
                    IntegrationHelpers.publish_and_flush(client, healthy_subject, "ok"; timeout=io_timeout)
                    @test String(take!(healthy; timeout=io_timeout)) == "ok"
                finally
                    close(healthy)
                end
            finally
                close(sub)
            end
        finally
            close(client)
        end
    else
        @info "Skipping chaos slow consumer test; set NATTER_RUN_INTEGRATION=true and NATTER_RUN_CHAOS=true to enable it."
    end
end

@testitem "real nats-server chaos JetStream fetch disconnect recovers" setup=[IntegrationHelpers, ChaosTestHelpers] begin
    using Natter
    using Natter.JetStream
    using Natter.KeyValue
    using Random

    const N = Natter

    if ChaosTestHelpers.chaos_enabled() && get(ENV, "NATTER_RUN_JETSTREAM", "false") == "true"
        url = get(ENV, "NATTER_URL", "nats://127.0.0.1:4222")
        scheme, host, port, _, _ = N._server_parts(url)
        io_timeout = IntegrationHelpers.integration_timeout()

        if scheme == "nats"
            proxy = IntegrationHelpers.start_tcp_proxy(host, port)
            client = connect(proxy.url;
                             connect_timeout=IntegrationHelpers.integration_connect_timeout(),
                             ping_interval=0.2,
                             max_outstanding_pings=1,
                             reconnect_wait=0.05,
                             reconnect_jitter=0.0,
                             max_reconnect_attempts=80,
                             record_stats=true)
            js = jetstream(client; timeout=io_timeout)
            stream = "NATTER_CHAOS_FETCH_$(randstring(8))"
            subject = "natter.chaos.fetch.$(randstring(8))"
            durable = "FETCH_$(randstring(6))"
            stream_created = Ref(false)
            psub = Ref{Any}(nothing)
            try
                stream_create(js, StreamConfig(name=stream, subjects=[subject], storage=StorageType.MEMORY))
                stream_created[] = true
                psub[] = pull_subscribe(js, subject; stream, durable)

                before_out = stats(client).out_msgs
                fetch_task = Threads.@spawn fetch(psub[], 1; timeout=max(io_timeout, 5.0), heartbeat=0)
                @test timedwait(io_timeout; pollint=0.01) do
                    stats(client).out_msgs > before_out
                end != :timed_out

                expected_reconnects = stats(client).reconnects + 1
                proxy.pause_new_connections()
                proxy.drop_connections()
                err = ChaosTestHelpers.task_error(fetch_task)
                @test err isa FetchDisconnectedError

                proxy.release()
                @test ChaosTestHelpers.wait_reconnected(client, expected_reconnects;
                                                        timeout=max(io_timeout, 5.0))

                js_publish(js, subject, "after"; stream, timeout=io_timeout)
                msgs = fetch(psub[], 1; timeout=io_timeout, heartbeat=0)
                @test length(msgs) == 1
                @test String(first(msgs)) == "after"
                ack(first(msgs))
            finally
                try
                    try
                        !isnothing(psub[]) && status(client) == ConnectionStatus.CONNECTED && close(psub[])
                        stream_created[] && status(client) == ConnectionStatus.CONNECTED &&
                            stream_delete(js, stream; timeout=io_timeout)
                    finally
                        close(client)
                    end
                finally
                    proxy.stop()
                end
            end
        else
            @info "Skipping chaos JetStream fetch test; NATTER_URL must use nats:// for the local proxy."
        end
    else
        @info "Skipping chaos JetStream fetch test; set NATTER_RUN_INTEGRATION=true, NATTER_RUN_CHAOS=true, and NATTER_RUN_JETSTREAM=true to enable it."
    end
end

@testitem "real nats-server chaos KeyValue watcher survives reconnect" setup=[IntegrationHelpers, ChaosTestHelpers] begin
    using Natter
    using Natter.JetStream
    using Natter.KeyValue
    using Random

    const N = Natter

    if ChaosTestHelpers.chaos_enabled() && get(ENV, "NATTER_RUN_JETSTREAM", "false") == "true"
        url = get(ENV, "NATTER_URL", "nats://127.0.0.1:4222")
        scheme, host, port, _, _ = N._server_parts(url)
        io_timeout = IntegrationHelpers.integration_timeout()

        if scheme == "nats"
            proxy = IntegrationHelpers.start_tcp_proxy(host, port)
            client = connect(proxy.url;
                             connect_timeout=IntegrationHelpers.integration_connect_timeout(),
                             ping_interval=0.2,
                             max_outstanding_pings=1,
                             reconnect_wait=0.05,
                             reconnect_jitter=0.0,
                             max_reconnect_attempts=80,
                             record_stats=true)
            js = jetstream(client; timeout=io_timeout)
            bucket = "NATTERCHAOSKV_$(randstring(8))"
            kv = Ref{Union{Nothing,KeyValueBucket}}(nothing)
            watcher = Ref{Any}(nothing)
            try
                kv[] = kv_create(js, bucket; storage="memory", timeout=io_timeout)
                watcher[] = kv_watch(kv[]; key="watched", updates_only=true, meta_only=true,
                                     timeout=io_timeout)

                kv_put(kv[], "watched", "before"; timeout=io_timeout)
                before = N._kv_take!(watcher[], io_timeout)
                @test before isa KeyValueEntry
                @test before.key == "watched"

                expected_reconnects = stats(client).reconnects + 1
                proxy.pause_new_connections()
                proxy.drop_connections()
                @test ChaosTestHelpers.wait_reconnecting(client; timeout=max(io_timeout, 2.0))
                proxy.release()
                @test ChaosTestHelpers.wait_reconnected(client, expected_reconnects;
                                                        timeout=max(io_timeout, 5.0))

                kv_put(kv[], "watched", "after"; timeout=io_timeout)
                after = N._kv_take!(watcher[], io_timeout)
                @test after isa KeyValueEntry
                @test after.key == "watched"
            finally
                try
                    try
                        !isnothing(watcher[]) && status(client) == ConnectionStatus.CONNECTED && close(watcher[])
                        !isnothing(kv[]) && status(client) == ConnectionStatus.CONNECTED &&
                            kv_delete_bucket(kv[]; timeout=io_timeout)
                    finally
                        close(client)
                    end
                finally
                    proxy.stop()
                end
            end
        else
            @info "Skipping chaos KeyValue watcher test; NATTER_URL must use nats:// for the local proxy."
        end
    else
        @info "Skipping chaos KeyValue watcher test; set NATTER_RUN_INTEGRATION=true, NATTER_RUN_CHAOS=true, and NATTER_RUN_JETSTREAM=true to enable it."
    end
end

@testitem "real nats-server stress reconnect workload" setup=[IntegrationHelpers, ChaosTestHelpers] begin
    using Natter
    using Natter.JetStream
    using Natter.KeyValue
    using Random

    const N = Natter

    if ChaosTestHelpers.stress_enabled()
        url = get(ENV, "NATTER_URL", "nats://127.0.0.1:4222")
        scheme, host, port, _, _ = N._server_parts(url)
        io_timeout = IntegrationHelpers.integration_timeout()

        if scheme == "nats"
            proxy = IntegrationHelpers.start_tcp_proxy(host, port)
            client = connect(proxy.url;
                             connect_timeout=IntegrationHelpers.integration_connect_timeout(),
                             ping_interval=0.2,
                             max_outstanding_pings=1,
                             reconnect_wait=0.05,
                             reconnect_jitter=0.0,
                             max_reconnect_attempts=-1,
                             pending_size=4 * 1024 * 1024,
                             sub_pending_msgs_limit=4096,
                             record_stats=true)
            sub = Ref{Any}(nothing)
            task_errors = Channel{Any}(128)
            stop = Ref(false)
            published = Ref(0)
            received = Ref(0)
            try
                subject = "natter.stress.$(randstring(10))"
                sub[] = subscribe(client, subject; pending_msgs_limit=4096)
                deadline = time() + IntegrationHelpers.stress_seconds()

                consumer_task = Threads.@spawn begin
                    try
                        while !stop[]
                            try
                                take!(sub[]; timeout=0.1)
                                received[] += 1
                            catch err
                                if err isa TimeoutError
                                    continue
                                elseif err isa ConnectionClosedError && stop[]
                                    break
                                else
                                    put!(task_errors, err)
                                    break
                                end
                            end
                        end
                    catch err
                        put!(task_errors, err)
                    end
                end

                publisher_task = Threads.@spawn begin
                    try
                        while published[] == 0 || time() < deadline
                            published[] += 1
                            publish(client, subject, "msg-$(published[])"; mode=PublishMode.REPLAYABLE)
                            sleep(0.005)
                        end
                    catch err
                        put!(task_errors, err)
                    end
                end

                chaos_task = Threads.@spawn begin
                    try
                        drop_count = 0
                        while drop_count == 0 || time() < deadline
                            sleep(0.4)
                            connected_before_drop = timedwait(5.0; pollint=0.02) do
                                status(client) == ConnectionStatus.CONNECTED
                            end
                            if connected_before_drop == :timed_out
                                put!(task_errors, ErrorException("stress client did not become connected before drop"))
                                break
                            end
                            expected_reconnects = stats(client).reconnects + 1
                            proxy.pause_new_connections()
                            proxy.drop_connections()
                            if !ChaosTestHelpers.wait_reconnecting(client; timeout=2.0)
                                put!(task_errors, ErrorException("stress client did not enter reconnecting state"))
                                break
                            end
                            sleep(0.05)
                            proxy.release()
                            if !ChaosTestHelpers.wait_reconnected(client, expected_reconnects; timeout=5.0)
                                put!(task_errors, ErrorException("stress client did not reconnect"))
                                break
                            end
                            drop_count += 1
                        end
                    catch err
                        put!(task_errors, err)
                    finally
                        proxy.release()
                    end
                end

                wait(publisher_task)
                wait(chaos_task)
                @test ChaosTestHelpers.wait_reconnected(client, stats(client).reconnects;
                                                        timeout=max(io_timeout, 5.0))
                flush(client; timeout=io_timeout)
                sleep(0.2)
                stop[] = true
                close(sub[])
                wait(consumer_task)

                if isready(task_errors)
                    throw(take!(task_errors))
                end
                @test published[] > 0
                @test received[] > 0
                @test stats(client).reconnects >= 1
            finally
                stop[] = true
                try
                    try
                        !isnothing(sub[]) && !sub[].closed && close(sub[])
                    finally
                        close(client)
                    end
                finally
                    proxy.stop()
                end
            end
        else
            @info "Skipping reconnect stress workload; NATTER_URL must use nats:// for the local proxy."
        end
    else
        @info "Skipping reconnect stress workload; set NATTER_RUN_INTEGRATION=true and NATTER_RUN_STRESS=true to enable it."
    end
end

@testitem "real nats-server stress ordered push reset cleanup" setup=[IntegrationHelpers, ChaosTestHelpers] begin
    using Natter
    using Natter.JetStream
    using Natter.KeyValue
    using Random

    const N = Natter

    function no_ordered_tasks(psub)
        @lock psub.close_lock isempty(psub.ordered_cleanup_tasks)
    end

    function ordered_reset_clear(handler)
        @lock handler.lock !handler.ordered_resetting
    end

    if ChaosTestHelpers.stress_enabled() && get(ENV, "NATTER_RUN_JETSTREAM", "false") == "true"
        url = get(ENV, "NATTER_URL", "nats://127.0.0.1:4222")
        io_timeout = max(IntegrationHelpers.integration_timeout(), 10.0)
        client = connect(url; connect_timeout=IntegrationHelpers.integration_connect_timeout(),
                         ping_interval=0.5,
                         max_outstanding_pings=2,
                         record_stats=true)
        js = jetstream(client; timeout=io_timeout)
        stream = "NATTER_STRESS_PUSH_$(randstring(8))"
        subject = "natter.stress.push.$(randstring(8))"
        stream_created = Ref(false)
        ordered_sub = Ref{Any}(nothing)
        try
            stream_create(js, StreamConfig(name=stream, subjects=[subject], storage=StorageType.MEMORY))
            stream_created[] = true
            deadline = time() + IntegrationHelpers.stress_seconds()
            iterations = 0

            while iterations == 0 || time() < deadline
                ordered_sub[] = push_subscribe(js, subject; stream, ordered=true,
                                               config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW),
                                               timeout=io_timeout)
                psub = ordered_sub[]::PushSubscription
                handler = psub.control_handler::N._JetStreamPushControlHandler

                js_publish(js, subject, "ordered-reset-$iterations"; stream, timeout=io_timeout)
                msg = N.take!(psub; timeout=io_timeout)
                N._request_ordered_push_reset!(handler, metadata(msg).stream_sequence + 1)

                @test timedwait(io_timeout; pollint=0.005) do
                    !isnothing(psub.ordered_reset_task)
                end != :timed_out

                close(psub; timeout=io_timeout)
                ordered_sub[] = nothing

                @test no_ordered_tasks(psub)
                @test ordered_reset_clear(handler)
                @test timedwait(io_timeout; pollint=0.02) do
                    isempty(consumer_list(js, stream; timeout=io_timeout))
                end != :timed_out

                iterations += 1
            end

            @test iterations > 0
        finally
            try
                if !isnothing(ordered_sub[]) && status(client) == ConnectionStatus.CONNECTED
                    close(ordered_sub[]; timeout=io_timeout)
                end
            finally
                try
                    stream_created[] && status(client) == ConnectionStatus.CONNECTED &&
                        stream_delete(js, stream; timeout=io_timeout)
                finally
                    close(client)
                end
            end
        end
    else
        @info "Skipping ordered push reset stress workload; set NATTER_RUN_INTEGRATION=true, NATTER_RUN_JETSTREAM=true, and NATTER_RUN_STRESS=true to enable it."
    end
end

@testitem "real nats-server stress KeyValue watcher reconnect workload" setup=[IntegrationHelpers, ChaosTestHelpers] begin
    using Natter
    using Natter.JetStream
    using Natter.KeyValue
    using Random

    const N = Natter

    function drain_initial!(watcher, timeout::Real)
        count = 0
        while true
            update = N._kv_take!(watcher, timeout)
            update isa KeyValueWatchInitialDone && return count
            @test update isa KeyValueEntry
            count += 1
        end
    end

    function take_keys!(watcher, expected::Int, timeout::Real)
        keys = Set{String}()
        deadline = time() + timeout
        while length(keys) < expected
            update = N._kv_take!(watcher, max(0.001, deadline - time()))
            update isa KeyValueEntry && push!(keys, update.key)
        end
        keys
    end

    if ChaosTestHelpers.stress_enabled() && get(ENV, "NATTER_RUN_JETSTREAM", "false") == "true"
        url = get(ENV, "NATTER_URL", "nats://127.0.0.1:4222")
        scheme, host, port, _, _ = N._server_parts(url)
        io_timeout = max(IntegrationHelpers.integration_timeout(), 10.0)
        key_count = max(1, IntegrationHelpers.stress_keys())
        watcher_count = max(1, IntegrationHelpers.stress_watchers())
        channel_size = max(64, 3 * key_count + 64)

        if scheme == "nats"
            proxy = IntegrationHelpers.start_tcp_proxy(host, port)
            client = connect(proxy.url;
                             connect_timeout=IntegrationHelpers.integration_connect_timeout(),
                             ping_interval=0.2,
                             max_outstanding_pings=1,
                             reconnect_wait=0.05,
                             reconnect_jitter=0.0,
                             max_reconnect_attempts=-1,
                             sub_pending_msgs_limit=max(8192, 3 * key_count + 1024),
                             sub_pending_bytes_limit=256 * 1024 * 1024,
                             record_stats=true)
            js = jetstream(client; timeout=io_timeout)
            bucket = "NATTERSTRESSKV_$(randstring(8))"
            kv = Ref{Union{Nothing,KeyValueBucket}}(nothing)
            watchers = Any[]
            try
                kv[] = kv_create(js, bucket; history=5, storage="memory", direct=true,
                                 timeout=io_timeout)

                for i in 1:key_count
                    group = isodd(i) ? "a" : "b"
                    kv_put(kv[], "group.$group.key.$i", "seed-$i"; timeout=io_timeout)
                end

                specs = [
                    (filter=">", expected=key_count),
                    (filter="group.a.>", expected=(key_count + 1) ÷ 2),
                    (filter="group.b.>", expected=key_count ÷ 2),
                ]
                for i in 1:watcher_count
                    spec = specs[mod1(i, length(specs))]
                    watcher = kv_watch(kv[]; key=spec.filter, meta_only=true,
                                       channel_size, timeout=io_timeout)
                    push!(watchers, watcher)
                    @test drain_initial!(watcher, io_timeout) == spec.expected
                end

                post_watcher = kv_watch(kv[]; key="post.>", updates_only=true,
                                        meta_only=true, channel_size, timeout=io_timeout)
                push!(watchers, post_watcher)
                ignore_delete_watcher = kv_watch(kv[]; key="delete.>", updates_only=true,
                                                 ignore_deletes=true, meta_only=true,
                                                 channel_size, timeout=io_timeout)
                push!(watchers, ignore_delete_watcher)

                expected_reconnects = stats(client).reconnects + 1
                proxy.pause_new_connections()
                proxy.drop_connections()
                @test ChaosTestHelpers.wait_reconnecting(client; timeout=max(io_timeout, 2.0))
                proxy.release()
                @test ChaosTestHelpers.wait_reconnected(client, expected_reconnects;
                                                        timeout=max(io_timeout, 10.0))

                for i in 1:key_count
                    kv_put(kv[], "post.key.$i", "post-$i"; timeout=io_timeout)
                end
                received = take_keys!(post_watcher, key_count, max(io_timeout, 20.0))
                @test length(received) == key_count
                @test "post.key.1" in received
                @test "post.key.$key_count" in received

                kv_put(kv[], "delete.tracked", "before-delete"; timeout=io_timeout)
                put_entry = N._kv_take!(ignore_delete_watcher, io_timeout)
                @test put_entry isa KeyValueEntry
                @test put_entry.key == "delete.tracked"
                @test put_entry.operation == KeyValueOperation.PUT
                kv_delete(kv[], "delete.tracked"; timeout=io_timeout)
                @test timedwait(0.25; pollint=0.01) do
                    isready(ignore_delete_watcher.updates)
                end == :timed_out
            finally
                try
                    if status(client) == ConnectionStatus.CONNECTED
                        for watcher in reverse(watchers)
                            close(watcher; timeout=io_timeout)
                        end
                        !isnothing(kv[]) && kv_delete_bucket(kv[]; timeout=io_timeout)
                    end
                finally
                    try
                        close(client)
                    finally
                        proxy.stop()
                    end
                end
            end
        else
            @info "Skipping KeyValue watcher stress workload; NATTER_URL must use nats:// for the local proxy."
        end
    else
        @info "Skipping KeyValue watcher stress workload; set NATTER_RUN_INTEGRATION=true, NATTER_RUN_JETSTREAM=true, and NATTER_RUN_STRESS=true to enable it."
    end
end

@testitem "real nats-server stress KeyValue large keys and history" setup=[IntegrationHelpers, ChaosTestHelpers] begin
    using Natter
    using Natter.JetStream
    using Natter.KeyValue
    using Random

    if ChaosTestHelpers.stress_enabled() && get(ENV, "NATTER_RUN_JETSTREAM", "false") == "true"
        url = get(ENV, "NATTER_URL", "nats://127.0.0.1:4222")
        io_timeout = max(IntegrationHelpers.integration_timeout(), 10.0)
        key_count = max(1, IntegrationHelpers.stress_keys())
        client = connect(url; connect_timeout=IntegrationHelpers.integration_connect_timeout(),
                         ping_interval=0.5,
                         max_outstanding_pings=2,
                         record_stats=true)
        js = jetstream(client; timeout=io_timeout)
        bucket = "NATTERSTRESSKEYS_$(randstring(8))"
        kv = Ref{Union{Nothing,KeyValueBucket}}(nothing)
        try
            kv[] = kv_create(js, bucket; history=5, storage="memory", direct=true,
                             timeout=io_timeout)

            history_revisions = Int[]
            for i in 1:5
                push!(history_revisions,
                      kv_put(kv[], "history.key", "history-$i"; timeout=io_timeout))
            end

            for i in 1:key_count
                kv_put(kv[], "scan.key.$i", "scan-$i"; timeout=io_timeout)
            end
            key_count >= 1 && kv_delete(kv[], "scan.key.1"; timeout=io_timeout)
            key_count >= 2 && kv_purge(kv[], "scan.key.2"; timeout=io_timeout)

            history = kv_history(kv[], "history.key"; batch=2, timeout=io_timeout)
            @test [entry.revision for entry in history] == history_revisions
            @test [String(entry) for entry in history] == ["history-$i" for i in 1:5]

            keys = Set(kv_keys(kv[]; timeout=max(io_timeout, 20.0)))
            @test "history.key" in keys
            if key_count >= 1
                @test !("scan.key.1" in keys)
            end
            if key_count >= 2
                @test !("scan.key.2" in keys)
            end
            if key_count >= 3
                @test "scan.key.3" in keys
            end
            @test length(keys) >= max(1, key_count - min(key_count, 2) + 1)
        finally
            try
                !isnothing(kv[]) && status(client) == ConnectionStatus.CONNECTED &&
                    kv_delete_bucket(kv[]; timeout=io_timeout)
            finally
                close(client)
            end
        end
    else
        @info "Skipping KeyValue large keys and history stress workload; set NATTER_RUN_INTEGRATION=true, NATTER_RUN_JETSTREAM=true, and NATTER_RUN_STRESS=true to enable it."
    end
end
