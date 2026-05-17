using TestItems

@testmodule ChaosTestHelpers begin
    using Natter

    const N = Natter

    chaos_enabled() =
        get(ENV, "NATTER_RUN_INTEGRATION", "false") == "true" &&
        get(ENV, "NATTER_RUN_CHAOS", "false") == "true"

    stress_enabled() =
        get(ENV, "NATTER_RUN_INTEGRATION", "false") == "true" &&
        get(ENV, "NATTER_RUN_STRESS", "false") == "true"

    function wait_reconnecting(client; timeout::Real)
        timedwait(timeout; pollint=0.01) do
            status(client) == N.ConnectionStatus.RECONNECTING
        end != :timed_out
    end

    function wait_reconnected(client, expected_reconnects::Int; timeout::Real)
        timedwait(timeout; pollint=0.02) do
            status(client) == N.ConnectionStatus.CONNECTED &&
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
                @test String(next(sub[]; timeout=io_timeout)) == "before"

                for i in 1:IntegrationHelpers.chaos_iterations()
                    expected_reconnects = stats(client).reconnects + 1
                    proxy.pause_new_connections()
                    proxy.drop_connections()
                    @test ChaosTestHelpers.wait_reconnecting(client; timeout=max(io_timeout, 2.0))

                    publish(client, subject, "during-$i")
                    proxy.release()
                    @test ChaosTestHelpers.wait_reconnected(client, expected_reconnects;
                                                            timeout=max(io_timeout, 5.0))

                    flush(client; timeout=io_timeout)
                    @test String(next(sub[]; timeout=io_timeout)) == "during-$i"
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

                @test startswith(String(next(sub; timeout=io_timeout)), "msg-")

                healthy_subject = "$subject.healthy"
                healthy = subscribe(client, healthy_subject)
                try
                    IntegrationHelpers.publish_and_flush(client, healthy_subject, "ok"; timeout=io_timeout)
                    @test String(next(healthy; timeout=io_timeout)) == "ok"
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
                fetch_task = @async fetch(psub[], 1; timeout=max(io_timeout, 5.0), heartbeat=0)
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
                        !isnothing(psub[]) && status(client) == N.ConnectionStatus.CONNECTED && close(psub[])
                        stream_created[] && status(client) == N.ConnectionStatus.CONNECTED &&
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
            kv = Ref{Union{Nothing,KeyValue}}(nothing)
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
                        !isnothing(watcher[]) && status(client) == N.ConnectionStatus.CONNECTED && close(watcher[])
                        !isnothing(kv[]) && status(client) == N.ConnectionStatus.CONNECTED &&
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

                consumer_task = @async begin
                    try
                        while !stop[]
                            try
                                next(sub[]; timeout=0.1)
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

                publisher_task = @async begin
                    try
                        while published[] == 0 || time() < deadline
                            published[] += 1
                            publish(client, subject, "msg-$(published[])")
                            sleep(0.005)
                        end
                    catch err
                        put!(task_errors, err)
                    end
                end

                chaos_task = @async begin
                    try
                        drop_count = 0
                        while drop_count == 0 || time() < deadline
                            sleep(0.4)
                            connected_before_drop = timedwait(5.0; pollint=0.02) do
                                status(client) == N.ConnectionStatus.CONNECTED
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
