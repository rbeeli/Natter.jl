using TestItems

@testitem "real nats-server core integration" setup=[TestHelpers] begin
    using Natter
    using Dates
    using Random

    const N = Natter

    if get(ENV, "NATTER_RUN_INTEGRATION", "false") == "true"
        url = get(ENV, "NATTER_URL", "nats://127.0.0.1:4222")
        reconnected = Ref(false)
        client = connect(url; ping_interval=2.0, max_outstanding_pings=2,
                         reconnect_wait=0.05, max_reconnect_attempts=20,
                         reconnected_cb=() -> (reconnected[] = true))
        try
            subject = "natter.test.$(randstring(10))"
            sub = subscribe(client, subject)
            publish(client, subject, "hello")
            msg = next(sub; timeout=2.0)
            @test String(msg) == "hello"

            service = subscribe(client, "$subject.req") do req
                publish(client, req.reply, uppercase(String(req.data)))
            end
            response = request(client, "$subject.req", "ok"; timeout=2.0)
            @test String(response) == "OK"
            async_response = fetch(request_async(client, "$subject.req", "async"; timeout=2.0))
            @test String(async_response) == "ASYNC"
            close(service)

            @test_throws NoRespondersError request(client, "$subject.none", ""; timeout=2.0)
            async_no_responders = TestHelpers.thrown_exception() do
                fetch(request_async(client, "$subject.none", ""; timeout=2.0))
            end
            @test async_no_responders isa CapturedException
            @test async_no_responders.ex isa NoRespondersError

            slow_started = Channel{Bool}(1)
            slow_service = subscribe(client, "$subject.slow") do req
                put!(slow_started, true)
                sleep(0.3)
                publish(client, req.reply, "late")
            end
            before_timeout_sids = sort(collect(keys(client.subscriptions)))
            @test_throws TimeoutError request(client, "$subject.slow", "slow"; timeout=0.05)
            @test timedwait(1.0; pollint=0.01) do
                isready(slow_started)
            end != :timed_out
            @test sort(collect(keys(client.subscriptions))) == before_timeout_sids
            sleep(0.4)
            @test sort(collect(keys(client.subscriptions))) == before_timeout_sids
            close(slow_service)

            async_sub = fetch(subscribe_async(client, "$subject.async"))
            fetch(publish_async(client, "$subject.async", "from task"))
            fetch(flush_async(client; timeout=2.0))
            @test String(fetch(next_async(async_sub; timeout=2.0))) == "from task"
            fetch(unsubscribe_async(async_sub))

            close(client.socket)
            result = timedwait(5.0; pollint=0.05) do
                reconnected[] && status(client) == N.ConnectionStatus.CONNECTED
            end
            @test result != :timed_out
            publish(client, subject, "after reconnect")
            @test String(next(sub; timeout=2.0)) == "after reconnect"

            drain(sub; timeout=2.0)
            @test sub.closed

            started = Channel{Bool}(1)
            release = Channel{Bool}(1)
            drained = Ref(false)
            callback_sub = subscribe(client, "$subject.callback-drain") do msg
                put!(started, true)
                take!(release)
            end
            publish(client, "$subject.callback-drain", "work")
            started_result = timedwait(2.0; pollint=0.01) do
                isready(started)
            end
            @test started_result != :timed_out
            @test take!(started) == true
            drain_task = @async begin
                drain(callback_sub; timeout=2.0)
                drained[] = true
            end
            sleep(0.1)
            @test !drained[]
            put!(release, true)
            wait(drain_task)
            @test drained[]

            drain_client = connect(url; ping_interval=2.0, max_outstanding_pings=2)
            try
                drain_sub = subscribe(drain_client, "$subject.drain")
                publish(drain_client, "$subject.drain", "drain")
                @test String(next(drain_sub; timeout=2.0)) == "drain"
                drain(drain_client; timeout=2.0)
                @test status(drain_client) == N.ConnectionStatus.CLOSED
            finally
                close(drain_client)
            end
        finally
            close(client)
        end
    else
        @info "Skipping real nats-server core integration tests; set NATTER_RUN_INTEGRATION=true to enable them."
    end
end

@testitem "real nats-server reconnect server-pool failover" begin
    using Natter
    using Random
    using Sockets

    const N = Natter

    function _proxy_close(resource, operation::String)
        try
            close(resource)
        catch err
            @debug "Natter integration proxy cleanup failed" operation exception=(err, catch_backtrace())
        end
        nothing
    end

    function _remember_proxy_resource!(resources::Vector{Any}, resource_lock::ReentrantLock, resource)
        lock(resource_lock)
        try
            push!(resources, resource)
        finally
            unlock(resource_lock)
        end
        resource
    end

    function _proxy_resources_snapshot(resources::Vector{Any}, resource_lock::ReentrantLock)
        lock(resource_lock)
        try
            return copy(resources)
        finally
            unlock(resource_lock)
        end
    end

    function _proxy_pump(from, to)
        try
            while true
                data = readavailable(from)
                isempty(data) && break
                write(to, data)
                flush(to)
            end
        catch err
            @debug "Natter integration proxy pump stopped" exception=(err, catch_backtrace())
        finally
            _proxy_close(from, "close proxy source")
            _proxy_close(to, "close proxy destination")
        end
        nothing
    end

    function _start_tcp_proxy(target_host::AbstractString, target_port::Int; released::Bool=true)
        server = Sockets.listen(ip"127.0.0.1", 0)
        _, proxy_port = Sockets.getsockname(server)
        resources = Any[server]
        resource_lock = ReentrantLock()
        release_gate = Channel{Bool}(1)
        release_state = Ref(released)

        function release!()
            if !release_state[]
                release_state[] = true
                isready(release_gate) || put!(release_gate, true)
            end
            nothing
        end

        accept_task = @async begin
            while true
                client_sock = try
                    Sockets.accept(server)
                catch err
                    @debug "Natter integration proxy accept stopped" exception=(err, catch_backtrace())
                    break
                end
                _remember_proxy_resource!(resources, resource_lock, client_sock)

                if !release_state[]
                    try
                        take!(release_gate)
                    catch err
                        @debug "Natter integration proxy release wait stopped" exception=(err, catch_backtrace())
                        _proxy_close(client_sock, "close unreleased proxy client")
                        break
                    end
                end

                server_sock = try
                    Sockets.connect(String(target_host), target_port)
                catch err
                    @debug "Natter integration proxy target connect failed" exception=(err, catch_backtrace())
                    _proxy_close(client_sock, "close proxy client after target connect failure")
                    continue
                end
                _remember_proxy_resource!(resources, resource_lock, server_sock)

                @async _proxy_pump(client_sock, server_sock)
                @async _proxy_pump(server_sock, client_sock)
            end
        end

        function stop!()
            release!()
            for resource in reverse(_proxy_resources_snapshot(resources, resource_lock))
                _proxy_close(resource, "stop proxy")
            end
            timedwait(0.5; pollint=0.01) do
                istaskdone(accept_task)
            end
            nothing
        end

        (; url="nats://127.0.0.1:$(Int(proxy_port))", release=release!, stop=stop!)
    end

    if get(ENV, "NATTER_RUN_INTEGRATION", "false") == "true"
        url = get(ENV, "NATTER_URL", "nats://127.0.0.1:4222")
        scheme, host, port, _, _ = N._server_parts(url)

        if scheme == "nats"
            primary = _start_tcp_proxy(host, port)
            secondary = _start_tcp_proxy(host, port; released=false)
            disconnected = Ref(false)
            reconnected = Ref(false)
            client = N.connect([primary.url, secondary.url];
                               connect_timeout=0.5,
                               ping_interval=2.0,
                               max_outstanding_pings=2,
                               reconnect_wait=0.05,
                               reconnect_jitter=0.0,
                               max_reconnect_attempts=40,
                               disconnected_cb=() -> (disconnected[] = true),
                               reconnected_cb=() -> (reconnected[] = true))
            try
                @test connected_url(client) == primary.url

                subject = "natter.failover.$(randstring(10))"
                sub = subscribe(client, subject)
                publish(client, subject, "before failover")
                @test String(next(sub; timeout=2.0)) == "before failover"

                primary.stop()
                result = timedwait(2.0; pollint=0.01) do
                    disconnected[] || status(client) == N.ConnectionStatus.RECONNECTING
                end
                @test result != :timed_out

                publish(client, subject, "during failover")
                secondary.release()

                result = timedwait(5.0; pollint=0.02) do
                    reconnected[] &&
                        status(client) == N.ConnectionStatus.CONNECTED &&
                        connected_url(client) == secondary.url
                end
                @test result != :timed_out
                @test String(next(sub; timeout=2.0)) == "during failover"
                @test stats(client).reconnects >= 1
            finally
                close(client)
                primary.stop()
                secondary.stop()
            end
        else
            @info "Skipping reconnect server-pool failover integration test; NATTER_URL must use nats:// for the local proxy."
        end
    else
        @info "Skipping reconnect server-pool failover integration test; set NATTER_RUN_INTEGRATION=true to enable it."
    end
end

@testitem "real nats-server JetStream integration" setup=[TestHelpers] begin
    using Natter
    using Dates
    using Random

    if get(ENV, "NATTER_RUN_INTEGRATION", "false") == "true" && get(ENV, "NATTER_RUN_JETSTREAM", "false") == "true"
        url = get(ENV, "NATTER_URL", "nats://127.0.0.1:4222")
        client = connect(url; ping_interval=2.0, max_outstanding_pings=2)
        js = jetstream(client)
        stream = "NATTER_$(randstring(8))"
        subject_root = "natter.js.$(randstring(8))"
        subject = "$subject_root.main"
        other_subject = "$subject_root.other"
        stream_created = Ref(false)
        kv = Ref{Union{Nothing,KeyValue}}(nothing)
        try
            sinfo = stream_create(js, StreamConfig(name=stream, subjects=["$subject_root.*"], storage=StorageType.MEMORY, allow_direct=true))
            stream_created[] = true
            @test sinfo.config isa StreamConfig
            @test sinfo.config.storage == StorageType.MEMORY
            pa = js_publish(js, subject, "payload"; stream=stream, headers=Headers("X-Test" => ["direct"]))
            @test pa.stream == stream
            @test pa.seq == 1
            js_publish(js, other_subject, "other"; stream=stream)
            js_publish(js, subject, "payload2"; stream=stream)
            direct_seq = stream_message_get(js, stream; seq=pa.seq, direct=true)
            @test direct_seq.subject == subject
            @test String(direct_seq) == "payload"
            @test header(direct_seq, "X-Test") == "direct"
            direct_subject = stream_message_get(js, stream; subject, direct=true)
            @test direct_subject.subject == subject
            @test String(direct_subject) == "payload2"
            direct_next = stream_message_get(js, stream; seq=pa.seq + 1, subject, next_by_subject=true, direct=true)
            @test direct_next.subject == subject
            @test String(direct_next) == "payload2"
            pa_async = fetch(js_publish_async(js, subject, "async-payload"; stream=stream))
            @test pa_async.stream == stream
            async_direct = fetch(stream_message_get_async(js, stream; seq=pa_async.seq, direct=true))
            @test String(async_direct) == "async-payload"

            durable = "DUR_$(randstring(6))"
            psub = pull_subscribe(js, subject; stream=stream, durable, config=ConsumerConfig(max_ack_pending=33))
            cinfo = consumer_info(js, stream, durable)
            @test cinfo.config isa ConsumerConfig
            @test cinfo.config.durable_name == durable
            @test cinfo.config.max_ack_pending == 33
            @test_throws JetStreamError consumer_create(js, stream, ConsumerConfig(
                name=durable,
                durable_name=durable,
                filter_subject=subject,
                max_ack_pending=44,
            ))
            @test consumer_info(js, stream, durable).config.max_ack_pending == 33
            @test_throws ArgumentError pull_subscribe(js, subject; stream=stream, durable, config=ConsumerConfig(max_ack_pending=44))
            @test consumer_info(js, stream, durable).config.max_ack_pending == 33
            missing_durable = "MISSING_$(randstring(6))"
            @test_throws JetStreamError consumer_update(js, stream, ConsumerConfig(
                name=missing_durable,
                durable_name=missing_durable,
                filter_subject=subject,
                ack_policy=AckPolicy.EXPLICIT,
            ))
            @test_throws ArgumentError fetch(psub, 1; timeout=0.0)
            @test endswith(psub.deliver, ".*")
            msgs = fetch(psub, 1; timeout=2.0)
            @test length(msgs) == 1
            @test String(first(msgs)) == "payload"
            ack(first(msgs))
            close(psub)
            bound_psub = pull_subscribe(js, subject; stream=stream, durable)
            try
                @test bound_psub.consumer == durable
                @test consumer_info(js, stream, durable).config.max_ack_pending == 33
            finally
                close(bound_psub)
            end

            async_durable = "DURASYNC_$(randstring(6))"
            async_psub = fetch(pull_subscribe_async(js, subject; stream=stream, durable=async_durable))
            async_msgs = fetch(fetch_async(async_psub, 1; timeout=2.0))
            @test length(async_msgs) == 1
            fetch(ack_async(first(async_msgs)))
            fetch(close_async(async_psub))

            ack_none_subject = "$subject_root.ack-none"
            ack_none_errors = Channel{Any}(4)
            ack_none_client = connect(url; ping_interval=2.0, max_outstanding_pings=2,
                                      error_cb=err -> put!(ack_none_errors, err))
            try
                ack_none_js = jetstream(ack_none_client)
                received = Channel{String}(1)
                ack_none_sub = push_subscribe(ack_none_js, ack_none_subject; stream=stream,
                                              callback=msg -> put!(received, String(msg)),
                                              config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW,
                                                                    ack_policy=AckPolicy.NONE))
                try
                    flush(ack_none_client; timeout=2.0)
                    js_publish(js, ack_none_subject, "ack-none"; stream=stream)
                    @test timedwait(2.0; pollint=0.01) do
                        isready(received)
                    end != :timed_out
                    @test take!(received) == "ack-none"
                    @test timedwait(0.2; pollint=0.01) do
                        isready(ack_none_errors)
                    end == :timed_out
                finally
                    close(ack_none_sub)
                end
            finally
                close(ack_none_client)
            end

            heartbeat_subject = "$subject_root.heartbeat"
            heartbeat_sub = push_subscribe(js, heartbeat_subject; stream=stream,
                                           config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW,
                                                                 ack_policy=AckPolicy.EXPLICIT,
                                                                 idle_heartbeat=0.1))
            try
                @test_throws TimeoutError next(heartbeat_sub.sub; timeout=0.35)
            finally
                close(heartbeat_sub)
            end

            flow_subject = "$subject_root.flow"
            flow_sub = push_subscribe(js, flow_subject; stream=stream,
                                      config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW,
                                                            ack_policy=AckPolicy.EXPLICIT,
                                                            flow_control=true,
                                                            idle_heartbeat=0.1))
            try
                js_publish(js, flow_subject, "flow-control"; stream=stream)
                msg = next(flow_sub.sub; timeout=2.0)
                @test msg.subject == flow_subject
                @test String(msg) == "flow-control"
                ack(msg)
            finally
                close(flow_sub)
            end

            bound_push_subject = "$subject_root.bound-push"
            bound_push_consumer = "PUSHBOUND_$(randstring(6))"
            bound_push_sub = push_subscribe(js, bound_push_subject; stream=stream, durable=bound_push_consumer,
                                            config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW,
                                                                  ack_policy=AckPolicy.EXPLICIT))
            try
                flush(client; timeout=2.0)
                @test timedwait(2.0; pollint=0.01) do
                    consumer_info(js, stream, bound_push_consumer).push_bound
                end != :timed_out
                @test_throws ArgumentError push_subscribe(js, bound_push_subject; stream=stream, durable=bound_push_consumer)
            finally
                close(bound_push_sub)
            end

            queue_subject = "$subject_root.queue"
            queue_consumer = "QUEUE_$(randstring(6))"
            queue_sub = push_subscribe(js, queue_subject; stream=stream,
                                       config=ConsumerConfig(name=queue_consumer,
                                                             durable_name=queue_consumer,
                                                             deliver_group="workers",
                                                             deliver_policy=DeliverPolicy.NEW,
                                                             ack_policy=AckPolicy.EXPLICIT))
            try
                flush(client; timeout=2.0)
                js_publish(js, queue_subject, "queue-config"; stream=stream)
                msg = next(queue_sub.sub; timeout=2.0)
                @test msg.subject == queue_subject
                @test String(msg) == "queue-config"
                ack(msg)
            finally
                close(queue_sub)
            end

            queue_only_subject = "$subject_root.queue-only"
            queue_group = "QONLY_$(randstring(6))"
            queue_only_sub1 = push_subscribe(js, queue_only_subject; stream=stream,
                                             queue=queue_group,
                                             config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW,
                                                                   ack_policy=AckPolicy.EXPLICIT))
            queue_only_sub2 = push_subscribe(js, queue_only_subject; stream=stream,
                                             queue=queue_group,
                                             config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW,
                                                                   ack_policy=AckPolicy.EXPLICIT))
            try
                @test queue_only_sub1.consumer == queue_group
                @test queue_only_sub2.consumer == queue_group
                queue_only_info = consumer_info(js, stream, queue_group)
                @test queue_only_info.config.durable_name == queue_group
                @test queue_only_info.config.deliver_group == queue_group
                @test_throws ArgumentError push_subscribe(js, queue_only_subject; stream=stream, durable=queue_group)
                @test_throws ArgumentError push_subscribe(js, queue_only_subject; stream=stream, durable=queue_group, queue="other")

                flush(client; timeout=2.0)
                payloads = ["queue-only-$i" for i in 1:4]
                for payload in payloads
                    js_publish(js, queue_only_subject, payload; stream=stream)
                end

                received = String[]
                deadline = time() + 3.0
                while length(received) < length(payloads) && time() < deadline
                    for sub in (queue_only_sub1.sub, queue_only_sub2.sub)
                        try
                            msg = next(sub; timeout=0.05)
                            push!(received, String(msg))
                            ack(msg)
                        catch err
                            err isa TimeoutError || rethrow()
                        end
                    end
                end
                @test sort(received) == payloads
                @test_throws TimeoutError next(queue_only_sub1.sub; timeout=0.1)
                @test_throws TimeoutError next(queue_only_sub2.sub; timeout=0.1)
            finally
                close(queue_only_sub1)
                close(queue_only_sub2)
            end

            bucket = "NATTERKV_$(randstring(8))"
            kv[] = kv_create(js, bucket; history=5, storage="memory", direct=true)
            try
                @test kv[].direct
                empty_status = kv_status(kv[])
                @test empty_status isa KeyValueStatus
                @test empty_status.bucket == bucket
                @test empty_status.stream == "KV_$bucket"
                @test empty_status.values == 0
                @test empty_status.history == 5
                @test empty_status.storage == StorageType.MEMORY
                @test empty_status.direct
                @test fetch(kv_status_async(kv[])).bucket == bucket
                @test_throws KeyValueKeyNotFoundError kv_get(kv[], "missing")
                pa2 = kv_put(kv[], "alpha", "one")
                @test pa2.seq >= 1
                alpha = kv_get(kv[], "alpha")
                @test alpha isa KeyValueEntry
                @test alpha.bucket == bucket
                @test alpha.key == "alpha"
                @test alpha.revision == pa2.seq
                @test alpha.created isa Dates.DateTime
                @test alpha.delta == 0
                @test alpha.operation == KeyValueOperation.PUT
                @test String(alpha) == "one"
                revision_alpha = kv_get(kv[], "alpha"; revision=pa2.seq)
                @test revision_alpha.revision == pa2.seq
                @test String(revision_alpha) == "one"
                kv_update(kv[], "alpha", "two", pa2.seq)
                updated_alpha = kv_get(kv[], "alpha")
                @test updated_alpha.revision > pa2.seq
                @test String(updated_alpha) == "two"
                alpha_history = kv_history(kv[], "alpha"; batch=2)
                @test length(alpha_history) >= 2
                @test all(entry -> entry isa KeyValueEntry && entry.key == "alpha", alpha_history)
                @test alpha_history[end].revision == updated_alpha.revision
                kv_put(kv[], "watch-initial", "ready")
                initial_updates = Channel{KeyValueEntry}(1)
                initial_watcher = kv_watch(kv[]; key="watch-initial") do entry
                    put!(initial_updates, entry)
                end
                try
                    @test timedwait(2.0; pollint=0.01) do
                        isready(initial_updates)
                    end != :timed_out
                    initial = take!(initial_updates)
                    @test initial.key == "watch-initial"
                    @test initial.operation == KeyValueOperation.PUT
                    @test String(initial) == "ready"
                finally
                    close(initial_watcher)
                end
                pa3 = fetch(kv_put_async(kv[], "beta", "async-one"))
                @test pa3.seq >= 1
                @test String(fetch(kv_get_async(kv[], "beta"))) == "async-one"
                @test_throws KeyValueWrongRevisionError kv_update(kv[], "beta", "wrong-revision", pa3.seq + 100)
                @test_throws KeyValueWrongRevisionError kv_delete(kv[], "beta"; revision=pa3.seq + 100)
                updates = Channel{KeyValueEntry}(4)
                watcher = kv_watch(kv[]; key="gamma") do entry
                    put!(updates, entry)
                end
                try
                    kv_put(kv[], "gamma", "watched")
                    flush(client)
                    @test timedwait(2.0; pollint=0.01) do
                        isready(updates)
                    end != :timed_out
                    watched = take!(updates)
                    @test watched.key == "gamma"
                    @test watched.operation == KeyValueOperation.PUT
                    @test String(watched) == "watched"
                finally
                    close(watcher)
                end
                fetch(kv_delete_async(kv[], "beta"; revision=pa3.seq))
                async_deleted = TestHelpers.thrown_exception() do
                    fetch(kv_get_async(kv[], "beta"))
                end
                @test async_deleted isa CapturedException
                @test async_deleted.ex isa KeyValueKeyDeletedError
                purge_ack = kv_put(kv[], "purge-me", "value")
                @test_throws KeyValueWrongRevisionError kv_purge(kv[], "purge-me"; revision=purge_ack.seq + 100)
                fetch(kv_purge_async(kv[], "purge-me"; revision=purge_ack.seq))
                @test_throws KeyValueKeyDeletedError kv_get(kv[], "purge-me")
                kv_delete(kv[], "alpha")
                @test_throws KeyValueKeyDeletedError kv_get(kv[], "alpha")
                @test !("alpha" in kv_keys(kv[]))
                recreated = kv_create_key(kv[], "alpha", "three")
                @test recreated.seq > pa2.seq
                @test String(kv_get(kv[], "alpha")) == "three"
                @test "alpha" in kv_keys(kv[])
                @test_throws KeyValueKeyExistsError kv_create_key(kv[], "alpha", "duplicate")
                large_key = "large"
                kv_put(kv[], large_key, repeat("x", 8192))
                limited_client = connect(url; ping_interval=2.0, max_outstanding_pings=2, max_inbound_payload=4096)
                try
                    limited_kv = kv_open(jetstream(limited_client), bucket)
                    @test large_key in kv_keys(limited_kv)
                finally
                    close(limited_client)
                end
            finally
                isnothing(kv[]) || kv_delete_bucket(kv[])
            end
        finally
            try
                stream_created[] && stream_delete(js, stream)
            finally
                close(client)
            end
        end
    else
        @info "Skipping real nats-server JetStream integration tests; set NATTER_RUN_INTEGRATION=true and NATTER_RUN_JETSTREAM=true to enable them."
    end
end

@testitem "real nats-server JetStream fetch disconnect integration" begin
    using Natter
    using Random

    const N = Natter

    if get(ENV, "NATTER_RUN_INTEGRATION", "false") == "true" && get(ENV, "NATTER_RUN_JETSTREAM", "false") == "true"
        url = get(ENV, "NATTER_URL", "nats://127.0.0.1:4222")
        reconnected = Ref(false)
        client = connect(url; ping_interval=2.0, max_outstanding_pings=2,
                         reconnect_wait=0.05, max_reconnect_attempts=20,
                         reconnected_cb=() -> (reconnected[] = true))
        js = jetstream(client)
        stream = "NATTER_FETCH_$(randstring(8))"
        subject = "natter.fetch.$(randstring(8))"
        durable = "FETCH_$(randstring(6))"
        stream_created = Ref(false)
        psub = Ref{Any}(nothing)
        try
            stream_create(js, StreamConfig(name=stream, subjects=[subject], storage=StorageType.MEMORY))
            stream_created[] = true
            psub[] = pull_subscribe(js, subject; stream, durable)

            before_out = stats(client).out_msgs
            fetch_task = @async fetch(psub[], 1; timeout=5.0, heartbeat=0)
            @test timedwait(2.0; pollint=0.01) do
                stats(client).out_msgs > before_out
            end != :timed_out
            close(client.socket)

            err = try
                fetch(fetch_task)
                nothing
            catch caught
                caught isa TaskFailedException ? first(Base.current_exceptions(fetch_task)).exception : caught
            end
            @test err isa FetchDisconnectedError

            @test timedwait(5.0; pollint=0.02) do
                reconnected[] && status(client) == N.ConnectionStatus.CONNECTED
            end != :timed_out
        finally
            isnothing(psub[]) || close(psub[])
            stream_created[] && status(client) == N.ConnectionStatus.CONNECTED && stream_delete(js, stream)
            close(client)
        end
    else
        @info "Skipping real nats-server JetStream fetch disconnect integration test; set NATTER_RUN_INTEGRATION=true and NATTER_RUN_JETSTREAM=true to enable it."
    end
end

@testitem "real nats-server TLS first integration" begin
    using Natter
    using Random

    if get(ENV, "NATTER_RUN_TLS", "false") == "true"
        url = get(ENV, "NATTER_TLS_URL", "tls://127.0.0.1:4222")
        ca_path = get(ENV, "NATTER_TLS_CA", "")
        tls_verify = lowercase(get(ENV, "NATTER_TLS_VERIFY", "true")) != "false"
        tls_verify && isempty(ca_path) && error("NATTER_TLS_CA must point to a CA certificate when NATTER_RUN_TLS=true and NATTER_TLS_VERIFY is not false")

        client = connect(url; tls_verify, tls_ca_path=isempty(ca_path) ? nothing : ca_path, connect_timeout=5.0,
                         ping_interval=2.0, max_outstanding_pings=2)
        try
            subject = "natter.tls.$(randstring(10))"
            sub = subscribe(client, subject)
            publish(client, subject, "secure")
            @test String(next(sub; timeout=2.0)) == "secure"
        finally
            close(client)
        end
    else
        @info "Skipping real nats-server TLS first integration tests; set NATTER_RUN_TLS=true with NATTER_TLS_URL and either NATTER_TLS_CA or NATTER_TLS_VERIFY=false to enable them."
    end
end
