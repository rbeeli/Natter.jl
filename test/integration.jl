using TestItems

@testitem "real nats-server core integration" begin
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
            @test_throws NoRespondersError fetch(request_async(client, "$subject.none", ""; timeout=2.0))

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

@testitem "real nats-server JetStream integration" begin
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
                pa3 = fetch(kv_put_async(kv[], "beta", "async-one"))
                @test pa3.seq >= 1
                @test String(fetch(kv_get_async(kv[], "beta"))) == "async-one"
                @test_throws KeyValueWrongRevisionError kv_update(kv[], "beta", "wrong-revision", pa3.seq + 100)
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
                fetch(kv_delete_async(kv[], "beta"))
                @test_throws KeyValueKeyDeletedError fetch(kv_get_async(kv[], "beta"))
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
