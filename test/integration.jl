using TestItems

@testitem "real nats-server core integration" begin
    using Natter
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
            close(service)

            @test_throws NoRespondersError request(client, "$subject.none", ""; timeout=2.0)

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

            durable = "DUR_$(randstring(6))"
            psub = pull_subscribe(js, subject; stream=stream, durable)
            cinfo = consumer_info(js, stream, durable)
            @test cinfo.config isa ConsumerConfig
            @test cinfo.config.durable_name == durable
            msgs = fetch(psub, 1; timeout=2.0)
            @test length(msgs) == 1
            @test String(first(msgs)) == "payload"
            ack(first(msgs))
            close(psub)

            bucket = "NATTERKV_$(randstring(8))"
            kv[] = kv_create(js, bucket; storage="memory", direct=true)
            try
                @test kv[].direct
                pa2 = kv_put(kv[], "alpha", "one")
                @test pa2.seq >= 1
                @test String(kv_get(kv[], "alpha")) == "one"
                @test String(kv_get(kv[], "alpha"; revision=pa2.seq)) == "one"
                kv_update(kv[], "alpha", "two", pa2.seq)
                @test String(kv_get(kv[], "alpha")) == "two"
                kv_delete(kv[], "alpha")
                @test_throws KeyError kv_get(kv[], "alpha")
                @test !("alpha" in kv_keys(kv[]))
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
