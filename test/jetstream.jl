using TestItems

@testitem "JetStream typed stream config serialization" begin
    using Dates
    using Natter
    using Natter.JetStream

    const N = Natter

    cfg = StreamConfig(
        name="ORDERS",
        description="orders stream",
        subjects=["orders.*"],
        retention=RetentionPolicy.LIMITS,
        storage=StorageType.FILE,
        discard=DiscardPolicy.NEW,
        compression=StoreCompression.S2,
        persist_mode=PersistMode.ASYNC,
        max_consumers=-1,
        max_msgs=-1,
        max_bytes=-1,
        max_age=2.5,
        duplicate_window=1.25,
        first_seq=42,
        allow_rollup_hdrs=true,
        allow_direct=true,
        mirror_direct=true,
        allow_msg_ttl=true,
        allow_msg_schedules=true,
        allow_atomic=true,
        allow_batched=true,
        metadata=Dict("owner" => "core"),
        placement=Placement(cluster="C1", preferred="S1", tags=["ssd", "east"]),
        mirror=StreamSource(
            name="UPSTREAM",
            opt_start_seq=10,
            opt_start_time=DateTime(2026, 1, 2, 3, 4, 5),
            external=ExternalStreamSource(api="\$JS.domain.API", deliver="deliver.subject"),
            subject_transforms=[SubjectTransform(src="orders.*", dest="archive.*")],
        ),
        sources=[StreamSource(name="SOURCE", filter_subject="orders.created")],
        republish=RePublish(src="orders.>", dest="audit.>", headers_only=true),
        subject_transform=SubjectTransform(src="orders.*", dest="events.*"),
        consumer_limits=StreamConsumerLimits(inactive_threshold=3.0, max_ack_pending=100),
        subject_delete_marker_ttl=4.0,
    )

    payload = N._js_config_payload(cfg)

    @test payload["name"] == "ORDERS"
    @test payload["retention"] == "limits"
    @test payload["storage"] == "file"
    @test payload["discard"] == "new"
    @test payload["compression"] == "s2"
    @test payload["persist_mode"] == "async"
    @test payload["max_age"] == 2_500_000_000
    @test payload["duplicate_window"] == 1_250_000_000
    @test payload["subject_delete_marker_ttl"] == 4_000_000_000
    @test payload["metadata"] == Dict("owner" => "core")
    @test payload["placement"]["tags"] == ["ssd", "east"]
    @test payload["mirror"]["external"]["api"] == "\$JS.domain.API"
    @test startswith(payload["mirror"]["opt_start_time"], "2026-01-02T03:04:05")
    @test payload["mirror"]["subject_transforms"][1]["dest"] == "archive.*"
    @test payload["sources"][1]["filter_subject"] == "orders.created"
    @test payload["republish"]["headers_only"] == true
    @test payload["consumer_limits"]["inactive_threshold"] == 3_000_000_000
end

@testitem "JetStream typed consumer config serialization" begin
    using Dates
    using Natter
    using Natter.JetStream

    const N = Natter

    cfg = ConsumerConfig(
        name="worker",
        durable_name="worker",
        description="worker consumer",
        deliver_policy=DeliverPolicy.BY_START_TIME,
        opt_start_time="2026-01-02T03:04:05Z",
        ack_policy=AckPolicy.EXPLICIT,
        ack_wait=0.5,
        max_deliver=5,
        backoff=[0.1, 0.2],
        filter_subjects=["orders.created", "orders.updated"],
        replay_policy=ReplayPolicy.ORIGINAL,
        rate_limit_bps=1024,
        sample_freq="50%",
        max_waiting=8,
        max_ack_pending=64,
        flow_control=true,
        idle_heartbeat=1.0,
        headers_only=true,
        deliver_subject="_INBOX.worker",
        deliver_group="workers",
        inactive_threshold=2.0,
        num_replicas=1,
        mem_storage=true,
        metadata=Dict("role" => "worker"),
        pause_until=DateTime(2026, 1, 3, 0, 0, 0),
        direct=true,
        max_batch=128,
        max_expires=3.0,
        max_bytes=4096,
    )

    payload = N._js_config_payload(cfg)

    @test payload["deliver_policy"] == "by_start_time"
    @test payload["ack_policy"] == "explicit"
    @test payload["ack_wait"] == 500_000_000
    @test payload["backoff"] == [100_000_000, 200_000_000]
    @test payload["filter_subjects"] == ["orders.created", "orders.updated"]
    @test payload["replay_policy"] == "original"
    @test payload["idle_heartbeat"] == 1_000_000_000
    @test payload["inactive_threshold"] == 2_000_000_000
    @test payload["max_expires"] == 3_000_000_000
    @test startswith(payload["pause_until"], "2026-01-03T00:00:00")
end

@testitem "JetStream priority policy values serialize and parse" begin
    using Natter
    using Natter.JetStream

    const N = Natter

    no_policy_payload = N._js_config_payload(ConsumerConfig(priority_policy=PriorityPolicy.NONE))
    @test no_policy_payload["priority_policy"] == "none"

    cases = (
        (PriorityPolicy.OVERFLOW, "overflow"),
        (PriorityPolicy.PINNED_CLIENT, "pinned_client"),
        (PriorityPolicy.PRIORITIZED, "prioritized"),
    )

    for (policy, wire_value) in cases
        payload = N._js_config_payload(ConsumerConfig(
            priority_groups=["fast"],
            priority_policy=policy,
        ))
        @test payload["priority_policy"] == wire_value

        parsed = N._consumer_config_from_payload(Dict{String,Any}(
            "priority_groups" => ["fast"],
            "priority_policy" => wire_value,
        ))
        @test parsed.priority_policy == policy
    end
end

@testitem "JetStream consumer filter config validation" begin
    using Natter
    using Natter.JetStream

    const N = Natter

    payload = N._js_config_payload(ConsumerConfig(filter_subject="orders.created", filter_subjects=String[]))
    @test payload["filter_subject"] == "orders.created"
    @test !haskey(payload, "filter_subjects")

    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(
        filter_subject="orders.created",
        filter_subjects=["orders.updated"],
    ))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(filter_subject="orders..created"))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(filter_subjects=["orders.created", "orders..updated"]))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(filter_subjects=["orders.*", "orders.created"]))
end

@testitem "JetStream typed stream config validates local schema" begin
    using Natter
    using Natter.JetStream

    const N = Natter

    @test_throws ArgumentError N._js_config_payload(StreamConfig(name="bad.name"))
    @test_throws ArgumentError N._js_config_payload(StreamConfig(name="ORDERS", subjects=["orders..created"]))
    @test_throws ArgumentError N._js_config_payload(StreamConfig(name="ORDERS", subjects=["orders.*", "orders.created"]))
    @test_throws ArgumentError N._js_config_payload(StreamConfig(name="ORDERS", max_msgs=-2))
    @test_throws ArgumentError N._js_config_payload(StreamConfig(name="ORDERS", max_msg_size=typemax(Int32) + 1))
    @test_throws ArgumentError N._js_config_payload(StreamConfig(name="ORDERS", num_replicas=6))
    @test_throws ArgumentError N._js_config_payload(StreamConfig(name="ORDERS", first_seq=-1))
    @test_throws ArgumentError N._js_config_payload(StreamConfig(name="ORDERS", max_age=0.01))
    @test_throws ArgumentError N._js_config_payload(StreamConfig(name="ORDERS", max_age=1.0, duplicate_window=2.0))
    @test_throws ArgumentError N._js_config_payload(StreamConfig(name="ORDERS", subject_delete_marker_ttl=0.5))
    @test_throws ArgumentError N._js_config_payload(StreamConfig(name="ORDERS", discard_new_per_subject=true))
    @test_throws ArgumentError N._js_config_payload(StreamConfig(
        name="ORDERS",
        sources=[StreamSource(name="SOURCE", filter_subject="orders.created",
                              subject_transforms=[SubjectTransform(src="orders.*", dest="archive.*")])],
    ))
end

@testitem "JetStream raw stream config validates local schema" begin
    using Natter
    using Natter.JetStream

    const N = Natter

    payload = N._stream_config_payload(Dict{String,Any}(
        "name" => "ORDERS",
        "subjects" => ["orders.*"],
        "mirror" => Dict("name" => "UPSTREAM", "external" => Dict("api" => "\$JS.domain.API")),
        "republish" => Dict("src" => "orders.>", "dest" => "audit.>"),
        "consumer_limits" => Dict("max_ack_pending" => 10),
    ))

    @test payload["subjects"] == ["orders.*"]
    @test payload["mirror"]["external"]["api"] == "\$JS.domain.API"

    @test_throws ArgumentError N._stream_config_payload(Dict{String,Any}("name" => "bad.name"))
    @test_throws ArgumentError N._stream_config_payload(Dict{String,Any}("name" => "ORDERS", "subjects" => ["orders..created"]))
    @test_throws ArgumentError N._stream_config_payload(Dict{String,Any}("name" => "ORDERS", "max_msgs" => -2))
    @test_throws ArgumentError N._stream_config_payload(Dict{String,Any}("name" => "ORDERS", "num_replicas" => 99))
    @test_throws ArgumentError N._stream_config_payload(Dict{String,Any}(
        "name" => "ORDERS",
        "mirror" => Dict("name" => "UPSTREAM", "filter_subject" => "orders..created"),
    ))
    @test_throws ArgumentError N._stream_config_payload(Dict{String,Any}(
        "name" => "ORDERS",
        "sources" => [
            Dict("name" => "SOURCE", "filter_subject" => "orders.created",
                 "subject_transforms" => [Dict("src" => "orders.*", "dest" => "archive.*")]),
        ],
    ))
end

@testitem "JetStream typed consumer config validates local schema" begin
    using Natter
    using Natter.JetStream

    const N = Natter

    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(name="bad.name"))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(name="a", durable_name="b"))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(deliver_subject="deliver.*"))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(deliver_subject="deliver..worker"))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(deliver_group=""))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(deliver_group="bad group"))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(max_deliver=-2))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(max_ack_pending=-2))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(max_waiting=-1))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(rate_limit_bps=-1))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(num_replicas=6))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(max_batch=-1))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(max_bytes=-1))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(max_expires=0.0005))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(idle_heartbeat=0.01))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(backoff=[0.1, -0.2]))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(sample_freq="-1%"))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(sample_freq="101"))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(sample_freq="101%"))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(priority_groups=["bad group"]))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(priority_policy=PriorityPolicy.OVERFLOW))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(priority_groups=["fast"]))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(priority_policy=PriorityPolicy.NONE,
                                                                    priority_groups=["fast"]))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(priority_policy=PriorityPolicy.OVERFLOW,
                                                                    priority_groups=["fast"],
                                                                    deliver_subject="_INBOX.deliver"))
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(priority_policy=PriorityPolicy.PRIORITIZED,
                                                                    priority_groups=["fast"],
                                                                    priority_timeout=1.0))
    @test_throws ArgumentError N._validate_consumer_config_payload!(
        N._js_config_payload(Dict{String,Any}("priority_policy" => "bad")))

    priority_payload = N._js_config_payload(ConsumerConfig(priority_policy=PriorityPolicy.PINNED_CLIENT,
                                                           priority_groups=["fast"],
                                                           priority_timeout=1.0))
    @test priority_payload["priority_policy"] == "pinned_client"
    @test priority_payload["priority_groups"] == ["fast"]
    @test priority_payload["priority_timeout"] == 1_000_000_000
end

@testitem "JetStream raw dict config serialization matches typed config units" begin
    using Dates
    using Natter
    using Natter.JetStream

    const N = Natter

    stream = N._js_config_payload(Dict{String,Any}(
        "retention" => RetentionPolicy.LIMITS,
        "max_age" => 2.5,
        "duplicate_window" => 1.25,
        "subject_delete_marker_ttl" => 4.0,
        "metadata" => Dict("owner" => "core"),
        "mirror" => Dict("name" => "UPSTREAM", "opt_start_time" => DateTime(2026, 1, 2, 3, 4, 5)),
        "consumer_limits" => Dict("inactive_threshold" => 3.0),
    ))

    @test stream["retention"] == "limits"
    @test stream["max_age"] == 2_500_000_000
    @test stream["duplicate_window"] == 1_250_000_000
    @test stream["subject_delete_marker_ttl"] == 4_000_000_000
    @test stream["metadata"] == Dict("owner" => "core")
    @test startswith(stream["mirror"]["opt_start_time"], "2026-01-02T03:04:05")
    @test stream["consumer_limits"]["inactive_threshold"] == 3_000_000_000

    consumer = N._js_config_payload(Dict{String,Any}(
        "ack_policy" => AckPolicy.EXPLICIT,
        "ack_wait" => 0.5,
        "backoff" => [0.1, 0.2],
        "idle_heartbeat" => 1.0,
        "inactive_threshold" => 2.0,
        "max_expires" => 3.0,
        "priority_policy" => PriorityPolicy.PINNED_CLIENT,
        "priority_timeout" => 4.0,
        "pause_until" => DateTime(2026, 1, 3, 0, 0, 0),
    ))

    @test consumer["ack_policy"] == "explicit"
    @test consumer["ack_wait"] == 500_000_000
    @test consumer["backoff"] == [100_000_000, 200_000_000]
    @test consumer["idle_heartbeat"] == 1_000_000_000
    @test consumer["inactive_threshold"] == 2_000_000_000
    @test consumer["max_expires"] == 3_000_000_000
    @test consumer["priority_policy"] == "pinned_client"
    @test consumer["priority_timeout"] == 4_000_000_000
    @test startswith(consumer["pause_until"], "2026-01-03T00:00:00")
end

@testitem "JetStream normalized consumer payload is not converted twice" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(;
        status=N.ConnectionStatus.CONNECTED,
        info=N.ServerInfo(; headers=true, version="2.10.0"),
        write_io=capture,
    )
    js = jetstream(client)
    payload = N._js_config_payload(Dict{String,Any}(
        "name" => "worker",
        "ack_wait" => 0.5,
        "backoff" => [0.1, 0.2],
    ))

    @test_throws TimeoutError N._consumer_create_payload_request(js, "ORDERS", payload; timeout=0.001, action="create")

    written = TestHelpers.capture_text(capture)
    json_match = match(r"\{.*\}", written)
    @test !isnothing(json_match)
    request = N._json_dict(json_match.match)
    config = N._consumer_normalized_config_value(request["config"])
    @test config["ack_wait"] == 500_000_000
    @test config["backoff"] == [100_000_000, 200_000_000]
end

@testitem "JetStream timeout arguments are positive finite before protocol writes" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    for invalid_timeout in (-1.0, 0.0, Inf, NaN, true)
        capture = TestHelpers.WriteCapture()
        client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)

        @test_throws ArgumentError jetstream(client; timeout=invalid_timeout)
        js = jetstream(client)
        @test_throws ArgumentError js_publish(js, "orders.created", "payload"; timeout=invalid_timeout)
        @test TestHelpers.capture_text(capture) == ""
        @test isempty(client.subscriptions)

        @test_throws ArgumentError stream_list(js; timeout=invalid_timeout)
        @test_throws ArgumentError stream_list_page(js; timeout=invalid_timeout)
        @test_throws ArgumentError stream_names(js; timeout=invalid_timeout)
        @test_throws ArgumentError stream_names_page(js; timeout=invalid_timeout)
        @test_throws ArgumentError consumer_list(js, "ORDERS"; timeout=invalid_timeout)
        @test_throws ArgumentError consumer_list_page(js, "ORDERS"; timeout=invalid_timeout)
        @test TestHelpers.capture_text(capture) == ""
        @test isempty(client.subscriptions)

        @test_throws ArgumentError pull_subscribe(js, "orders.created"; stream="ORDERS", timeout=invalid_timeout)
        @test_throws ArgumentError push_subscribe(js, "orders.created"; stream="ORDERS", timeout=invalid_timeout)
        @test TestHelpers.capture_text(capture) == ""
        @test isempty(client.subscriptions)

        msg = N.JetStreamMsg(
            Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.1.1.0", UInt8[]),
            client,
        )
        @test_throws ArgumentError ack_sync(msg; timeout=invalid_timeout)
        @test TestHelpers.capture_text(capture) == ""
        @test isempty(client.subscriptions)
        @test !N._acknowledged(msg)
    end

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)
    @test_throws ArgumentError stream_list(js; offset=-1)
    @test_throws ArgumentError stream_list_page(js; offset=-1)
    @test_throws ArgumentError consumer_list(js, "ORDERS"; offset=-1)
    @test_throws ArgumentError consumer_list_page(js, "ORDERS"; offset=-1)
    @test_throws ArgumentError stream_names(js; offset=-1)
    @test_throws ArgumentError stream_names_page(js; offset=-1)
    @test TestHelpers.capture_text(capture) == ""
    @test isempty(client.subscriptions)
end

@testitem "JetStream list page and iterator APIs page lazily" setup=[TestHelpers] begin
    using JSON3
    using Natter
    using Natter.JetStream

    const N = Natter

    function respond_next_request!(client, payload::AbstractString)
        result = timedwait(1.0; pollint=0.001) do
            @lock client.lock begin
                mux = client.request_mux
                !isnothing(mux) && !isempty(mux.waiters)
            end
        end
        @test result != :timed_out
        subject, sid = @lock client.lock begin
            mux = client.request_mux
            token = first(keys(mux.waiters))
            "$(mux.prefix).$token", mux.sub.sid
        end
        N._dispatch_msg(client, Msg(subject, nothing, TestHelpers.bytes(payload); sid))
    end

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    js = jetstream(client)

    stream_payload = JSON3.write(Dict(
        "streams" => [
            Dict("config" => Dict("name" => "ORDERS"), "state" => Dict()),
        ],
        "offset" => 2,
        "total" => 3,
        "limit" => 1,
    ))
    stream_task = Threads.@spawn stream_list_page(js; offset=2)
    respond_next_request!(client, stream_payload)
    stream_page = fetch(stream_task)
    @test stream_page isa JetStreamPage{StreamInfo}
    @test stream_page.offset == 2
    @test stream_page.total == 3
    @test stream_page.limit == 1
    @test only(stream_page.items).name == "ORDERS"

    consumer_payload = JSON3.write(Dict(
        "consumers" => [
            Dict("stream_name" => "ORDERS", "name" => "worker",
                 "config" => Dict("durable_name" => "worker")),
        ],
        "offset" => 0,
        "total" => 1,
        "limit" => 1,
    ))
    consumer_task = Threads.@spawn consumer_list_page(js, "ORDERS")
    respond_next_request!(client, consumer_payload)
    consumer_page = fetch(consumer_task)
    @test consumer_page isa JetStreamPage{ConsumerInfo}
    @test only(consumer_page).name == "worker"

    names_task = Threads.@spawn collect(stream_names_iter(js))
    respond_next_request!(client, JSON3.write(Dict(
        "streams" => ["A", "B"],
        "offset" => 0,
        "total" => 3,
        "limit" => 2,
    )))
    respond_next_request!(client, JSON3.write(Dict(
        "streams" => ["C"],
        "offset" => 2,
        "total" => 3,
        "limit" => 2,
    )))
    @test fetch(names_task) == ["A", "B", "C"]

    lazy_iter = stream_names_pages(js)
    first_task = Threads.@spawn iterate(lazy_iter)
    respond_next_request!(client, JSON3.write(Dict(
        "streams" => ["D"],
        "offset" => 0,
        "total" => 2,
        "limit" => 1,
    )))
    first_result = fetch(first_task)
    @test !isnothing(first_result)
    first_page, state = first_result
    @test first_page.items == ["D"]
    @test (@lock client.lock isempty(client.request_mux.waiters))

    second_task = Threads.@spawn iterate(lazy_iter, state)
    respond_next_request!(client, JSON3.write(Dict(
        "streams" => ["E"],
        "offset" => 1,
        "total" => 2,
        "limit" => 1,
    )))
    second_result = fetch(second_task)
    @test !isnothing(second_result)
    second_page, _ = second_result
    @test second_page.items == ["E"]
end

@testitem "JetStream publish options serialize supported headers" setup=[TestHelpers] begin
    using Dates
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)

    @test_throws TimeoutError js_publish(
        js,
        "orders.created",
        "payload";
        timeout=0.001,
        stream=SubString("ORDERS.extra", 1, 6),
        msg_id="msg-1",
        expected_last_sequence=10,
        expected_last_subject_sequence=3,
        expected_last_subject="orders.created",
        expected_last_msg_id="msg-0",
        ttl=1.5,
        schedule_every=5.0,
        schedule_target="orders.scheduled",
        schedule_source="orders.source",
        schedule_ttl=:never,
        schedule_timezone="UTC",
    )

    written = TestHelpers.capture_text(capture)
    @test occursin("HPUB orders.created ", written)
    @test occursin("Nats-Expected-Stream: ORDERS\r\n", written)
    @test occursin("Nats-Msg-Id: msg-1\r\n", written)
    @test occursin("Nats-Expected-Last-Sequence: 10\r\n", written)
    @test occursin("Nats-Expected-Last-Subject-Sequence: 3\r\n", written)
    @test occursin("Nats-Expected-Last-Subject-Sequence-Subject: orders.created\r\n", written)
    @test occursin("Nats-Expected-Last-Msg-Id: msg-0\r\n", written)
    @test occursin("Nats-TTL: 1.5s\r\n", written)
    @test occursin("Nats-Schedule: @every 5s\r\n", written)
    @test occursin("Nats-Schedule-Target: orders.scheduled\r\n", written)
    @test occursin("Nats-Schedule-Source: orders.source\r\n", written)
    @test occursin("Nats-Schedule-TTL: never\r\n", written)
    @test occursin("Nats-Schedule-Time-Zone: UTC\r\n", written)

    @test isnothing(N._js_publish_headers(nothing))
    hdrs = N._js_publish_headers(nothing; schedule_at=DateTime(2026, 1, 2, 3, 4, 5))
    @test hdrs["Nats-Schedule"] == ["@at 2026-01-02T03:04:05.000Z"]

    TestHelpers.clear_capture!(capture)
    @test_throws ArgumentError js_publish(js, "orders.created", "payload"; stream="A", expected_stream="B")
    @test_throws ArgumentError js_publish(js, "orders.created", "payload"; expected_last_subject="orders.created")
    @test_throws ArgumentError js_publish(js, "orders.created", "payload"; ttl=0)
    @test_throws ArgumentError js_publish(js, "orders.created", "payload"; ttl=0.5)
    @test_throws ArgumentError js_publish(js, "orders.created", "payload"; schedule="x", schedule_every=1.0)
    @test_throws ArgumentError js_publish(js, "orders.created", "payload"; retry_attempts=-1)
    @test_throws ArgumentError js_publish_future(js, "orders.created", "payload"; retry_attempts=-1)
    @test_throws ArgumentError js_publish_future(js, "orders.created", "payload"; retry_wait=0)
    @test TestHelpers.capture_text(capture) == ""
end

@testitem "JetStream publish ack parser decodes typed ack shape" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    ack = N._js_read_puback(
        Msg("_INBOX.reply", nothing, TestHelpers.bytes("""{"stream":"ORDERS","seq":42}""")),
    )
    @test ack.stream == "ORDERS"
    @test ack.seq == 42
    @test !ack.duplicate
    @test isnothing(ack.domain)

    duplicate = N._js_read_puback(
        Msg("_INBOX.reply", nothing,
            TestHelpers.bytes("""{"stream":"ORDERS","seq":43,"duplicate":true,"domain":"HUB"}""")),
    )
    @test duplicate.stream == "ORDERS"
    @test duplicate.seq == 43
    @test duplicate.duplicate
    @test duplicate.domain == "HUB"

    err_payload = """{"error":{"code":400,"err_code":10071,"description":"wrong stream"}}"""
    err = TestHelpers.thrown_exception() do
        N._js_read_puback(Msg("_INBOX.reply", nothing, TestHelpers.bytes(err_payload)))
    end
    @test err isa JetStreamError
    @test err.code == 400
    @test err.err_code == 10071

    missing = TestHelpers.thrown_exception() do
        N._js_read_puback(Msg("_INBOX.reply", nothing, TestHelpers.bytes("{}")))
    end
    @test missing isa ProtocolError
    @test occursin("stream", missing.message)
end

@testitem "JetStream async publish uses protocol futures" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client; publish_future_max_pending=8)

    future = js_publish_future(js, "orders.created", "payload"; timeout=1.0)

    @test future isa JetStreamPublishFuture
    @test !isready(future)
    @test future.retry_attempts == N.DEFAULT_JS_PUBLISH_RETRY_ATTEMPTS
    @test js_publish_future_pending(js) == 1
    @test length(client.subscriptions) == 1
    sub = only(values(client.subscriptions))
    @test !sub.has_callback
    @test sub.processor === nothing
    @test sub.control_handler isa N._JetStreamAsyncPublishControlHandler
    @test sub.subject == "$(js.publish_futures.prefix).*"

    written = TestHelpers.capture_text(capture)
    @test occursin("SUB $(js.publish_futures.prefix).* $(sub.sid)\r\n", written)
    @test occursin("PUB orders.created $(future.reply) 7\r\npayload\r\n", written)

    ack_payload = """{"stream":"ORDERS","seq":1,"duplicate":false}"""
    N._dispatch_msg(client, Msg(future.reply, nothing, TestHelpers.bytes(ack_payload); sid=sub.sid))
    @test !isready(sub.messages)
    @test timedwait(1.0; pollint=0.001) do
        isready(future) && js_publish_future_pending(js) == 0
    end == :ok

    ack = fetch(future)
    @test ack.stream == "ORDERS"
    @test ack.seq == 1
    @test !ack.duplicate
    @test isnothing(js_publish_future_complete(js; timeout=0.1))

    close(client)
end

@testitem "JetStream publish futures honor cancellation while waiting for write lock" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)
    N._ensure_js_async_publish_subscription!(js.publish_futures)
    TestHelpers.clear_capture!(capture)

    source = CancellationSource()
    token = cancellation_token(source)
    lock(client.write_lock)
    task = Threads.@spawn TestHelpers.thrown_exception() do
        js_publish_future(js, "orders.created", "payload"; cancel_token=token)
    end
    try
        @test timedwait(1.0; pollint=0.001) do
            client.write_waiters[] > 0
        end == :ok
        @test cancel!(source)
        finished = timedwait(1.0; pollint=0.001) do
            istaskdone(task)
        end
        @test finished == :ok
        err = finished == :ok ? fetch(task) : nothing
        @test err isa CancelledError
        @test js_publish_future_pending(js) == 0
        @test TestHelpers.capture_text(capture) == ""
        @test N.status(client) == N.ConnectionStatus.CONNECTED
    finally
        iscancelled(token) || cancel!(source)
        unlock(client.write_lock)
        istaskdone(task) || wait(task)
        close(client)
    end
end

@testitem "JetStream publish future setup honors cancellation while waiting for write lock" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)

    source = CancellationSource()
    token = cancellation_token(source)
    lock(client.write_lock)
    task = Threads.@spawn TestHelpers.thrown_exception() do
        js_publish_future(js, "orders.created", "payload"; cancel_token=token)
    end
    try
        @test timedwait(1.0; pollint=0.001) do
            client.write_waiters[] > 0
        end == :ok
        @test cancel!(source)
        finished = timedwait(1.0; pollint=0.001) do
            istaskdone(task)
        end
        @test finished == :ok
        err = finished == :ok ? fetch(task) : nothing
        @test err isa CancelledError
        @test js_publish_future_pending(js) == 0
        @test TestHelpers.capture_text(capture) == ""
        @test isempty(client.subscriptions)
        @test N.status(client) == N.ConnectionStatus.CONNECTED
    finally
        iscancelled(token) || cancel!(source)
        unlock(client.write_lock)
        istaskdone(task) || wait(task)
        close(client)
    end
end

@testitem "JetStream publish future waits support timeout and cancellation" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)
    future = js_publish_future(js, "orders.created", "payload"; timeout=5.0)
    sub = only(values(client.subscriptions))

    @test_throws TimeoutError fetch(future; timeout=0.01)
    @test !isready(future)
    @test js_publish_future_pending(js) == 1

    source = CancellationSource()
    token = cancellation_token(source)
    task = Threads.@spawn TestHelpers.thrown_exception() do
        fetch(future; cancel_token=token)
    end
    @test timedwait(0.05; pollint=0.001) do
        istaskdone(task)
    end == :timed_out
    @test cancel!(source)
    @test timedwait(1.0; pollint=0.001) do
        istaskdone(task)
    end == :ok
    @test fetch(task) isa CancelledError
    @test !isready(future)
    @test js_publish_future_pending(js) == 1

    N._dispatch_msg(client, Msg(future.reply, nothing,
                               TestHelpers.bytes("""{"stream":"ORDERS","seq":9}"""); sid=sub.sid))
    ack = fetch(future; timeout=1.0)
    @test ack.stream == "ORDERS"
    @test ack.seq == 9
    @test js_publish_future_pending(js) == 0

    close(client)
end

@testitem "JetStream async publish timeout monitor accepts long deadlines" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)

    future = js_publish_future(js, "orders.created", "payload"; timeout=1.0e20)
    task = js.publish_futures.timeout_task
    @test task isa Task

    sleep(0.02)
    @test !istaskfailed(task)
    @test !isready(future)
    @test js_publish_future_pending(js) == 1

    close(client)
    @test timedwait(1.0; pollint=0.001) do
        isready(future) && js_publish_future_pending(js) == 0
    end == :ok
end

@testitem "JetStream async publish pending futures are cleared on reconnect" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)

    future = js_publish_future(js, "orders.created", "payload"; timeout=1.0)
    @test js_publish_future_pending(js) == 1
    @test !isempty(client.lifecycle_watchers)

    TestHelpers.clear_capture!(capture)
    N._trigger_reconnect(client, ErrorException("transport failed"))
    @test timedwait(1.0; pollint=0.001) do
        isready(future) && js_publish_future_pending(js) == 0
    end == :ok

    err = TestHelpers.thrown_exception(() -> fetch(future))
    @test err isa ConnectionReconnectingError
    @test TestHelpers.capture_text(capture) == ""
    @test client.pending_bytes == 0

    @test timedwait(1.0; pollint=0.001) do
        N.status(client) == N.ConnectionStatus.DISCONNECTED
    end == :ok
end

@testitem "JetStream async publish applies backpressure and waits for completion" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client; publish_future_max_pending=1)

    first = js_publish_future(js, "orders.created", "one"; timeout=1.0)
    @test js_publish_future_pending(js) == 1

    backpressure_err = TestHelpers.thrown_exception() do
        js_publish_future(js, "orders.created", "two"; timeout=0.01)
    end
    @test backpressure_err isa TimeoutError
    @test js_publish_future_pending(js) == 1

    complete_task = Threads.@spawn js_publish_future_complete(js; timeout=1.0)
    sleep(0.02)
    @test !istaskdone(complete_task)

    sub = only(values(client.subscriptions))
    N._dispatch_msg(client, Msg(first.reply, nothing,
                               TestHelpers.bytes("""{"stream":"ORDERS","seq":1}"""); sid=sub.sid))
    @test isnothing(fetch(complete_task))
    @test js_publish_future_pending(js) == 0

    close(client)
end

@testitem "JetStream async publish futures receive server errors and timeouts" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)

    failed = js_publish_future(js, "orders.created", "bad"; timeout=1.0)
    sub = only(values(client.subscriptions))
    err_payload = """{"error":{"code":400,"err_code":10071,"description":"wrong stream"}}"""
    N._dispatch_msg(client, Msg(failed.reply, nothing, TestHelpers.bytes(err_payload); sid=sub.sid))
    @test timedwait(1.0; pollint=0.001) do
        isready(failed)
    end == :ok
    failed_err = TestHelpers.thrown_exception(() -> fetch(failed))
    @test failed_err isa JetStreamError
    @test failed_err.code == 400
    @test js_publish_future_pending(js) == 0

    ack_error_payload = """{"stream":"ORDERS","seq":2,"error":{"code":400,"err_code":10071,"description":"wrong last sequence"}}"""
    ack_error = TestHelpers.thrown_exception() do
        N._js_read_puback(Msg("\$JS.ACK", nothing, TestHelpers.bytes(ack_error_payload)))
    end
    @test ack_error isa JetStreamError
    @test ack_error.code == 400
    @test ack_error.err_code == 10071

    no_responders = js_publish_future(js, "orders.created", "missing"; timeout=1.0,
                                     retry_attempts=0)
    status_headers = Headers("Status" => ["503"], "Description" => ["No Responders"])
    N._dispatch_msg(client, Msg(no_responders.reply, nothing, UInt8[];
                               headers=status_headers, sid=sub.sid))
    @test timedwait(1.0; pollint=0.001) do
        isready(no_responders)
    end == :ok
    no_responders_err = TestHelpers.thrown_exception(() -> fetch(no_responders))
    @test no_responders_err isa NoRespondersError
    @test js_publish_future_pending(js) == 0

    TestHelpers.clear_capture!(capture)
    retried = js_publish_future(js, "orders.created", "retry"; timeout=5.0,
                               retry_attempts=1, retry_wait=0.001)
    retry_reply = retried.reply
    N._dispatch_msg(client, Msg(retry_reply, nothing, UInt8[];
                               headers=status_headers, sid=sub.sid))
    @test timedwait(1.0; pollint=0.001) do
        length(collect(eachmatch(r"PUB orders.created ", TestHelpers.capture_text(capture)))) == 2
    end == :ok
    @test !isready(retried)

    N._dispatch_msg(client, Msg(retry_reply, nothing,
                               TestHelpers.bytes("""{"stream":"ORDERS","seq":3}"""); sid=sub.sid))
    @test timedwait(1.0; pollint=0.001) do
        isready(retried)
    end == :ok
    retry_ack = fetch(retried)
    @test retry_ack.stream == "ORDERS"
    @test retry_ack.seq == 3
    @test js_publish_future_pending(js) == 0

    timed_out = js_publish_future(js, "orders.created", "slow"; timeout=0.01)
    timeout_err = TestHelpers.thrown_exception(() -> fetch(timed_out))
    @test timeout_err isa TimeoutError
    @test js_publish_future_pending(js) == 0

    close(client)
end

@testitem "JetStream typed config response parsing" begin
    using Dates
    using Natter
    using Natter.JetStream

    const N = Natter

    stream = N._stream_config_from_payload(Dict{String,Any}(
        "name" => "ORDERS",
        "storage" => "memory",
        "retention" => "workqueue",
        "discard" => "old",
        "compression" => "none",
        "persist_mode" => "default",
        "max_age" => 2_000_000_000,
        "duplicate_window" => 1_000_000_000,
        "subject_delete_marker_ttl" => 3_000_000_000,
        "placement" => Dict("cluster" => "C1", "preferred" => "S1"),
        "mirror" => Dict("name" => "UPSTREAM",
                         "opt_start_time" => "2026-01-02T03:04:05.123456789Z",
                         "external" => Dict("api" => "\$JS.domain.API")),
        "consumer_limits" => Dict("inactive_threshold" => 4_000_000_000, "max_ack_pending" => 10),
        "metadata" => Dict("owner" => "core"),
    ))

    @test stream.name == "ORDERS"
    @test stream.storage == StorageType.MEMORY
    @test stream.retention == RetentionPolicy.WORK_QUEUE
    @test stream.discard == DiscardPolicy.OLD
    @test stream.compression == StoreCompression.NONE
    @test stream.persist_mode == PersistMode.DEFAULT
    @test stream.max_age == 2.0
    @test stream.duplicate_window == 1.0
    @test stream.subject_delete_marker_ttl == 3.0
    @test stream.placement.preferred == "S1"
    @test stream.mirror.external.api == "\$JS.domain.API"
    @test stream.mirror.opt_start_time == DateTime(2026, 1, 2, 3, 4, 5, 123)
    @test stream.consumer_limits.inactive_threshold == 4.0

    consumer = N._consumer_config_from_payload(Dict{String,Any}(
        "name" => "worker",
        "deliver_policy" => "last_per_subject",
        "opt_start_time" => "2026-01-03T04:05:06Z",
        "ack_policy" => "none",
        "ack_wait" => 5_000_000_000,
        "backoff" => [1_000_000_000, 2_000_000_000],
        "replay_policy" => "instant",
        "idle_heartbeat" => 250_000_000,
        "priority_policy" => "overflow",
        "priority_timeout" => 6_000_000_000,
        "priority_groups" => ["fast"],
        "pause_until" => "2026-01-04T05:06:07.987654321Z",
    ))

    @test consumer.name == "worker"
    @test consumer.deliver_policy == DeliverPolicy.LAST_PER_SUBJECT
    @test consumer.ack_policy == AckPolicy.NONE
    @test consumer.ack_wait == 5.0
    @test consumer.backoff == [1.0, 2.0]
    @test consumer.replay_policy == ReplayPolicy.INSTANT
    @test consumer.idle_heartbeat == 0.25
    @test consumer.priority_policy == PriorityPolicy.OVERFLOW
    @test consumer.priority_timeout == 6.0
    @test consumer.opt_start_time == DateTime(2026, 1, 3, 4, 5, 6)
    @test consumer.pause_until == DateTime(2026, 1, 4, 5, 6, 7, 987)

    info = N._consumer_info(Dict{String,Any}(
        "stream_name" => "ORDERS",
        "name" => "worker",
        "push_bound" => true,
        "num_pending" => 3,
        "delivered" => Dict{String,Any}("consumer_seq" => 2, "stream_seq" => 9),
        "config" => Dict{String,Any}("deliver_subject" => "_INBOX.worker"),
    ))
    @test info.push_bound
    @test info.num_pending == 3
    @test info.delivered.consumer_seq == 2
    @test info.delivered.stream_seq == 9

    stream_info = N._stream_info(Dict{String,Any}(
        "config" => Dict{String,Any}("name" => "ORDERS"),
        "state" => Dict{String,Any}(
            "messages" => 2,
            "bytes" => 128,
            "subjects" => Dict{String,Any}("orders.created" => 2),
            "lost" => Dict{String,Any}("msgs" => [4], "bytes" => 16),
        ),
    ))
    @test stream_info.state.messages == 2
    @test stream_info.state.bytes == 128
    @test stream_info.state.subjects == Dict("orders.created" => 2)
    @test stream_info.state.lost.msgs == [4]
    @test stream_info.state.lost.bytes == 16
    @test fieldtype(StreamInfo, :state) === StreamState
    @test !(:raw in fieldnames(StreamInfo))
    @test !(:raw in fieldnames(ConsumerInfo))
end

@testitem "JetStream typed config rejects invalid local metadata" begin
    using Natter
    using Natter.JetStream

    const N = Natter

    @test_throws MethodError StreamConfig(name="S", metadata=Dict{String,Any}("ok" => 1))
    @test_throws ArgumentError N._js_field_value(:metadata, Dict{String,Any}("ok" => 1))
end

@testitem "JetStream consumer filter config validation happens before request" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)

    @test_throws ArgumentError N._consumer_create_payload_request(
        js,
        "ORDERS",
        Dict{String,Any}("name" => "worker", "filter_subject" => "orders..created");
        timeout=0.001,
        action="create",
    )
    @test TestHelpers.capture_text(capture) == ""

    @test_throws ArgumentError N._consumer_create_payload_request(
        js,
        "ORDERS",
        Dict{String,Any}(
            "name" => "worker",
            "filter_subject" => "orders.created",
            "filter_subjects" => ["orders.updated"],
        );
        timeout=0.001,
        action="create",
    )
    @test TestHelpers.capture_text(capture) == ""

    TestHelpers.clear_capture!(capture)
    @test_throws ArgumentError pull_subscribe(js, "orders.created"; config=Dict("filter_subject" => "orders..created"))
    @test TestHelpers.capture_text(capture) == ""

    TestHelpers.clear_capture!(capture)
    @test_throws ArgumentError push_subscribe(js, "orders.created"; config=Dict("filter_subject" => "orders..created"))
    @test TestHelpers.capture_text(capture) == ""
end

@testitem "JetStream stream config validation happens before request" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)

    @test_throws ArgumentError stream_create(
        js,
        StreamConfig(name="ORDERS", subjects=["orders..created"]);
        timeout=0.001,
    )
    @test TestHelpers.capture_text(capture) == ""

    TestHelpers.clear_capture!(capture)
    @test_throws ArgumentError stream_create(
        js,
        Dict{String,Any}("name" => "ORDERS", "subjects" => ["orders..created"]);
        timeout=0.001,
    )
    @test TestHelpers.capture_text(capture) == ""

    TestHelpers.clear_capture!(capture)
    @test_throws ArgumentError stream_update(
        js,
        Dict{String,Any}("name" => "ORDERS", "max_msgs" => -2);
        timeout=0.001,
    )
    @test TestHelpers.capture_text(capture) == ""
end

@testitem "JetStream configs must be reflected by server response" begin
    using Natter
    using Natter.JetStream
    using JSON3

    const N = Natter

    stream_requested = N._stream_config_payload(StreamConfig(
        name="ORDERS",
        subjects=["orders.*"],
        subject_transform=SubjectTransform(src="orders.*", dest="archive.*"),
        sources=[StreamSource(
            name="ARCHIVE",
            subject_transforms=[SubjectTransform(src="archive.*", dest="orders.*")],
        )],
        allow_msg_ttl=true,
    ))
    stream_observed = deepcopy(stream_requested)
    N._assert_js_config_reflected!("stream", stream_requested, stream_observed)

    missing_transform = deepcopy(stream_requested)
    delete!(missing_transform, "subject_transform")
    @test_throws UnsupportedFeatureError N._assert_js_config_reflected!(
        "stream",
        stream_requested,
        missing_transform,
    )

    missing_source_transform = deepcopy(stream_requested)
    delete!(missing_source_transform["sources"][1], "subject_transforms")
    @test_throws UnsupportedFeatureError N._assert_js_config_reflected!(
        "stream",
        stream_requested,
        missing_source_transform,
    )

    consumer_requested = N._js_config_payload(ConsumerConfig(
        name="worker",
        filter_subjects=["orders.created", "orders.updated"],
        priority_groups=["fast", "slow"],
        priority_policy=PriorityPolicy.OVERFLOW,
    ))
    consumer_observed = deepcopy(consumer_requested)
    N._assert_js_config_reflected!("consumer", consumer_requested, consumer_observed)

    missing_filters = deepcopy(consumer_requested)
    delete!(missing_filters, "filter_subjects")
    @test_throws UnsupportedFeatureError N._assert_js_config_reflected!(
        "consumer",
        consumer_requested,
        missing_filters,
    )

    stream_clears = Dict{String,Any}(
        "description" => "",
        "max_age" => 0,
        "allow_direct" => false,
        "consumer_limits" => Dict{String,Any}(),
        "metadata" => Dict{String,String}(),
        "sources" => Any[],
    )
    stream_observed_cleared = Dict{String,Any}(
        "metadata" => Dict{String,String}("_nats.ver" => "2.11.17"),
    )
    N._assert_js_config_reflected!("stream", stream_clears, stream_observed_cleared)

    stream_still_enabled = deepcopy(stream_observed_cleared)
    stream_still_enabled["allow_direct"] = true
    @test_throws UnsupportedFeatureError N._assert_js_config_reflected!(
        "stream",
        stream_clears,
        stream_still_enabled,
    )

    stream_still_limited = deepcopy(stream_observed_cleared)
    stream_still_limited["max_age"] = 1
    @test_throws UnsupportedFeatureError N._assert_js_config_reflected!(
        "stream",
        stream_clears,
        stream_still_limited,
    )

    stream_still_described = deepcopy(stream_observed_cleared)
    stream_still_described["description"] = "old"
    @test_throws UnsupportedFeatureError N._assert_js_config_reflected!(
        "stream",
        stream_clears,
        stream_still_described,
    )

    stream_still_tagged = deepcopy(stream_observed_cleared)
    stream_still_tagged["metadata"]["owner"] = "core"
    @test_throws UnsupportedFeatureError N._assert_js_config_reflected!(
        "stream",
        stream_clears,
        stream_still_tagged,
    )

    stream_still_sourced = deepcopy(stream_observed_cleared)
    stream_still_sourced["sources"] = [Dict{String,Any}("name" => "ARCHIVE")]
    @test_throws UnsupportedFeatureError N._assert_js_config_reflected!(
        "stream",
        stream_clears,
        stream_still_sourced,
    )

    consumer_clears = Dict{String,Any}(
        "flow_control" => false,
        "idle_heartbeat" => 0,
        "headers_only" => false,
        "backoff" => Any[],
        "metadata" => Dict{String,String}(),
    )
    consumer_observed_cleared = Dict{String,Any}(
        "metadata" => Dict{String,String}("_nats.ver" => "2.11.17"),
    )
    N._assert_js_config_reflected!("consumer", consumer_clears, consumer_observed_cleared)

    consumer_still_enabled = deepcopy(consumer_observed_cleared)
    consumer_still_enabled["headers_only"] = true
    @test_throws UnsupportedFeatureError N._assert_js_config_reflected!(
        "consumer",
        consumer_clears,
        consumer_still_enabled,
    )

    consumer_still_heartbeat = deepcopy(consumer_observed_cleared)
    consumer_still_heartbeat["idle_heartbeat"] = 1
    @test_throws UnsupportedFeatureError N._assert_js_config_reflected!(
        "consumer",
        consumer_clears,
        consumer_still_heartbeat,
    )

    consumer_still_backoff = deepcopy(consumer_observed_cleared)
    consumer_still_backoff["backoff"] = [1]
    @test_throws UnsupportedFeatureError N._assert_js_config_reflected!(
        "consumer",
        consumer_clears,
        consumer_still_backoff,
    )

    raw_stream_requested = Dict{String,Any}(
        "name" => "ORDERS",
        "subjects" => ["orders.*"],
        "future_stream_field" => Dict{String,Any}("enabled" => true),
    )
    raw_stream_response = JSON3.read("""
    {
      "config": {
        "name": "ORDERS",
        "subjects": ["orders.*"],
        "future_stream_field": {"enabled": true}
      },
      "state": {}
    }
    """)
    @test N._assert_stream_config_reflected!(raw_stream_requested, raw_stream_response).name == "ORDERS"

    raw_stream_response_with_server_nested = JSON3.read("""
    {
      "config": {
        "name": "ORDERS",
        "subjects": ["orders.*"],
        "future_stream_field": {"enabled": true, "server_default": {"mode": "auto"}}
      },
      "state": {}
    }
    """)
    @test N._assert_stream_config_reflected!(
        raw_stream_requested,
        raw_stream_response_with_server_nested;
        allow_unknown_field_extras=true,
    ).name == "ORDERS"
    @test_throws UnsupportedFeatureError N._assert_stream_config_reflected!(
        raw_stream_requested,
        raw_stream_response_with_server_nested,
    )

    raw_stream_missing = JSON3.read("""
    {
      "config": {
        "name": "ORDERS",
        "subjects": ["orders.*"]
      },
      "state": {}
    }
    """)
    @test_throws UnsupportedFeatureError N._assert_stream_config_reflected!(
        raw_stream_requested,
        raw_stream_missing,
    )

    raw_known_nested_requested = Dict{String,Any}(
        "name" => "ORDERS",
        "subjects" => ["orders.*"],
        "consumer_limits" => Dict{String,Any}(),
    )
    raw_known_nested_observed = JSON3.read("""
    {
      "config": {
        "name": "ORDERS",
        "subjects": ["orders.*"],
        "consumer_limits": {"inactive_threshold": 1}
      },
      "state": {}
    }
    """)
    @test_throws UnsupportedFeatureError N._assert_stream_config_reflected!(
        raw_known_nested_requested,
        raw_known_nested_observed;
        allow_unknown_field_extras=true,
    )

    raw_consumer_requested = Dict{String,Any}(
        "durable_name" => "worker",
        "ack_policy" => "explicit",
        "future_consumer_field" => Dict{String,Any}("mode" => "new"),
    )
    raw_consumer_response = JSON3.read("""
    {
      "stream_name": "ORDERS",
      "name": "worker",
      "config": {
        "durable_name": "worker",
        "ack_policy": "explicit",
        "future_consumer_field": {"mode": "new"}
      }
    }
    """)
    @test N._assert_consumer_config_reflected!(raw_consumer_requested, raw_consumer_response).name == "worker"

    raw_consumer_response_with_server_nested = JSON3.read("""
    {
      "stream_name": "ORDERS",
      "name": "worker",
      "config": {
        "durable_name": "worker",
        "ack_policy": "explicit",
        "future_consumer_field": {"mode": "new", "server_default": true}
      }
    }
    """)
    @test N._assert_consumer_config_reflected!(
        raw_consumer_requested,
        raw_consumer_response_with_server_nested;
        allow_unknown_field_extras=true,
    ).name == "worker"

    raw_consumer_missing = JSON3.read("""
    {
      "stream_name": "ORDERS",
      "name": "worker",
      "config": {
        "durable_name": "worker",
        "ack_policy": "explicit"
      }
    }
    """)
    @test_throws UnsupportedFeatureError N._assert_consumer_config_reflected!(
        raw_consumer_requested,
        raw_consumer_missing,
    )
end

@testitem "JetStream stream discovery validates subjects before request" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)

    @test_throws ArgumentError stream_names(js; subject="orders..created", timeout=0.001)
    @test TestHelpers.capture_text(capture) == ""

    TestHelpers.clear_capture!(capture)
    @test_throws ArgumentError pull_subscribe(js, "orders..created")
    @test TestHelpers.capture_text(capture) == ""

    TestHelpers.clear_capture!(capture)
    @test_throws ArgumentError push_subscribe(js, "orders..created")
    @test TestHelpers.capture_text(capture) == ""
end

@testitem "JetStream consumer config validation happens before request" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)

    @test_throws ArgumentError consumer_create(
        js,
        "ORDERS",
        ConsumerConfig(name="worker", deliver_subject="deliver.*");
        timeout=0.001,
    )
    @test TestHelpers.capture_text(capture) == ""
end

@testitem "JetStream pull consumer priority config validation" begin
    using Natter
    using Natter.JetStream

    const N = Natter

    info(config) = N.ConsumerInfo("ORDERS", "worker", config)

    policy, groups = N._validate_pull_consumer_priority_config(info(ConsumerConfig()))
    @test isnothing(policy)
    @test isempty(groups)

    policy, groups = N._validate_pull_consumer_priority_config(info(ConsumerConfig(
        priority_policy=PriorityPolicy.OVERFLOW,
        priority_groups=["fast", "slow"],
    )))
    @test policy == PriorityPolicy.OVERFLOW
    @test groups == ["fast", "slow"]

    @test_throws ProtocolError N._validate_pull_consumer_priority_config(info(ConsumerConfig(
        priority_policy=PriorityPolicy.OVERFLOW,
    )))
    @test_throws ProtocolError N._validate_pull_consumer_priority_config(info(ConsumerConfig(
        priority_groups=["fast"],
    )))
    @test_throws ProtocolError N._validate_pull_consumer_priority_config(info(ConsumerConfig(
        priority_policy=PriorityPolicy.OVERFLOW,
        priority_groups=["fast"],
        priority_timeout=1.0,
    )))
end

@testitem "JetStream stream purge validates filter locally" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)

    @test_throws ArgumentError stream_purge(js, "ORDERS"; filter_subject="orders..created", timeout=0.001)
    @test TestHelpers.capture_text(capture) == ""

    @test_throws ArgumentError stream_purge(js, "ORDERS"; keep=-1, timeout=0.001)
    @test TestHelpers.capture_text(capture) == ""

    @test_throws TimeoutError stream_purge(js, "ORDERS"; filter_subject="orders.created", keep=1, timeout=0.001)
    written = TestHelpers.capture_text(capture)
    json_match = match(r"\{.*\}", written)
    @test !isnothing(json_match)
    request = N._json_dict(json_match.match)
    @test request["filter"] == "orders.created"
    @test request["keep"] == 1
end

@testitem "JetStream push callback auto ack respects ack none policy" begin
    using Natter
    using Natter.JetStream

    const N = Natter

    info(policy) = N.ConsumerInfo("ORDERS", "worker", ConsumerConfig(ack_policy=policy))
    callback = _ -> nothing

    @test !N._push_callback_auto_ack(true, callback, info(AckPolicy.EXPLICIT))
    @test !N._push_callback_auto_ack(false, nothing, info(AckPolicy.EXPLICIT))
    @test !N._push_callback_auto_ack(false, callback, info(AckPolicy.NONE))
    @test N._push_callback_auto_ack(false, callback, info(AckPolicy.EXPLICIT))
    @test N._push_callback_auto_ack(false, callback, info(AckPolicy.ALL))
    @test N._push_callback_auto_ack(false, callback, info(nothing))
    @test !N._push_callback_auto_ack(false, callback, Dict{String,Any}("ack_policy" => "none"))
    @test N._push_callback_auto_ack(false, callback, Dict{String,Any}("ack_policy" => "explicit"))
    @test N._push_callback_auto_ack(false, callback, Dict{String,Any}())
end

@testitem "JetStream APIs accept abstract strings and callable objects" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    mutable struct JetStreamCallable
        subjects::Vector{String}
    end
    function (cb::JetStreamCallable)(msg::JetStreamMsg)
        push!(cb.subjects, msg.subject)
        nothing
    end
    mutable struct BorrowedJetStreamCallable
        subjects::Vector{String}
        borrowed::Bool
    end
    function (cb::BorrowedJetStreamCallable)(msg::BorrowedJetStreamMsg)
        push!(cb.subjects, msg.subject)
        cb.borrowed = msg.data isa SubArray
        nothing
    end

    stream = SubString("ORDERS.extra", 1, 6)
    durable = SubString("worker.extra", 1, 6)
    queue = SubString("workers.extra", 1, 7)
    callback = JetStreamCallable(String[])
    client = TestHelpers.fake_client()
    js = jetstream(client; prefix=SubString("\$JS.API.extra", 1, 7))
    @test js.prefix == "\$JS.API"

    wrapped = N._jetstream_push_callback(js, callback, false)
    wrapped(Msg("orders.created", "\$JS.ACK.ORDERS.worker.1.1.1.0.0", TestHelpers.bytes("work")))
    @test callback.subjects == ["orders.created"]

    borrowed_callback = BorrowedJetStreamCallable(String[], false)
    borrowed_wrapped = N._jetstream_push_callback(js, borrowed_callback, false; borrowed=true)
    data = TestHelpers.bytes("work")
    borrowed_wrapped(BorrowedMsg("orders.created", "\$JS.ACK.ORDERS.worker.1.1.1.0.0",
                                 @view(data[1:4]), nothing, 1, 0))
    @test borrowed_callback.subjects == ["orders.created"]
    @test borrowed_callback.borrowed

    ack_capture = TestHelpers.WriteCapture()
    ack_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=ack_capture)
    ack_js = jetstream(ack_client)
    ack_wrapped = N._jetstream_push_callback(ack_js, BorrowedJetStreamCallable(String[], false), true;
                                             borrowed=true)
    ack_wrapped(BorrowedMsg("orders.created", "ACK.REPLY", @view(data[1:4]), nothing, 1, 0))
    @test TestHelpers.capture_text(ack_capture) == "PUB ACK.REPLY 0\r\n\r\n"

    @test_throws ConnectionClosedError pull_subscribe(js, "orders.created"; stream, durable)
    @test_throws ArgumentError push_subscribe(
        js,
        "orders.created";
        stream,
        durable,
        queue,
        callback,
        config=ConsumerConfig(flow_control=true, idle_heartbeat=0.1),
    )

    req = N._stream_message_get_request(nothing, SubString("orders.created.extra", 1, 14), false)
    @test req["last_by_subj"] == "orders.created"
end

@testitem "JetStream in-progress ack is allowed only before terminal ack" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    function fresh_msg()
        capture = TestHelpers.WriteCapture()
        client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
        msg = JetStreamMsg(Msg("orders.created", "ACK.REPLY", TestHelpers.bytes("work")), client)
        msg, capture
    end

    progress_msg, progress_capture = fresh_msg()
    in_progress(progress_msg)
    in_progress(progress_msg)
    @test !N._acknowledged(progress_msg)
    @test TestHelpers.capture_text(progress_capture) ==
          "PUB ACK.REPLY 4\r\n+WPI\r\nPUB ACK.REPLY 4\r\n+WPI\r\n"

    for terminal_ack in (ack, msg -> nak(msg), term)
        terminal_msg, terminal_capture = fresh_msg()
        in_progress(terminal_msg)
        @test !N._acknowledged(terminal_msg)

        terminal_ack(terminal_msg)
        @test N._acknowledged(terminal_msg)
        written = TestHelpers.capture_text(terminal_capture)

        @test_throws JetStreamError in_progress(terminal_msg)
        @test TestHelpers.capture_text(terminal_capture) == written
        @test_throws JetStreamError terminal_ack(terminal_msg)
        @test TestHelpers.capture_text(terminal_capture) == written
    end
end

@testitem "borrowed JetStream messages share terminal ack guard" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    function fresh_msg()
        capture = TestHelpers.WriteCapture()
        client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
        data = TestHelpers.bytes("work")
        msg = BorrowedJetStreamMsg(BorrowedMsg("orders.created", "ACK.REPLY",
                                               @view(data[1:4]), nothing, 1, 0),
                                   client)
        msg, capture
    end

    progress_msg, progress_capture = fresh_msg()
    in_progress(progress_msg)
    in_progress(progress_msg)
    @test !N._acknowledged(progress_msg)
    @test TestHelpers.capture_text(progress_capture) ==
          "PUB ACK.REPLY 4\r\n+WPI\r\nPUB ACK.REPLY 4\r\n+WPI\r\n"

    for terminal_ack in (ack, msg -> nak(msg), term)
        terminal_msg, terminal_capture = fresh_msg()
        terminal_ack(terminal_msg)
        @test N._acknowledged(terminal_msg)
        written = TestHelpers.capture_text(terminal_capture)

        @test_throws JetStreamError in_progress(terminal_msg)
        @test_throws JetStreamError terminal_ack(terminal_msg)
        @test TestHelpers.capture_text(terminal_capture) == written
    end

    data = TestHelpers.bytes("work")
    auto_capture = TestHelpers.WriteCapture()
    auto_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=auto_capture)
    js = jetstream(auto_client)
    wrapped = N._jetstream_push_callback(js, msg -> ack(msg), true; borrowed=true)

    @test_throws JetStreamError wrapped(BorrowedMsg("orders.created", "ACK.REPLY",
                                                    @view(data[1:4]), nothing, 1, 0))
    @test TestHelpers.capture_text(auto_capture) == "PUB ACK.REPLY 0\r\n\r\n"
end

@testitem "JetStream terminal acks are not marked done while reconnecting" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    cases = (
        (ack, "PUB ACK.REPLY 0\r\n\r\n"),
        (msg -> nak(msg), "PUB ACK.REPLY 4\r\n-NAK\r\n"),
        (term, "PUB ACK.REPLY 5\r\n+TERM\r\n"),
    )

    for (terminal_ack, expected) in cases
        capture = TestHelpers.WriteCapture()
        client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING,
                                         write_io=capture)
        msg = JetStreamMsg(Msg("orders.created", "ACK.REPLY", TestHelpers.bytes("work")), client)

        @test_throws ConnectionReconnectingError terminal_ack(msg)
        @test !N._acknowledged(msg)
        @test client.pending_bytes == 0
        @test TestHelpers.capture_text(capture) == ""

        @lock client.lock N._store_status_locked!(client, N.ConnectionStatus.CONNECTED)
        terminal_ack(msg)
        @test N._acknowledged(msg)
        @test TestHelpers.capture_text(capture) == expected
    end
end

@testitem "JetStream terminal acks flush buffered transports before marking done" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    transport = TestHelpers.WriteCapture()
    opts = N.ConnectOptions(write_buffer_size=1024 * 1024)
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED,
                                     write_io=N.BufferedWriteIO(transport))
    msg = JetStreamMsg(Msg("orders.created", "ACK.REPLY", TestHelpers.bytes("work")), client)

    ack(msg)

    @test N._acknowledged(msg)
    @test client.pending_bytes == 0
    @test TestHelpers.capture_text(transport) == "PUB ACK.REPLY 0\r\n\r\n"
end

@testitem "JetStream nak validates delay" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    function fresh_msg()
        capture = TestHelpers.WriteCapture()
        client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
        msg = JetStreamMsg(Msg("orders.created", "ACK.REPLY", TestHelpers.bytes("work")), client)
        msg, capture
    end

    for delay in (-0.001, Inf, -Inf, NaN, true, "1")
        msg, capture = fresh_msg()
        @test_throws ArgumentError nak(msg; delay)
        @test !N._acknowledged(msg)
        @test TestHelpers.capture_text(capture) == ""
    end

    msg, capture = fresh_msg()
    nak(msg; delay=0)
    @test N._acknowledged(msg)
    @test TestHelpers.capture_text(capture) == "PUB ACK.REPLY 16\r\n-NAK {\"delay\":0}\r\n"
end

@testitem "JetStream terminal ack guard is synchronized" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    mutable struct BlockingWriteCapture <: IO
        capture::TestHelpers.WriteCapture
        started::Channel{Nothing}
        release::Channel{Nothing}
        blocked::Bool
    end

    BlockingWriteCapture() =
        BlockingWriteCapture(TestHelpers.WriteCapture(), Channel{Nothing}(1), Channel{Nothing}(1), false)

    function blocking_write(t::BlockingWriteCapture, data)
        if !t.blocked
            t.blocked = true
            put!(t.started, nothing)
            take!(t.release)
        end
        write(t.capture, data)
    end
    Base.write(t::BlockingWriteCapture, byte::UInt8) = blocking_write(t, byte)
    Base.write(t::BlockingWriteCapture, data::Vector{UInt8}) = blocking_write(t, data)
    Base.write(t::BlockingWriteCapture, data::Base.CodeUnits{UInt8}) = blocking_write(t, data)
    Base.write(t::BlockingWriteCapture, data::Union{String,SubString{String}}) = blocking_write(t, data)
    Base.write(t::BlockingWriteCapture, data::AbstractString) = blocking_write(t, data)
    Base.write(t::BlockingWriteCapture, ch::Char) = blocking_write(t, ch)
    Base.flush(t::BlockingWriteCapture) = flush(t.capture)
    Base.close(t::BlockingWriteCapture) = close(t.capture)

    capture = BlockingWriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    msg = JetStreamMsg(Msg("orders.created", "ACK.REPLY", TestHelpers.bytes("work")), client)

    ack_result() = try
        ack(msg)
        nothing
    catch err
        err
    end

    first = Threads.@spawn ack_result()
    take!(capture.started)
    second = Threads.@spawn ack_result()
    for _ in 1:100
        yield()
    end

    put!(capture.release, nothing)
    results = (fetch(first), fetch(second))

    @test count(isnothing, results) == 1
    @test count(err -> err isa JetStreamError, results) == 1
    @test N._acknowledged(msg)
    @test TestHelpers.capture_text(capture.capture) == "PUB ACK.REPLY 0\r\n\r\n"
end

@testitem "JetStream push flow control requires positive heartbeat" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    client = TestHelpers.fake_client()
    js = jetstream(client)

    @test_throws ArgumentError push_subscribe(js, "orders.created"; stream="ORDERS",
                                              config=ConsumerConfig(flow_control=true))
    @test_throws ArgumentError push_subscribe(js, "orders.created"; stream="ORDERS",
                                              config=ConsumerConfig(flow_control=true, idle_heartbeat=0.0))
    @test_throws ArgumentError push_subscribe(js, "orders.created"; stream="ORDERS",
                                              config=Dict("flow_control" => true))

    cfg = Dict{String,Any}("flow_control" => true, "idle_heartbeat" => 1)
    @test N._validate_push_consumer_control_config!(cfg) === cfg
end

@testitem "JetStream pull rejects push delivery config before create" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)

    function rejects_without_write(config)
        TestHelpers.clear_capture!(capture)
        @test_throws ArgumentError pull_subscribe(js, "orders.created"; stream="ORDERS", config=config)
        @test TestHelpers.capture_text(capture) == ""
    end

    rejects_without_write(ConsumerConfig(deliver_subject="_INBOX.worker"))
    rejects_without_write(ConsumerConfig(deliver_group="workers"))
    rejects_without_write(Dict("deliver_subject" => "_INBOX.worker"))
    rejects_without_write(Dict("deliver_group" => "workers"))

    push_info = N._consumer_info(Dict{String,Any}(
        "stream_name" => "ORDERS",
        "name" => "worker",
        "config" => Dict{String,Any}(
            "deliver_subject" => "_INBOX.worker",
        ),
    ))
    queue_info = N._consumer_info(Dict{String,Any}(
        "stream_name" => "ORDERS",
        "name" => "worker",
        "config" => Dict{String,Any}(
            "deliver_group" => "workers",
        ),
    ))

    @test_throws ArgumentError N._validate_existing_pull_consumer(push_info)
    @test_throws ArgumentError N._validate_existing_pull_consumer(queue_info)
end

@testitem "JetStream push queue configuration is resolved before create" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    cfg = Dict{String,Any}("deliver_group" => "workers")
    @test N._resolve_push_queue!(cfg, nothing) == "workers"
    @test cfg["deliver_group"] == "workers"

    cfg = Dict{String,Any}()
    @test N._resolve_push_queue!(cfg, "workers") == "workers"
    @test cfg["deliver_group"] == "workers"

    cfg = Dict{String,Any}("deliver_group" => "workers")
    @test N._resolve_push_queue!(cfg, "workers") == "workers"
    @test_throws ArgumentError N._resolve_push_queue!(Dict{String,Any}("deliver_group" => "other"), "workers")
    @test_throws ArgumentError N._resolve_push_queue!(Dict{String,Any}("deliver_group" => 1), nothing)
    @test_throws ArgumentError N._resolve_push_queue!(Dict{String,Any}("deliver_group" => ""), nothing)

    cfg = Dict{String,Any}()
    bind_fields = Set{String}(keys(cfg))
    queue = N._resolve_push_queue!(cfg, "workers")
    @test N._default_push_queue_consumer!(cfg, bind_fields, queue) === cfg
    @test cfg["name"] == "workers"
    @test cfg["durable_name"] == "workers"
    @test "durable_name" in bind_fields

    cfg = Dict{String,Any}("deliver_group" => "workers")
    bind_fields = Set{String}(keys(cfg))
    queue = N._resolve_push_queue!(cfg, nothing)
    @test N._default_push_queue_consumer!(cfg, bind_fields, queue) === cfg
    @test cfg["name"] == "workers"
    @test cfg["durable_name"] == "workers"
    @test "durable_name" in bind_fields

    cfg = Dict{String,Any}("name" => "custom", "deliver_group" => "workers")
    bind_fields = Set{String}(keys(cfg))
    @test N._default_push_queue_consumer!(cfg, bind_fields, "workers") === cfg
    @test cfg["name"] == "custom"
    @test !haskey(cfg, "durable_name")
    @test !("durable_name" in bind_fields)

    cfg = Dict{String,Any}()
    bind_fields = Set{String}(keys(cfg))
    @test_throws ArgumentError N._default_push_queue_consumer!(cfg, bind_fields, "workers.v1")
end

@testitem "JetStream queue push rejects heartbeat and flow control" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    client = TestHelpers.fake_client()
    js = jetstream(client)

    @test_throws ArgumentError push_subscribe(js, "orders.created"; stream="ORDERS", queue="workers",
                                              config=ConsumerConfig(idle_heartbeat=0.1))
    @test_throws ArgumentError push_subscribe(js, "orders.created"; stream="ORDERS", queue="workers",
                                              config=ConsumerConfig(flow_control=true, idle_heartbeat=0.1))
    @test_throws ArgumentError push_subscribe(js, "orders.created"; stream="ORDERS",
                                              config=ConsumerConfig(deliver_group="workers", idle_heartbeat=0.1))
    @test_throws ArgumentError push_subscribe(js, "orders.created"; stream="ORDERS",
                                              config=ConsumerConfig(deliver_group="workers", flow_control=true, idle_heartbeat=0.1))
    @test_throws ArgumentError push_subscribe(js, "orders.created"; stream="ORDERS", queue="workers",
                                              config=ConsumerConfig(deliver_group="other"))

    cfg = Dict{String,Any}("flow_control" => true, "idle_heartbeat" => 1)
    @test N._validate_push_queue_control_config!(cfg, nothing) === cfg
    @test_throws ArgumentError N._validate_push_queue_control_config!(cfg, "workers")
    @test_throws ArgumentError N._validate_push_queue_control_config!(Dict{String,Any}("idle_heartbeat" => 1), "workers")

    flow_info = N._consumer_info(Dict{String,Any}(
        "stream_name" => "ORDERS",
        "name" => "worker",
        "config" => Dict{String,Any}(
            "deliver_subject" => "_INBOX.worker",
            "deliver_group" => "workers",
            "flow_control" => true,
        ),
    ))
    heartbeat_info = N._consumer_info(Dict{String,Any}(
        "stream_name" => "ORDERS",
        "name" => "worker",
        "config" => Dict{String,Any}(
            "deliver_subject" => "_INBOX.worker",
            "deliver_group" => "workers",
            "idle_heartbeat" => 1_000_000_000,
        ),
    ))
    @test_throws ArgumentError N._validate_existing_push_queue_control(flow_info, "workers")
    @test_throws ArgumentError N._validate_existing_push_queue_control(heartbeat_info, "workers")
end

@testitem "JetStream push rejects binding active non-queue consumer" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    inactive = N._consumer_info(Dict{String,Any}(
        "stream_name" => "ORDERS",
        "name" => "worker",
        "push_bound" => false,
        "config" => Dict{String,Any}(
            "deliver_subject" => "_INBOX.worker",
        ),
    ))
    active = N._consumer_info(Dict{String,Any}(
        "stream_name" => "ORDERS",
        "name" => "worker",
        "push_bound" => true,
        "config" => Dict{String,Any}(
            "deliver_subject" => "_INBOX.worker",
        ),
    ))
    active_queue = N._consumer_info(Dict{String,Any}(
        "stream_name" => "ORDERS",
        "name" => "workers",
        "push_bound" => true,
        "config" => Dict{String,Any}(
            "deliver_subject" => "_INBOX.workers",
            "deliver_group" => "workers",
        ),
    ))

    @test N._validate_existing_push_bind(inactive, nothing, false) === inactive
    @test_throws ArgumentError N._validate_existing_push_bind(active, nothing, false)
    @test_throws ArgumentError N._validate_existing_push_bind(active_queue, "workers", false)
    @test_throws ArgumentError N._validate_existing_push_bind(active_queue, "other", true)
    @test N._validate_existing_push_bind(active_queue, "workers", true) === active_queue
end

@testitem "JetStream push subscribe installs core subscription before creating consumer" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(;
        status=N.ConnectionStatus.CONNECTED,
        info=N.ServerInfo(; headers=true, version="2.10.0"),
        write_io=capture,
    )
    js = jetstream(client; timeout=0.001)

    @test_throws TimeoutError push_subscribe(js, "orders.created"; stream="ORDERS",
                                             config=Dict("deliver_subject" => "deliver.test",
                                                         "ack_policy" => AckPolicy.NONE))

    written = TestHelpers.capture_text(capture)
    sub_range = findfirst("SUB deliver.test ", written)
    create_range = findfirst("PUB \$JS.API.CONSUMER.CREATE.ORDERS", written)
    @test sub_range !== nothing
    @test create_range !== nothing
    @test first(sub_range) < first(create_range)
    @test occursin("UNSUB ", written)
end

@testitem "JetStream public ordered push subscribe configures ordered consumer" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(;
        status=N.ConnectionStatus.CONNECTED,
        info=N.ServerInfo(; headers=true, version="2.10.0"),
        write_io=capture,
    )
    js = jetstream(client; timeout=0.001)

    @test_throws TimeoutError push_subscribe(js, "orders.created"; stream="ORDERS", ordered=true,
                                             config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW))

    written = TestHelpers.capture_text(capture)
    @test occursin("SUB _INBOX.", written)
    @test occursin("PUB \$JS.API.CONSUMER.CREATE.ORDERS", written)
    @test occursin("\"filter_subject\":\"orders.created\"", written)
    @test occursin("\"deliver_policy\":\"new\"", written)
    @test occursin("\"ack_policy\":\"none\"", written)
    @test occursin("\"flow_control\":true", written)
    @test occursin("\"idle_heartbeat\":5000000000", written)
    @test occursin("\"max_deliver\":1", written)
    @test occursin("\"num_replicas\":1", written)
    @test occursin("\"mem_storage\":true", written)
    @test !occursin("\"durable_name\"", written)
    @test occursin("UNSUB ", written)
end

@testitem "JetStream ordered push reset clears stale start time" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    opts = ConnectOptions(error_cb=_ -> nothing)
    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(;
        opts,
        status=N.ConnectionStatus.CONNECTED,
        info=N.ServerInfo(; headers=true, version="2.10.0"),
        write_io=capture,
    )
    js = jetstream(client; timeout=0.001)
    handler = N._JetStreamPushControlHandler()
    @lock handler.lock handler.ordered = true
    sub = subscribe(client, "_INBOX.ordered"; _control_handler=handler)
    psub = N.PushSubscription(js, sub, "ORDERS", "OLD", ReentrantLock(), false, false, nothing, handler)
    base_config = N._js_config_payload(ConsumerConfig(
        deliver_policy=DeliverPolicy.BY_START_TIME,
        opt_start_time="2026-01-02T03:04:05Z",
        filter_subject="orders.created",
    ))
    N._prepare_ordered_push_consumer_config!(base_config, nothing)
    TestHelpers.clear_capture!(capture)

    try
        N._ordered_push_reset_task(psub, base_config, 42)

        written = TestHelpers.capture_text(capture)
        json_match = match(r"\{.*\}", written)
        @test !isnothing(json_match)
        request = N._json_dict(json_match.match)
        config = N._consumer_normalized_config_value(request["config"])
        @test config["deliver_policy"] == "by_start_sequence"
        @test config["opt_start_seq"] == 42
        @test !haskey(config, "opt_start_time")
        @test base_config["deliver_policy"] == "by_start_time"
        @test haskey(base_config, "opt_start_time")
    finally
        close(psub)
    end
end

@testitem "JetStream ordered push reset rolls back subscription mapping after create failure" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    reported = Any[]
    opts = ConnectOptions(error_cb=err -> push!(reported, err))
    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(;
        opts,
        status=N.ConnectionStatus.CONNECTED,
        info=N.ServerInfo(; headers=true, version="2.10.0"),
        write_io=capture,
    )
    js = jetstream(client; timeout=0.001)
    handler = N._JetStreamPushControlHandler()
    @lock handler.lock handler.ordered = true
    sub = subscribe(client, "_INBOX.ordered"; _control_handler=handler)
    old_sid = sub.sid
    old_subject = sub.subject
    psub = N.PushSubscription(js, sub, "ORDERS", "OLD", ReentrantLock(), false, false, nothing, handler)
    base_config = N._js_config_payload(ConsumerConfig(filter_subject="orders.created"))
    N._prepare_ordered_push_consumer_config!(base_config, nothing)
    TestHelpers.clear_capture!(capture)

    try
        N._ordered_push_reset_task(psub, base_config, 42)

        mapped_sids = @lock client.lock sort!([sid for (sid, mapped) in client.subscriptions if mapped === sub])
        @test sub.sid == old_sid
        @test sub.subject == old_subject
        @test sub.server_active
        @test mapped_sids == [old_sid]
        written = TestHelpers.capture_text(capture)
        @test occursin("SUB _INBOX.ordered $old_sid\r\n", written)
        @test any(err -> err isa N.CleanupError && err.operation == "reset ordered push consumer", reported)
    finally
        close(psub)
    end
end

@testitem "JetStream ordered push subscribe rejects unsupported options before write" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client; timeout=0.001)

    function rejects_without_write(f)
        TestHelpers.clear_capture!(capture)
        @test_throws ArgumentError f()
        @test TestHelpers.capture_text(capture) == ""
    end

    rejects_without_write(() -> push_subscribe(js, "orders.created"; stream="ORDERS",
                                               ordered=true, durable="worker"))
    rejects_without_write(() -> push_subscribe(js, "orders.created"; stream="ORDERS",
                                               ordered=true, queue="workers"))
    rejects_without_write(() -> push_subscribe(js, "orders.created"; stream="ORDERS",
                                               ordered=true, config=ConsumerConfig(name="worker")))
    rejects_without_write(() -> push_subscribe(js, "orders.created"; stream="ORDERS",
                                               ordered=true, config=ConsumerConfig(deliver_subject="_INBOX.worker")))
    rejects_without_write(() -> push_subscribe(js, "orders.created"; stream="ORDERS",
                                               ordered=true, config=ConsumerConfig(ack_policy=AckPolicy.EXPLICIT)))
    rejects_without_write(() -> push_subscribe(js, "orders.created"; stream="ORDERS",
                                               ordered=true, config=ConsumerConfig(max_deliver=2)))
end

@testitem "JetStream push control dispatch filters heartbeats" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    push_sub = subscribe(client, "_INBOX.push"; _control_handler=N._JetStreamPushControlHandler())

    heartbeat = Msg("_INBOX.push", "_INBOX.hb", UInt8[];
                    headers=Headers("Status" => ["100"], "Description" => ["Idle Heartbeat"]),
                    sid=push_sub.sid)
    N._dispatch_msg(client, heartbeat)
    @test !isready(push_sub.messages)
    @test client.pending_bytes == 0

    plain_sub = subscribe(client, "_INBOX.plain")
    plain_heartbeat = Msg("_INBOX.plain", nothing, UInt8[];
                          headers=Headers("Status" => ["100"], "Description" => ["Idle Heartbeat"]),
                          sid=plain_sub.sid)
    N._dispatch_msg(client, plain_heartbeat)
    @test N._status_header(N.take!(plain_sub; timeout=0.1)) == 100

    close(push_sub)
    close(plain_sub)
end

@testitem "borrowed JetStream push control dispatch handles status frames" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    errors = Channel{Any}(2)
    capture = TestHelpers.WriteCapture()
    opts = ConnectOptions(error_cb=err -> put!(errors, err))
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED, write_io=capture)
    payload = UInt8[]
    called = Ref(0)

    heartbeat_sub = subscribe(client, "_INBOX.borrowed.hb";
                              callback=_ -> (called[] += 1),
                              borrowed=true,
                              _control_handler=N._JetStreamPushControlHandler())
    TestHelpers.clear_capture!(capture)
    heartbeat = BorrowedMsg("_INBOX.borrowed.hb", nothing, @view(payload[:]),
                            Headers("Status" => ["100"],
                                    "Description" => ["Idle Heartbeat"],
                                    "Nats-Consumer-Stalled" => ["_INBOX.fc"]),
                            heartbeat_sub.sid, 0)
    N._dispatch_msg(client, heartbeat)

    @test called[] == 0
    @test !isready(heartbeat_sub.messages)
    @test TestHelpers.capture_text(capture) == "PUB _INBOX.fc 0\r\n\r\n"

    TestHelpers.clear_capture!(capture)
    flow_sub = subscribe(client, "_INBOX.borrowed.fc";
                         callback=_ -> (called[] += 1),
                         borrowed=true,
                         _control_handler=N._JetStreamPushControlHandler())
    TestHelpers.clear_capture!(capture)
    flow_control = BorrowedMsg("_INBOX.borrowed.fc", "_INBOX.flow", @view(payload[:]),
                               Headers("Status" => ["100"],
                                       "Description" => ["FlowControl Request"]),
                               flow_sub.sid, 0)
    N._dispatch_msg(client, flow_control)

    @test called[] == 0
    @test !isready(flow_sub.messages)
    @test TestHelpers.capture_text(capture) == "PUB _INBOX.flow 0\r\n\r\n"

    deleted_sub = subscribe(client, "_INBOX.borrowed.deleted";
                            callback=_ -> (called[] += 1),
                            borrowed=true,
                            _control_handler=N._JetStreamPushControlHandler())
    deleted = BorrowedMsg("_INBOX.borrowed.deleted", nothing, @view(payload[:]),
                          Headers("Status" => ["409"],
                                  "Description" => ["Consumer Deleted"]),
                          deleted_sub.sid, 0)
    N._dispatch_msg(client, deleted)

    @test called[] == 0
    @test !isready(deleted_sub.messages)
    @test (@lock deleted_sub.lock deleted_sub.closed)
    @test timedwait(1.0; pollint=0.01) do
        isready(errors)
    end != :timed_out
    @test take!(errors) isa JetStreamError

    close(heartbeat_sub)
    close(flow_sub)
end

@testitem "JetStream status controls classify lazy descriptions without allocation" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    heartbeat_raw = TestHelpers.bytes("NATS/1.0 100 \tIDLE HEARTBEAT \t\r\n\r\n")
    heartbeat = N.Msg("_INBOX.push", nothing, UInt8[], N.LazyHeaders(heartbeat_raw), 1, length(heartbeat_raw))

    @test N._jetstream_status_action(heartbeat) == (:idle_heartbeat, nothing)
    @test isnothing(heartbeat.headers.parsed)
    N._jetstream_status_action(heartbeat)
    @test @allocated(N._jetstream_status_action(heartbeat)) == 0

    deleted_raw = TestHelpers.bytes("NATS/1.0 409 Consumer Deleted\r\n\r\n")
    deleted = N.Msg("_INBOX.push", nothing, UInt8[], N.LazyHeaders(deleted_raw), 1, length(deleted_raw))
    action, err = N._jetstream_status_action(deleted)

    @test action == :consumer_deleted
    @test err isa JetStreamError
    @test err.description == "Consumer Deleted"
    @test isnothing(deleted.headers.parsed)
end

@testitem "JetStream push idle heartbeat replies to stalled consumers" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    sub = subscribe(client, "_INBOX.push"; _control_handler=N._JetStreamPushControlHandler())
    TestHelpers.clear_capture!(capture)

    heartbeat = Msg("_INBOX.push", nothing, UInt8[];
                    headers=Headers("Status" => ["100"],
                                    "Description" => ["Idle Heartbeat"],
                                    "Nats-Consumer-Stalled" => ["_INBOX.fc"]),
                    sid=sub.sid)
    N._dispatch_msg(client, heartbeat)

    @test !isready(sub.messages)
    @test TestHelpers.capture_text(capture) == "PUB _INBOX.fc 0\r\n\r\n"
    close(sub)
end

@testitem "JetStream push idle heartbeat reports consumer sequence mismatch" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    errors = Channel{Any}(1)
    opts = ConnectOptions(error_cb=err -> put!(errors, err))
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.RECONNECTING)
    handler = N._JetStreamPushControlHandler(60.0)
    sub = subscribe(client, "_INBOX.push"; _control_handler=handler)

    data = Msg("_INBOX.push", "\$JS.ACK.ORDERS.C1.1.10.1.123456789.2",
               TestHelpers.bytes("one"); sid=sub.sid)
    N._dispatch_msg(client, data)
    @test String(N.take!(sub; timeout=0.1)) == "one"
    @test !isready(errors)

    heartbeat = Msg("_INBOX.push", nothing, UInt8[];
                    headers=Headers("Status" => ["100"],
                                    "Description" => ["Idle Heartbeat"],
                                    "Nats-Last-Consumer" => ["2"]),
                    sid=sub.sid)
    N._dispatch_msg(client, heartbeat)

    @test isready(errors)
    err = take!(errors)
    @test err isa ConsumerSequenceMismatchError
    @test err.stream_resume_sequence == 10
    @test err.consumer_sequence == 1
    @test err.last_consumer_sequence == 2
    @test !isready(sub.messages)
    close(sub)
end

@testitem "JetStream push control dispatch maps lifecycle statuses" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    errors = Channel{Any}(4)
    opts = ConnectOptions(error_cb=err -> put!(errors, err))
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.RECONNECTING)

    deleted_sub = subscribe(client, "_INBOX.deleted"; _control_handler=N._JetStreamPushControlHandler())
    deleted = Msg("_INBOX.deleted", nothing, UInt8[];
                  headers=Headers("Status" => ["409"], "Description" => ["Consumer Deleted"]),
                  sid=deleted_sub.sid)
    N._dispatch_msg(client, deleted)

    @test !isready(deleted_sub.messages)
    @test (@lock client.lock deleted_sub.closed)
    @test timedwait(1.0; pollint=0.01) do
        isready(errors)
    end != :timed_out
    deleted_err = take!(errors)
    @test deleted_err isa JetStreamError
    @test deleted_err.code == 409
    @test occursin("Consumer Deleted", deleted_err.description)

    leader_sub = subscribe(client, "_INBOX.leader"; _control_handler=N._JetStreamPushControlHandler())
    leadership = Msg("_INBOX.leader", nothing, UInt8[];
                     headers=Headers("Status" => ["409"], "Description" => ["Leadership Change"]),
                     sid=leader_sub.sid)
    N._dispatch_msg(client, leadership)

    @test !isready(leader_sub.messages)
    @test !(@lock client.lock leader_sub.closed)
    @test timedwait(1.0; pollint=0.01) do
        isready(errors)
    end != :timed_out
    leadership_err = take!(errors)
    @test leadership_err isa JetStreamError
    @test leadership_err.code == 409
    @test occursin("Leadership Change", leadership_err.description)

    close(leader_sub)
end

@testitem "JetStream push flow control replies internally" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED,
                                     read_io=capture, write_io=capture)
    sub = subscribe(client, "_INBOX.push"; _control_handler=N._JetStreamPushControlHandler())
    TestHelpers.clear_capture!(capture)
    flow_control = Msg("_INBOX.push", "_INBOX.fc", UInt8[];
                       headers=Headers("Status" => ["100"], "Description" => ["FlowControl Request"]),
                       sid=sub.sid)

    N._dispatch_msg(client, flow_control)

    expected = "PUB _INBOX.fc 0\r\n\r\n"
    @test !isready(sub.messages)
    @test client.pending_bytes == 0
    @test TestHelpers.capture_text(capture) == expected
    close(sub)
end

@testitem "JetStream push flow control waits for channel delivery" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED,
                                     read_io=capture, write_io=capture)
    handler = N._JetStreamPushControlHandler()
    sub = subscribe(client, "_INBOX.push"; _control_handler=handler)
    TestHelpers.clear_capture!(capture)
    data = Msg("_INBOX.push", "\$JS.ACK.S.C.1.1.1.0.0", TestHelpers.bytes("work");
               sid=sub.sid)
    flow_control = Msg("_INBOX.push", "_INBOX.fc", UInt8[];
                       headers=Headers("Status" => ["100"], "Description" => ["FlowControl Request"]),
                       sid=sub.sid)

    N._dispatch_msg(client, data)
    @test handler.flow_incoming[] == 1
    @test (@lock handler.lock handler.last_stream_seq == 0)

    N._dispatch_msg(client, flow_control)

    @test isready(sub.messages)
    @test client.pending_bytes == 0
    @test TestHelpers.capture_text(capture) == ""
    @test String(N.take!(sub; timeout=0.1)) == "work"

    expected = "PUB _INBOX.fc 0\r\n\r\n"
    @test client.pending_bytes == 0
    @test TestHelpers.capture_text(capture) == expected
    close(sub)
end

@testitem "JetStream push take! returns ackable messages" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    js = jetstream(client)
    sub = subscribe(client, "_INBOX.push"; _control_handler=N._JetStreamPushControlHandler())
    psub = N.PushSubscription(js, sub, "S", "C", ReentrantLock(), false, false)

    try
        N._dispatch_msg(client, Msg("_INBOX.push", "\$JS.ACK.S.C.1.1.1.0.0", TestHelpers.bytes("work");
                                   sid=sub.sid))
        msg = N.take!(psub; timeout=0.1)
        @test msg isa JetStreamMsg
        @test fieldtype(typeof(msg), :_client) === typeof(client)
        @test String(msg) == "work"
    finally
        close(psub)
    end
end

@testitem "JetStream push take! rejects callback subscriptions" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    js = jetstream(client)
    sub = subscribe(client, "_INBOX.push"; callback=_ -> nothing,
                    _control_handler=N._JetStreamPushControlHandler())
    psub = N.PushSubscription(js, sub, "S", "C", ReentrantLock(), false, false)

    try
        err = TestHelpers.thrown_exception(() -> N.take!(psub; timeout=0.1))
        @test err isa ArgumentError
        @test occursin("callback", err.msg)
        task = Threads.@spawn TestHelpers.thrown_exception(() -> N.take!(psub; timeout=0.1))
        @test fetch(task) isa ArgumentError
    finally
        close(psub)
    end
end

@testitem "JetStream push flow control waits for callback delivery" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED,
                                     read_io=capture, write_io=capture)
    started = Channel{String}(2)
    release = Channel{Bool}(1)
    sub = subscribe(client, "_INBOX.callback";
                    callback=msg -> begin
                        put!(started, String(msg))
                        String(msg) == "busy" && take!(release)
                    end,
                    _control_handler=N._JetStreamPushControlHandler())
    TestHelpers.clear_capture!(capture)

    try
        busy = Msg("_INBOX.callback", "\$JS.ACK.S.C.1.1.1.0.0", TestHelpers.bytes("busy");
                   sid=sub.sid)
        data = Msg("_INBOX.callback", "\$JS.ACK.S.C.1.2.2.0.0", TestHelpers.bytes("work");
                   sid=sub.sid)
        flow_control = Msg("_INBOX.callback", "_INBOX.fc", UInt8[];
                           headers=Headers("Status" => ["100"], "Description" => ["FlowControl Request"]),
                           sid=sub.sid)

        N._dispatch_msg(client, busy)
        @test timedwait(1.0; pollint=0.01) do
            isready(started)
        end != :timed_out
        @test take!(started) == "busy"

        N._dispatch_msg(client, data)
        N._dispatch_msg(client, flow_control)
        @test client.pending_bytes == 0
        @test TestHelpers.capture_text(capture) == ""

        put!(release, true)
        expected = "PUB _INBOX.fc 0\r\n\r\n"
        @test timedwait(1.0; pollint=0.01) do
            TestHelpers.capture_text(capture) == expected
        end != :timed_out
        @test client.pending_bytes == 0

        @test timedwait(1.0; pollint=0.01) do
            isready(started)
        end != :timed_out
        @test take!(started) == "work"
    finally
        isready(release) || put!(release, true)
        close(sub)
    end
end

@testitem "JetStream push flow control reply failures use error callback" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    errors = Channel{Any}(1)
    opts = ConnectOptions(error_cb=err -> put!(errors, err))
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.RECONNECTING)
    sub = subscribe(client, "_INBOX.push"; _control_handler=N._JetStreamPushControlHandler())
    @lock client.lock N._store_status_locked!(client, N.ConnectionStatus.DISCONNECTED)

    flow_control = Msg("_INBOX.push", "_INBOX.fc", UInt8[];
                       headers=Headers("Status" => ["100"], "Description" => ["FlowControl Request"]),
                       sid=sub.sid)
    N._dispatch_msg(client, flow_control)

    @test !isready(sub.messages)
    @test timedwait(1.0; pollint=0.01) do
        isready(errors)
    end != :timed_out
    @test take!(errors) isa ConnectionClosedError
    close(sub)
end

@testitem "JetStream push heartbeat monitor reports missed heartbeats" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    errors = Channel{Any}(1)
    opts = ConnectOptions(error_cb=err -> put!(errors, err))
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    js = jetstream(client)
    handler = N._JetStreamPushControlHandler(0.02)
    sub = subscribe(client, "_INBOX.push"; _control_handler=handler)
    psub = N.PushSubscription(js, sub, "S", "C", ReentrantLock(), false, false)
    psub.heartbeat_task = N._start_push_heartbeat_monitor(psub, handler)

    try
        @test timedwait(1.0; pollint=0.01) do
            isready(errors)
        end != :timed_out
        err = take!(errors)
        @test err isa JetStreamError
        @test err.code == 408
        @test occursin("heartbeat", lowercase(err.description))
    finally
        close(psub)
    end
end

@testitem "JetStream push data refreshes heartbeat activity" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    handler = N._JetStreamPushControlHandler(60.0)
    sub = subscribe(client, "_INBOX.push"; _control_handler=handler)
    stale = time() - 10
    handler.last_seen[] = stale

    data = Msg("_INBOX.push", "\$JS.ACK.S.C.1.1.1.0.0", TestHelpers.bytes("work");
               sid=sub.sid)
    N._dispatch_msg(client, data)

    @test handler.last_seen[] > stale
    @test String(N.take!(sub; timeout=0.1)) == "work"
    close(sub)

    ordered_handler = N._JetStreamPushControlHandler(60.0)
    @lock ordered_handler.lock ordered_handler.ordered = true
    ordered_sub = subscribe(client, "_INBOX.ordered"; _control_handler=ordered_handler)
    ordered_stale = time() - 10
    ordered_handler.last_seen[] = ordered_stale

    ordered_data = Msg("_INBOX.ordered", "\$JS.ACK.S.C.1.1.1.0.0", TestHelpers.bytes("ordered");
                       sid=ordered_sub.sid)
    N._dispatch_msg(client, ordered_data)

    @test ordered_handler.last_seen[] > ordered_stale
    @test String(N.take!(ordered_sub; timeout=0.1)) == "ordered"
    close(ordered_sub)
end

@testitem "JetStream push heartbeat sequence tracking parses first data reply only" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    handler = N._JetStreamPushControlHandler(60.0)
    first = Msg("_INBOX.push", "\$JS.ACK.ORDERS.C1.1.10.4.123456789.2",
                TestHelpers.bytes("one"))
    second = Msg("_INBOX.push", "\$JS.ACK.not-valid-metadata",
                 TestHelpers.bytes("two"))

    N._record_subscription_data_received!(handler, first)
    @test (@lock handler.lock begin
        handler.sequence_state_anchored &&
            handler.next_consumer_seq == 5 &&
            handler.last_stream_seq == 10
    end)

    N._record_subscription_data_received!(handler, second)
    @test (@lock handler.lock begin
        handler.sequence_state_anchored &&
            handler.next_consumer_seq == 6 &&
            handler.last_stream_seq == 11
    end)
end

@testitem "JetStream push control dispatch does not invoke callbacks" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    received = Channel{String}(1)
    sub = subscribe(client, "_INBOX.callback";
                    callback=msg -> put!(received, String(msg)),
                    _control_handler=N._JetStreamPushControlHandler())

    heartbeat = Msg("_INBOX.callback", nothing, UInt8[];
                    headers=Headers("Status" => ["100"], "Description" => ["Idle Heartbeat"]),
                    sid=sub.sid)
    flow_control = Msg("_INBOX.callback", "_INBOX.fc", UInt8[];
                       headers=Headers("Status" => ["100"], "Description" => ["FlowControl Request"]),
                       sid=sub.sid)
    N._dispatch_msg(client, heartbeat)
    N._dispatch_msg(client, flow_control)

    @test timedwait(0.1; pollint=0.01) do
        isready(received)
    end == :timed_out

    N._dispatch_msg(client, Msg("_INBOX.callback", nothing, TestHelpers.bytes("work"); sid=sub.sid))
    @test timedwait(1.0; pollint=0.01) do
        isready(received)
    end != :timed_out
    @test take!(received) == "work"
    close(sub)
end

@testitem "JetStream consumer create update and upsert request actions" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    client = TestHelpers.fake_client(; info=N.ServerInfo(; headers=true, version="2.10.0"))
    js = jetstream(client)
    payload = N._js_config_payload(ConsumerConfig(name="worker", durable_name="worker", filter_subject="orders.created"))

    create_request = N._consumer_request_payload(js, "ORDERS", payload, "create")
    @test create_request["stream_name"] == "ORDERS"
    @test create_request["config"] === payload
    @test create_request["action"] == "create"

    update_request = N._consumer_request_payload(js, "ORDERS", payload, "update")
    @test update_request["stream_name"] == "ORDERS"
    @test update_request["config"] === payload
    @test update_request["action"] == "update"

    upsert_request = N._consumer_request_payload(js, "ORDERS", payload)
    @test upsert_request["stream_name"] == "ORDERS"
    @test upsert_request["config"] === payload
    @test !haskey(upsert_request, "action")

    @test N._consumer_create_subject(js, "ORDERS", payload) == "\$JS.API.CONSUMER.CREATE.ORDERS.worker.orders.created"
    @test_throws ArgumentError N._consumer_request_payload(js, "ORDERS", payload, "delete")

    old_client = TestHelpers.fake_client(; info=N.ServerInfo(; headers=true, version="2.9.22"))
    @test_throws UnsupportedFeatureError N._consumer_request_payload(jetstream(old_client), "ORDERS", payload, "create")
    @test_throws UnsupportedFeatureError N._consumer_request_payload(jetstream(old_client), "ORDERS", payload, "update")

    missing_version_client = TestHelpers.fake_client(; info=N.ServerInfo(; headers=true))
    invalid_version_client = TestHelpers.fake_client(; info=N.ServerInfo(; headers=true, version="unknown"))
    overflow_version_client = TestHelpers.fake_client(; info=N.ServerInfo(; headers=true, version="$(typemax(Int))0.0"))
    @test !N._server_supports_consumer_name(missing_version_client)
    @test !N._server_supports_consumer_name(invalid_version_client)
    @test !N._server_supports_consumer_name(overflow_version_client)
    @test !N._server_supports_consumer_action(missing_version_client)
    @test !N._server_supports_consumer_action(invalid_version_client)
    @test !N._server_supports_consumer_action(overflow_version_client)
    @test N._consumer_create_subject(jetstream(missing_version_client), "ORDERS", payload) == "\$JS.API.CONSUMER.DURABLE.CREATE.ORDERS.worker"
    @test_throws UnsupportedFeatureError N._consumer_request_payload(jetstream(missing_version_client), "ORDERS", payload, "create")
    @test_throws UnsupportedFeatureError N._consumer_request_payload(jetstream(invalid_version_client), "ORDERS", payload, "update")
end

@testitem "JetStream durable bind-or-create binds after create conflict" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream
    using JSON3

    const N = Natter

    function respond_next_request!(client, payload::AbstractString)
        result = timedwait(1.0; pollint=0.001) do
            @lock client.lock begin
                mux = client.request_mux
                !isnothing(mux) && !isempty(mux.waiters)
            end
        end
        @test result != :timed_out
        subject, sid = @lock client.lock begin
            mux = client.request_mux
            token = first(keys(mux.waiters))
            "$(mux.prefix).$token", mux.sub.sid
        end
        N._dispatch_msg(client, Msg(subject, nothing, TestHelpers.bytes(payload); sid))
    end

    consumer_response(; deliver_subject=nothing) = begin
        cfg = Dict{String,Any}(
            "name" => "worker",
            "durable_name" => "worker",
            "filter_subject" => "orders.created",
        )
        isnothing(deliver_subject) || (cfg["deliver_subject"] = deliver_subject)
        JSON3.write(Dict{String,Any}("stream_name" => "ORDERS", "name" => "worker", "config" => cfg))
    end

    missing = JSON3.write(Dict("error" => Dict("code" => 404, "description" => "consumer not found")))
    conflict = JSON3.write(Dict("error" => Dict("code" => 400, "err_code" => 10105, "description" => "consumer already exists")))

    pull_client = TestHelpers.fake_client(;
        status=N.ConnectionStatus.CONNECTED,
        info=N.ServerInfo(; headers=true, version="2.10.0"),
        write_io=IOBuffer(),
    )
    pull_js = jetstream(pull_client)
    pull_payload = N._js_config_payload(ConsumerConfig(name="worker", durable_name="worker", filter_subject="orders.created"))
    pull_task = Threads.@spawn N._bind_or_create_consumer(pull_js, "ORDERS", "worker", pull_payload, Set(["name", "durable_name", "filter_subject"]))
    respond_next_request!(pull_client, missing)
    respond_next_request!(pull_client, conflict)
    respond_next_request!(pull_client, consumer_response())

    pull_info, pull_created = fetch(pull_task)
    @test !pull_created
    @test pull_info.name == "worker"

    push_client = TestHelpers.fake_client(;
        status=N.ConnectionStatus.CONNECTED,
        info=N.ServerInfo(; headers=true, version="2.10.0"),
        write_io=IOBuffer(),
    )
    push_js = jetstream(push_client)
    push_task = Threads.@spawn push_subscribe(push_js, "orders.created"; stream="ORDERS", durable="worker")
    respond_next_request!(push_client, missing)
    respond_next_request!(push_client, conflict)
    respond_next_request!(push_client, consumer_response(deliver_subject="_INBOX.existing"))

    psub = fetch(push_task)
    try
        @test psub.consumer == "worker"
        @test psub.sub.subject == "_INBOX.existing"
        written = String(take!(push_client.write_io))
        @test occursin("UNSUB ", written)
        @test occursin("SUB _INBOX.existing ", written)
    finally
        close(psub)
    end
end

@testitem "JetStream direct get request validation and response lifting" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream
    using Dates

    const N = Natter

    @test N._stream_message_get_request(1, nothing, false) == Dict{String,Any}("seq" => 1)
    @test N._stream_message_get_request(nothing, "orders.created", false) == Dict{String,Any}("last_by_subj" => "orders.created")
    @test N._stream_message_get_request(big(2), "orders.created", true) == Dict{String,Any}("seq" => 2, "next_by_subj" => "orders.created")
    @test_throws ArgumentError N._stream_message_get_request(nothing, nothing, false)
    @test_throws ArgumentError N._stream_message_get_request(0, nothing, false)
    @test_throws ArgumentError N._stream_message_get_request(true, nothing, false)
    @test_throws ArgumentError N._stream_message_get_request(big(typemax(Int)) + 1, nothing, false)
    @test_throws ArgumentError N._stream_message_get_request(1, "orders.created", false)
    @test_throws ArgumentError N._stream_message_get_request(nothing, "orders.created", true)
    @test_throws ArgumentError N._stream_message_get_request(0, "orders.created", true)
    @test_throws ArgumentError N._stream_message_get_request(1, "orders.*", true)

    delete_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    delete_js = jetstream(delete_client)
    @test_throws ArgumentError stream_message_delete(delete_js, "ORDERS", 0)
    @test_throws ArgumentError stream_message_delete(delete_js, "ORDERS", true)
    @test String(take!(delete_client.write_io)) == ""
    @test isempty(delete_client.subscriptions)

    api_msg = N._stream_message_from_api_payload(Dict{String,Any}(
        "subject" => "orders.created",
        "seq" => 7,
        "data" => "cGF5bG9hZA==",
        "time" => "2026-01-01T00:00:00.123456789Z",
    ))
    @test api_msg isa StoredMsg
    @test api_msg.seq == 7
    @test api_msg.created == DateTime(2026, 1, 1, 0, 0, 0, 123)
    @test api_msg.subject == "orders.created"
    @test String(api_msg) == "payload"

    response = Msg("_INBOX.reply", nothing, Vector{UInt8}(codeunits("payload"));
                   headers=Headers(
                       "NATS-Stream" => ["ORDERS"],
                       "nats-subject" => ["orders.created"],
                       "nats-sequence" => ["7"],
                       "Nats-Time-Stamp" => ["2026-01-01T00:00:00Z"],
                       "x-test" => ["ok"],
                   ))
    msg = N._direct_message_response_info("\$JS.API.DIRECT.GET.ORDERS", response)
    @test msg.subject == "orders.created"
    @test String(msg) == "payload"
    @test N.header(msg, "X-Test") == "ok"
    @test isnothing(N.header(msg, "Nats-Sequence"))
    info_msg = N._direct_message_response_info("\$JS.API.DIRECT.GET.ORDERS", response)
    @test info_msg isa StoredMsg
    @test info_msg.subject == "orders.created"
    @test info_msg.seq == 7
    @test info_msg.created == DateTime(2026, 1, 1)

    missing_stream = Msg("_INBOX.reply", nothing, Vector{UInt8}(codeunits("payload"));
                         headers=Headers(
                             "nats-subject" => ["orders.created"],
                             "nats-sequence" => ["7"],
                             "Nats-Time-Stamp" => ["2026-01-01T00:00:00Z"],
                         ))
    @test_throws ProtocolError N._direct_message_response_info("\$JS.API.DIRECT.GET.ORDERS", missing_stream)

    not_found = Msg("_INBOX.reply", nothing, UInt8[]; headers=Headers("Status" => ["404"], "Description" => ["no message found"]))
    @test_throws JetStreamError N._direct_message_response_info("\$JS.API.DIRECT.GET.ORDERS", not_found)
    no_responders = Msg("_INBOX.reply", nothing, UInt8[]; headers=Headers("Status" => ["503"]))
    @test_throws NoRespondersError N._direct_message_response_info("\$JS.API.DIRECT.GET.MISSING", no_responders)
end

@testitem "JetStream public stored message get returns metadata" setup=[TestHelpers] begin
    using JSON3
    using Dates
    using Natter
    using Natter.JetStream

    const N = Natter

    function next_request(client)
        result = timedwait(1.0; pollint=0.001) do
            @lock client.lock begin
                mux = client.request_mux
                !isnothing(mux) && !isempty(mux.waiters)
            end
        end
        @test result != :timed_out
        @lock client.lock begin
            mux = client.request_mux
            token = first(keys(mux.waiters))
            ("$(mux.prefix).$token", mux.sub.sid)
        end
    end

    function respond_next_request!(client, payload::AbstractString)
        subject, sid = next_request(client)
        N._dispatch_msg(client, Msg(subject, nothing, TestHelpers.bytes(payload); sid))
    end

    function respond_next_request!(client, msg::Msg)
        subject, sid = next_request(client)
        N._dispatch_msg(client, Msg(subject, msg.reply, msg.data, msg.headers, sid))
    end

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED,
                                     info=N.ServerInfo(; headers=true),
                                     write_io=IOBuffer())
    js = jetstream(client)

    api_task = Threads.@spawn stream_message_get(js, "ORDERS"; seq=7)
    respond_next_request!(client, JSON3.write(Dict(
        "message" => Dict(
            "subject" => "orders.created",
            "seq" => 7,
            "data" => "cGF5bG9hZA==",
            "time" => "2026-01-01T00:00:00.123456789Z",
        ),
    )))
    api_msg = fetch(api_task)
    @test api_msg isa StoredMsg
    @test api_msg.subject == "orders.created"
    @test api_msg.seq == 7
    @test api_msg.created == DateTime(2026, 1, 1, 0, 0, 0, 123)
    @test String(api_msg) == "payload"

    direct_task = Threads.@spawn stream_message_get(js, "ORDERS"; seq=8, direct=true)
    direct_response = Msg("_INBOX.reply", nothing, TestHelpers.bytes("fast");
                          headers=Headers(
                              "Nats-Stream" => ["ORDERS"],
                              "Nats-Subject" => ["orders.created"],
                              "Nats-Sequence" => ["8"],
                              "Nats-Time-Stamp" => ["2026-01-01T00:00:01Z"],
                          ))
    respond_next_request!(client, direct_response)
    direct_msg = fetch(direct_task)
    @test direct_msg isa StoredMsg
    @test direct_msg.subject == "orders.created"
    @test direct_msg.seq == 8
    @test direct_msg.created == DateTime(2026, 1, 1, 0, 0, 1)
    @test String(direct_msg) == "fast"

    async_task = Threads.@spawn stream_message_get(js, "ORDERS"; seq=9)
    respond_next_request!(client, JSON3.write(Dict(
        "message" => Dict(
            "subject" => "orders.created",
            "seq" => 9,
            "data" => "YXN5bmM=",
            "time" => "2026-01-01T00:00:02Z",
        ),
    )))
    async_msg = fetch(async_task)
    @test async_msg isa StoredMsg
    @test async_msg.seq == 9
    @test async_msg.created == DateTime(2026, 1, 1, 0, 0, 2)
    @test String(async_msg) == "async"
end

@testitem "JetStream metadata" begin
    using Natter
    using Natter.JetStream

    const N = Natter

    msg = Msg("s", "\$JS.ACK.ORDERS.C1.2.10.4.123456789.7", UInt8[])
    meta = metadata(msg)
    @test meta.stream == "ORDERS"
    @test meta.consumer == "C1"
    @test meta.delivered == 2
    @test meta.stream_sequence == 10
    @test meta.consumer_sequence == 4
    @test meta.timestamp_ns == 123456789
    @test meta.pending == 7
    @test isnothing(meta.domain)
    @test N._parse_msg_metadata(msg).stream_sequence == 10
    let local_msg = msg
        parsed = @inferred N._parse_msg_metadata(local_msg)
        @test parsed.stream_sequence == 10
    end

    legacy_meta = metadata(Msg("s", "\$JS.ACK.ORDERS.C1.2.10.4.123456789", UInt8[]))
    @test legacy_meta.stream == "ORDERS"
    @test legacy_meta.consumer == "C1"
    @test legacy_meta.delivered == 2
    @test legacy_meta.stream_sequence == 10
    @test legacy_meta.consumer_sequence == 4
    @test legacy_meta.timestamp_ns == 123456789
    @test legacy_meta.pending == 0
    @test isnothing(legacy_meta.domain)

    meta2 = metadata(Msg("s", "\$JS.ACK._.acc.ORDERS.C1.3.11.5.987654321.8.rand", UInt8[]))
    @test meta2.domain == ""
    @test meta2.stream == "ORDERS"
    @test meta2.consumer == "C1"
    @test meta2.timestamp_ns == 987654321

    meta3 = metadata(Msg("s", "\$JS.ACK.HUB.acc.EVENTS.C2.4.12.6.987654322.9.rand.extra", UInt8[]))
    @test meta3.domain == "HUB"
    @test meta3.stream == "EVENTS"
    @test meta3.consumer == "C2"
    @test meta3.pending == 9
end

@testitem "JetStream ordered push control detects sequence gaps" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED)
    resets = Int[]
    handler = N._JetStreamPushControlHandler()
    @lock handler.lock begin
        handler.ordered = true
        handler.ordered_reset_callback = seq -> push!(resets, seq)
    end

    first_msg = Msg("orders.created", "\$JS.ACK.ORDERS.C1.1.10.1.123456789.2", UInt8[])
    @test !N._handle_ordered_push_data!(handler, client, first_msg)
    @test isempty(resets)

    gap_msg = Msg("orders.created", "\$JS.ACK.ORDERS.C1.1.12.3.123456790.0", UInt8[])
    @test N._handle_ordered_push_data!(handler, client, gap_msg)
    @test resets == [11]
    @test handler.ordered_resetting
end

@testitem "JetStream pull fetch validates inputs before publishing" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    js = jetstream(client)
    psub = N.PullSubscription(js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)

    @test_throws ArgumentError fetch(psub, 0; timeout=1.0)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, -1; timeout=1.0)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, true; timeout=1.0)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, big(typemax(Int)) + 1; timeout=1.0)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=0.0)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=-1.0)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=Inf)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=NaN)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=true)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, expires=0.0)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, expires=-1.0)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, expires=1.0)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, expires=2.0)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, expires=0.9, heartbeat=-0.1)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, expires=0.9, heartbeat=0.6)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, max_bytes=0)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, max_bytes=true)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, no_wait=1)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, min_pending=0)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, min_ack_pending=true)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, priority_group="")
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, priority_group="workers")
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, priority=10, priority_group="workers")
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, min_pending=1)
    @test client.pending_bytes == 0

    overflow_psub = N.PullSubscription(js, "ORDERS", "PRIORITY", ReentrantLock(), ReentrantLock(), false, false,
                                       PriorityPolicy.OVERFLOW, ["fast", "slow"])
    @test_throws ArgumentError fetch(overflow_psub, 1; timeout=1.0)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(overflow_psub, 1; timeout=1.0, priority_group="other")
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(overflow_psub, 1; timeout=1.0, priority_group="fast", priority=1)
    @test client.pending_bytes == 0

    prioritized_psub = N.PullSubscription(js, "ORDERS", "PRIORITIZED", ReentrantLock(), ReentrantLock(), false, false,
                                          PriorityPolicy.PRIORITIZED, ["fast"])
    validated = N._validate_pull_fetch(prioritized_psub, 1, 1.0, 0.9, 0, nothing,
                                       false, nothing, nothing, "fast", 1)
    @test validated[end] == 1

    close(psub)
    @test_throws ConnectionClosedError fetch(psub, 1; timeout=1.0)
    @test client.pending_bytes == 0
end

@testitem "JetStream pull fetch serializes max bytes and no wait" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream
    using JSON3

    const N = Natter

    function pull_request_payload(frame::AbstractString)
        lines = split(frame, "\r\n"; keepempty=true)
        for i in 1:(length(lines) - 1)
            parts = split(lines[i])
            isempty(parts) && continue
            parts[1] == "PUB" || continue
            len = parse(Int, parts[end])
            payload = lines[i + 1]
            @test ncodeunits(payload) == len
            return JSON3.read(payload)
        end
        throw(AssertionError("PUB frame not found"))
    end

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    js = jetstream(client)
    psub = N.PullSubscription(js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false,
                              PriorityPolicy.OVERFLOW, ["workers"])

    try
        task = Threads.@spawn fetch(psub, big(10); timeout=10.0, heartbeat=0, max_bytes=256, no_wait=true,
                            min_pending=4, min_ack_pending=5, priority_group="workers")
        delivery = TestHelpers.wait_for_pull_delivery(psub)
        N._dispatch_msg(client, Msg(delivery.subject, nothing, UInt8[];
                                    headers=Headers("Status" => ["404"], "Description" => ["No Messages"]),
                                    sid=delivery.sid))
        @test isempty(fetch(task))
        payload = pull_request_payload(String(take!(client.write_io)))
        @test payload["batch"] == 10
        @test payload["max_bytes"] == 256
        @test payload["no_wait"] == true
        @test payload["expires"] == 9_000_000_000
        @test payload["min_pending"] == 4
        @test payload["min_ack_pending"] == 5
        @test payload["group"] == "workers"
    finally
        close(psub)
    end
end

@testitem "JetStream pull fetch expires server request before local timeout" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream
    using JSON3

    const N = Natter

    function pull_request_payload(frame::AbstractString)
        lines = split(frame, "\r\n"; keepempty=true)
        for i in 1:(length(lines) - 1)
            parts = split(lines[i])
            isempty(parts) && continue
            parts[1] == "PUB" || continue
            len = parse(Int, parts[end])
            payload = lines[i + 1]
            @test ncodeunits(payload) == len
            return JSON3.read(payload)
        end
        throw(AssertionError("PUB frame not found"))
    end

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    js = jetstream(client)
    psub = N.PullSubscription(js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)

    try
        task = Threads.@spawn fetch(psub, 1; timeout=10.0, heartbeat=0)
        delivery = TestHelpers.wait_for_pull_delivery(psub)
        N._dispatch_msg(client, Msg(delivery.subject, nothing, UInt8[];
                                    headers=Headers("Status" => ["404"], "Description" => ["No Messages"]),
                                    sid=delivery.sid))
        @test isempty(fetch(task))
        default_payload = pull_request_payload(String(take!(client.write_io)))
        @test default_payload["batch"] == 1
        @test default_payload["expires"] == 9_000_000_000
        @test !haskey(default_payload, "idle_heartbeat")

        task = Threads.@spawn fetch(psub, 1; timeout=10.0, expires=2.5, heartbeat=0)
        delivery = TestHelpers.wait_for_pull_delivery(psub)
        N._dispatch_msg(client, Msg(delivery.subject, nothing, UInt8[];
                                    headers=Headers("Status" => ["404"], "Description" => ["No Messages"]),
                                    sid=delivery.sid))
        @test isempty(fetch(task))
        explicit_payload = pull_request_payload(String(take!(client.write_io)))
        @test explicit_payload["expires"] == 2_500_000_000
    finally
        close(psub)
    end
end

@testitem "JetStream pull fetch publishes concurrent requests independently" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    function request_count(capture)
        length(collect(eachmatch(r"CONSUMER.MSG.NEXT", TestHelpers.capture_text(capture))))
    end

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)
    TestHelpers.clear_capture!(capture)
    psub = N.PullSubscription(js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)

    first = Threads.@spawn fetch(psub, 1; timeout=0.75, heartbeat=0)
    try
        @test timedwait(1.0; pollint=0.001) do
            request_count(capture) >= 1
        end != :timed_out
        @test !istaskdone(first)

        second = Threads.@spawn fetch(psub, 1; timeout=0.75, heartbeat=0)
        @test timedwait(0.25; pollint=0.001) do
            request_count(capture) >= 2
        end != :timed_out
        @test !istaskdone(first)

        @test isempty(fetch(first))
        @test isempty(fetch(second))
    finally
        close(psub)
    end
end

@testitem "JetStream continuous pull validates inputs and excludes fetch" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)
    TestHelpers.clear_capture!(capture)
    psub = N.PullSubscription(js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)

    @test_throws ArgumentError messages(psub; batch=0)
    @test_throws ArgumentError messages(psub; batch=true)
    @test_throws ArgumentError messages(psub; batch=big(typemax(Int)) + 1)
    @test_throws ArgumentError messages(psub; batch=1, max_bytes=0)
    @test_throws ArgumentError messages(psub; batch=1, max_bytes=true)
    @test_throws ArgumentError messages(psub; batch=2, threshold_messages=3)
    @test_throws ArgumentError messages(psub; batch=2, threshold_bytes=1)
    @test_throws ArgumentError messages(psub; batch=2, max_bytes=10, threshold_bytes=11)
    @test_throws ArgumentError messages(psub; batch=1, channel_size=0)
    @test_throws ArgumentError messages(psub; batch=1, stop_after=0)
    @test_throws ArgumentError messages(psub; batch=1, min_pending=1)
    @test_throws ArgumentError messages(psub; batch=1, priority_group="bad group")
    @test_throws ArgumentError messages(psub; batch=1, priority_group="workers")
    @test_throws ArgumentError messages(psub; batch=1, priority=-1, priority_group="workers")

    priority_psub = N.PullSubscription(js, "ORDERS", "PRIORITY", ReentrantLock(), ReentrantLock(), false, false,
                                       PriorityPolicy.PINNED_CLIENT, ["workers"])
    @test_throws ArgumentError messages(priority_psub; batch=1)
    @test_throws ArgumentError messages(priority_psub; batch=1, priority_group="other")
    @test_throws ArgumentError messages(priority_psub; batch=1, priority_group="workers", min_pending=1)

    big_stream = messages(psub; batch=big(1), expires=1.0, heartbeat=0, stop_after=1)
    close(big_stream)
    @test timedwait(1.0; pollint=0.001) do
        !(@lock psub.close_lock psub.active_stream)
    end != :timed_out

    stream = messages(psub; batch=1, expires=1.0, heartbeat=0, stop_after=1)
    try
        @test timedwait(1.0; pollint=0.001) do
            occursin("CONSUMER.MSG.NEXT", TestHelpers.capture_text(capture))
        end != :timed_out
        @test_throws ArgumentError fetch(psub, 1; timeout=0.1, heartbeat=0)
    finally
        close(stream)
        wait(stream)
        close(psub)
    end
end

@testitem "JetStream continuous pull take supports timeout and cancellation" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)
    TestHelpers.clear_capture!(capture)
    psub = N.PullSubscription(js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)

    stream = messages(psub; batch=1, expires=1.0, heartbeat=0, stop_after=1)
    try
        @test timedwait(1.0; pollint=0.001) do
            occursin("CONSUMER.MSG.NEXT", TestHelpers.capture_text(capture))
        end != :timed_out

        @test_throws TimeoutError take!(stream; timeout=0.01)
        @test isopen(stream)

        source = CancellationSource()
        token = cancellation_token(source)
        task = Threads.@spawn TestHelpers.thrown_exception() do
            take!(stream; cancel_token=token)
        end
        @test timedwait(0.05; pollint=0.001) do
            istaskdone(task)
        end == :timed_out
        @test cancel!(source)
        @test timedwait(1.0; pollint=0.001) do
            istaskdone(task)
        end == :ok
        @test fetch(task) isa CancelledError
        @test isopen(stream)

        N._dispatch_msg(client, Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.1.1.0.0",
                                    TestHelpers.bytes("one"); sid=stream.delivery.sid))
        @test String(take!(stream; timeout=1.0)) == "one"
        wait(stream)
        @test !(@lock psub.close_lock psub.active_stream)
    finally
        close(stream)
        close(psub)
    end
end

@testitem "JetStream continuous pull publishes outside stream state lock" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    mutable struct BlockingPullRequestTransport <: IO
        capture::TestHelpers.WriteCapture
        started::Channel{Nothing}
        release::Channel{Nothing}
        enabled::Bool
        blocked::Bool
    end

    BlockingPullRequestTransport() =
        BlockingPullRequestTransport(TestHelpers.WriteCapture(), Channel{Nothing}(1),
                                     Channel{Nothing}(1), false, false)

    function blocking_request_write(t::BlockingPullRequestTransport, data)
        text = data isa UInt8 ? string(Char(data)) : data isa Char ? string(data) : String(data)
        if t.enabled && !t.blocked && occursin("CONSUMER.MSG.NEXT", text)
            t.blocked = true
            put!(t.started, nothing)
            take!(t.release)
        end
        write(t.capture, data)
    end

    Base.write(t::BlockingPullRequestTransport, byte::UInt8) = blocking_request_write(t, byte)
    Base.write(t::BlockingPullRequestTransport, data::Vector{UInt8}) = blocking_request_write(t, data)
    Base.write(t::BlockingPullRequestTransport, data::Base.CodeUnits{UInt8}) = blocking_request_write(t, data)
    Base.write(t::BlockingPullRequestTransport, data::Union{String,SubString{String}}) = blocking_request_write(t, data)
    Base.write(t::BlockingPullRequestTransport, data::AbstractString) = blocking_request_write(t, data)
    Base.write(t::BlockingPullRequestTransport, ch::Char) = blocking_request_write(t, ch)
    Base.flush(t::BlockingPullRequestTransport) = flush(t.capture)
    Base.close(t::BlockingPullRequestTransport) = close(t.capture)

    function release_transport!(t::BlockingPullRequestTransport)
        isready(t.release) || put!(t.release, nothing)
        nothing
    end

    transport = BlockingPullRequestTransport()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=transport)
    js = jetstream(client)
    TestHelpers.clear_capture!(transport.capture)

    transport.enabled = true
    psub = N.PullSubscription(js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)

    stream = messages(psub; batch=1, expires=1.0, heartbeat=0, stop_after=1)
    stream_done = Ref(:timed_out)
    close_task = Ref{Union{Task,Nothing}}(nothing)
    try
        @test timedwait(1.0; pollint=0.001) do
            isready(transport.started)
        end != :timed_out

        close_task[] = Threads.@spawn close(stream)
        @test timedwait(0.2; pollint=0.001) do
            N._pull_stream_closed(stream.state)
        end != :timed_out
        release_transport!(transport)
        @test timedwait(1.0; pollint=0.001) do
            istaskdone(close_task[]::Task)
        end != :timed_out
        @test fetch(close_task[]::Task) === nothing
    finally
        release_transport!(transport)
        if isnothing(close_task[]) || istaskdone(close_task[]::Task)
            close(stream)
        end
        stream_done[] = timedwait(1.0; pollint=0.001) do
            istaskdone(stream.task)
        end
        stream_done[] == :timed_out || wait(stream)
        close(psub)
    end
    @test stream_done[] != :timed_out
end

@testitem "JetStream continuous pull refills at message thresholds" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    function request_count(capture)
        length(collect(eachmatch(r"CONSUMER.MSG.NEXT", TestHelpers.capture_text(capture))))
    end

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)
    TestHelpers.clear_capture!(capture)
    psub = N.PullSubscription(js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)

    stream = messages(psub; batch=2, expires=1.0, heartbeat=0, threshold_messages=1,
                      channel_size=4, stop_after=3)
    try
        @test timedwait(1.0; pollint=0.001) do
            request_count(capture) >= 1
        end != :timed_out
        N._dispatch_msg(client, Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.1.1.0.0", TestHelpers.bytes("one");
                                    sid=stream.delivery.sid))
        @test String(take!(stream)) == "one"
        @test timedwait(1.0; pollint=0.001) do
            request_count(capture) >= 2
        end != :timed_out

        N._dispatch_msg(client, Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.2.2.0.0", TestHelpers.bytes("two");
                                    sid=stream.delivery.sid))
        N._dispatch_msg(client, Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.3.3.0.0", TestHelpers.bytes("three");
                                    sid=stream.delivery.sid))
        @test String(take!(stream)) == "two"
        @test String(take!(stream)) == "three"
        wait(stream)
        @test !psub.active_stream
    finally
        close(stream)
        close(psub)
    end
end

@testitem "JetStream continuous pull treats max bytes status as terminal" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    function request_count(capture)
        length(collect(eachmatch(r"CONSUMER.MSG.NEXT", TestHelpers.capture_text(capture))))
    end

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)
    TestHelpers.clear_capture!(capture)
    psub = N.PullSubscription(js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)

    stream = messages(psub; batch=1, max_bytes=8, expires=1.0, heartbeat=0, channel_size=1)
    try
        @test timedwait(1.0; pollint=0.001) do
            request_count(capture) >= 1
        end != :timed_out

        max_bytes = Msg(stream.delivery.subject, nothing, UInt8[];
                        headers=Headers("Status" => ["409"],
                                        "Description" => ["Message Size Exceeds MaxBytes"],
                                        "Nats-Pending-Messages" => ["1"],
                                        "Nats-Pending-Bytes" => ["8"]),
                        sid=stream.delivery.sid)
        N._dispatch_msg(client, max_bytes)

        @test timedwait(1.0; pollint=0.001) do
            request_count(capture) >= 2
        end != :timed_out
        close(stream)
        wait(stream)
    finally
        close(stream)
        close(psub)
    end
end

@testitem "JetStream continuous pull accounting uses aggregate request credits" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    state = N._PullMessageStreamState()
    config = N._PullStreamConfig(2, nothing, 1.0, 0.0, 2, nothing,
                                 nothing, nothing, nothing, nothing, nothing, 4)

    first = N._reserve_pull_stream_request!(config, state)
    @test !isnothing(first)
    @test state.requested_messages == 2
    @test state.requested_bytes == 0
    @test length(state.requests) == 1

    msg = Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.1.1.0.0", TestHelpers.bytes("one"))
    @lock state.lock N._pull_stream_decrement_requested!(state, msg)
    @test state.requested_messages == 1
    @test length(state.requests) == 1

    second = N._reserve_pull_stream_request!(config, state)
    @test !isnothing(second)
    @test state.requested_messages == 3
    @test length(state.requests) == 2

    terminal = Msg("_INBOX.pull.1", nothing, UInt8[];
                   headers=Headers("Status" => ["404"], "Description" => ["No Messages"],
                                   "Nats-Pending-Messages" => ["1"]))
    @lock state.lock begin
        @test N._pull_stream_release_terminal_request!(state, terminal)
        @test state.requested_messages == 2
        @test length(state.requests) == 1
    end
end

@testitem "JetStream continuous pull accounting tracks byte credits from status headers" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    state = N._PullMessageStreamState()
    config = N._PullStreamConfig(4, 20, 1.0, 0.0, 0, 10,
                                 nothing, nothing, nothing, nothing, nothing, 4)

    first = N._reserve_pull_stream_request!(config, state)
    @test !isnothing(first)
    @test state.requested_messages == 4
    @test state.requested_bytes == 20

    msg = Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.1.1.0.0", TestHelpers.bytes("abcdefghijkl"))
    @lock state.lock N._pull_stream_decrement_requested!(state, msg)
    @test state.requested_messages == 3
    @test state.requested_bytes == 8

    second = N._reserve_pull_stream_request!(config, state)
    @test !isnothing(second)
    @test state.requested_messages == 4
    @test state.requested_bytes == 20

    terminal = Msg("_INBOX.pull.1", nothing, UInt8[];
                   headers=Headers("Status" => ["409"], "Description" => ["Batch Completed"],
                                   "Nats-Pending-Messages" => ["3"],
                                   "Nats-Pending-Bytes" => ["8"]))
    @lock state.lock begin
        @test N._pull_stream_release_terminal_request!(state, terminal)
        @test state.requested_messages == 1
        @test state.requested_bytes == 12
    end
end

@testitem "JetStream continuous pull put consumes request credit before terminal release" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED,
                                     write_io=TestHelpers.WriteCapture())
    js = jetstream(client)
    delivery = subscribe(client, "_INBOX.pull")
    psub = N.PullSubscription(js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)

    state = N._PullMessageStreamState()
    config = N._PullStreamConfig(2, 64, 1.0, 0.0, 1, 32,
                                 nothing, nothing, nothing, nothing, nothing, 2)
    queue_lock = ReentrantLock()
    queue_condition = Base.Threads.Condition(queue_lock)
    queue = N.MsgQueue{Msg}(2)
    stream_task = Threads.@spawn nothing
    stream = N.PullMessageStream{typeof(client),typeof(psub)}(
        psub, delivery, queue, queue_lock, queue_condition, config, UInt8[],
        stream_task, nothing, state)

    try
        reservation = N._reserve_pull_stream_request!(config, state)
        @test !isnothing(reservation)
        @test state.requested_messages == 2
        @test length(state.requests) == 1

        msg = Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.1.1.0.0", TestHelpers.bytes("one"))
        msg_bytes = N._pull_stream_msg_bytes(msg)
        N._pull_stream_put!(stream, msg)

        @lock state.lock begin
            @test state.requested_messages == 1
            @test state.requested_bytes == 64 - msg_bytes
            @test length(state.requests) == 1
            @test first(state.requests).remaining_messages == 1
            @test first(state.requests).remaining_bytes == 64 - msg_bytes
        end

        terminal = Msg("_INBOX.pull.1", nothing, UInt8[];
                       headers=Headers("Status" => ["409"], "Description" => ["Batch Completed"]))
        @lock state.lock begin
            @test N._pull_stream_release_terminal_request!(state, terminal)
            @test state.requested_messages == 0
            @test state.requested_bytes == 0
            @test isempty(state.requests)
        end
    finally
        close(stream)
        close(psub)
    end
end

@testitem "JetStream continuous pull keeps requested credits while queue put is blocked" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED,
                                     write_io=TestHelpers.WriteCapture())
    js = jetstream(client)
    delivery = subscribe(client, "_INBOX.pull")
    psub = N.PullSubscription(js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)

    state = N._PullMessageStreamState()
    config = N._PullStreamConfig(2, 32, 1.0, 0.0, 1, 16,
                                 nothing, nothing, nothing, nothing, nothing, 1)
    queue_lock = ReentrantLock()
    queue_condition = Base.Threads.Condition(queue_lock)
    queue = N.MsgQueue{Msg}(1)
    stream_task = Threads.@spawn nothing
    stream = N.PullMessageStream{typeof(client),typeof(psub)}(
        psub, delivery, queue, queue_lock, queue_condition, config, UInt8[],
        stream_task, nothing, state)

    first = Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.1.1.0.0", TestHelpers.bytes("first"))
    second = Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.2.2.0.0", TestHelpers.bytes("second"))
    first_bytes = N._pull_stream_msg_bytes(first)
    second_bytes = N._pull_stream_msg_bytes(second)

    @lock stream.message_lock begin
        put!(stream.messages, first)
        @lock state.lock begin
            state.buffered_messages = 1
            state.buffered_bytes = first_bytes
            state.requested_messages = 1
            state.requested_bytes = second_bytes
        end
    end

    done = Channel{Any}(1)
    task = Threads.@spawn begin
        try
            N._pull_stream_put!(stream, second)
            put!(done, :ok)
        catch err
            put!(done, err)
        end
    end

    try
        @test timedwait(0.1; pollint=0.001) do
            isready(done)
        end == :timed_out
        @lock state.lock begin
            @test state.requested_messages == 1
            @test state.requested_bytes == second_bytes
            @test state.buffered_messages == 1
            @test state.buffered_bytes == first_bytes
        end

        @lock stream.message_lock begin
            msg = take!(stream.messages)
            @test String(msg) == "first"
            @lock state.lock begin
                state.buffered_messages = max(0, state.buffered_messages - 1)
                state.buffered_bytes = max(0, state.buffered_bytes - N._pull_stream_msg_bytes(msg))
            end
            notify(stream.message_condition)
        end

        @test timedwait(1.0; pollint=0.001) do
            isready(done)
        end == :ok
        @test take!(done) == :ok
        @lock state.lock begin
            @test state.requested_messages == 0
            @test state.requested_bytes == 0
            @test state.buffered_messages == 1
            @test state.buffered_bytes == second_bytes
        end
        @lock stream.message_lock begin
            @test String(take!(stream.messages)) == "second"
        end
    finally
        close(stream)
        timedwait(1.0; pollint=0.001) do
            istaskdone(task)
        end
        close(psub)
    end
end

@testitem "JetStream continuous pull validates status pending headers" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    ok = Msg("_INBOX.pull.token", nothing, UInt8[];
             headers=Headers("Nats-Pending-Messages" => ["3"],
                             "Nats-Pending-Bytes" => ["42"]))
    @test N._pull_stream_status_pending(ok) == (3, 42)

    missing = Msg("_INBOX.pull.token", nothing, UInt8[])
    @test N._pull_stream_status_pending(missing) == (0, 0)

    state = N._PullMessageStreamState()
    config = N._PullStreamConfig(2, nothing, 1.0, 0.0, 2, nothing,
                                 nothing, nothing, nothing, nothing, nothing, 4)
    @test !isnothing(N._reserve_pull_stream_request!(config, state))
    delivered = Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.1.1.0.0", TestHelpers.bytes("one"))
    @lock state.lock N._pull_stream_decrement_requested!(state, delivered)
    @test state.requested_messages == 1
    @lock state.lock begin
        @test N._pull_stream_release_terminal_request!(state, missing)
        @test state.requested_messages == 0
        @test isempty(state.requests)
    end

    invalid_messages = Msg("_INBOX.pull.token", nothing, UInt8[];
                           headers=Headers("Nats-Pending-Messages" => ["abc"]))
    @test_throws ProtocolError N._pull_stream_status_pending(invalid_messages)

    invalid_bytes = Msg("_INBOX.pull.token", nothing, UInt8[];
                        headers=Headers("Nats-Pending-Bytes" => ["-1"]))
    @test_throws ProtocolError N._pull_stream_status_pending(invalid_bytes)
end

@testitem "JetStream continuous pull refills from buffered capacity" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream
    using JSON3

    const N = Natter

    function request_payloads(capture)
        [JSON3.read(m.match) for m in eachmatch(r"\{[^\r\n]+\}", TestHelpers.capture_text(capture))]
    end

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)
    TestHelpers.clear_capture!(capture)
    psub = N.PullSubscription(js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false,
                              PriorityPolicy.OVERFLOW, ["workers"])

    stream = messages(psub; batch=2, expires=1.0, heartbeat=0, threshold_messages=1,
                      channel_size=1, stop_after=2, min_pending=3,
                      min_ack_pending=4, priority_group="workers")
    try
        @test timedwait(1.0; pollint=0.001) do
            length(request_payloads(capture)) >= 1
        end != :timed_out
        first_payload = first(request_payloads(capture))
        @test first_payload["batch"] == 1
        @test first_payload["min_pending"] == 3
        @test first_payload["min_ack_pending"] == 4
        @test first_payload["group"] == "workers"

        N._dispatch_msg(client, Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.1.1.0.0", TestHelpers.bytes("one");
                                    sid=stream.delivery.sid))
        @test timedwait(1.0; pollint=0.001) do
            isready(stream.messages)
        end != :timed_out
        @test timedwait(0.1; pollint=0.001) do
            length(request_payloads(capture)) >= 2
        end == :timed_out

        @test String(take!(stream)) == "one"
        @test timedwait(1.0; pollint=0.001) do
            length(request_payloads(capture)) >= 2
        end != :timed_out
        @test last(request_payloads(capture))["batch"] == 1
    finally
        close(stream)
        close(psub)
    end
end

@testitem "JetStream consume callback drains continuous pull stream" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)
    TestHelpers.clear_capture!(capture)
    psub = N.PullSubscription(js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)
    received = Channel{String}(2)

    stream = consume(msg -> put!(received, String(msg)), psub;
                     batch=2, expires=1.0, heartbeat=0, channel_size=2, stop_after=2)
    try
        @test timedwait(1.0; pollint=0.001) do
            occursin("CONSUMER.MSG.NEXT", TestHelpers.capture_text(capture))
        end != :timed_out
        N._dispatch_msg(client, Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.1.1.0.0", TestHelpers.bytes("one");
                                    sid=stream.delivery.sid))
        N._dispatch_msg(client, Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.2.2.0.0", TestHelpers.bytes("two");
                                    sid=stream.delivery.sid))
        @test timedwait(1.0; pollint=0.001) do
            isready(received)
        end != :timed_out
        @test take!(received) == "one"
        @test timedwait(1.0; pollint=0.001) do
            isready(received)
        end != :timed_out
        @test take!(received) == "two"
        wait(stream)
    finally
        close(stream)
        close(psub)
    end
end

@testitem "JetStream pull fetch is reconnect-aware" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    reconnecting_client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    reconnecting_js = jetstream(reconnecting_client)
    reconnecting_psub = N.PullSubscription(reconnecting_js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)

    @test_throws FetchDisconnectedError fetch(reconnecting_psub, 1; timeout=0.1, heartbeat=0)
    @test reconnecting_client.pending_bytes == 0
    @test isempty(take!(reconnecting_client.pending))
    close(reconnecting_psub)

    function task_error(task)
        try
            fetch(task)
            nothing
        catch caught
            caught isa TaskFailedException ? first(Base.current_exceptions(task)).exception : caught
        end
    end

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)
    TestHelpers.clear_capture!(capture)
    psub = N.PullSubscription(js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)

    fetch_task = Threads.@spawn fetch(psub, 1; timeout=5.0, heartbeat=0)
    @test timedwait(1.0; pollint=0.001) do
        occursin("CONSUMER.MSG.NEXT", TestHelpers.capture_text(capture))
    end != :timed_out
    delivery = TestHelpers.active_pull_delivery(psub)
    @lock client.lock N._store_status_locked!(client, N.ConnectionStatus.RECONNECTING)
    N._notify_subscription_waiters!(delivery; all=true)
    @test task_error(fetch_task) isa FetchDisconnectedError
    close(psub)

    terminal_capture = TestHelpers.WriteCapture()
    terminal_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=terminal_capture)
    terminal_js = jetstream(terminal_client)
    TestHelpers.clear_capture!(terminal_capture)
    terminal_psub = N.PullSubscription(terminal_js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)

    terminal_task = Threads.@spawn fetch(terminal_psub, 1; timeout=5.0, heartbeat=0)
    @test timedwait(1.0; pollint=0.001) do
        occursin("CONSUMER.MSG.NEXT", TestHelpers.capture_text(terminal_capture))
    end != :timed_out
    terminal_delivery = TestHelpers.active_pull_delivery(terminal_psub)
    @lock terminal_client.lock begin
        N._store_status_locked!(terminal_client, N.ConnectionStatus.DISCONNECTED)
    end
    @lock terminal_delivery.lock begin
        terminal_delivery.closed = true
        N._notify_subscription_waiters_locked(terminal_delivery; all=true)
    end
    @test task_error(terminal_task) isa FetchDisconnectedError
    close(terminal_psub)

    partial_capture = TestHelpers.WriteCapture()
    partial_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=partial_capture)
    partial_js = jetstream(partial_client)
    TestHelpers.clear_capture!(partial_capture)
    partial_psub = N.PullSubscription(partial_js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)

    partial_task = Threads.@spawn fetch(partial_psub, 2; timeout=5.0, heartbeat=0)
    @test timedwait(1.0; pollint=0.001) do
        occursin("CONSUMER.MSG.NEXT", TestHelpers.capture_text(partial_capture))
    end != :timed_out
    partial_delivery = TestHelpers.active_pull_delivery(partial_psub)
    N._dispatch_msg(partial_client, Msg("orders.created", nothing, TestHelpers.bytes("payload");
                                        sid=partial_delivery.sid))
    @lock partial_client.lock N._store_status_locked!(partial_client, N.ConnectionStatus.RECONNECTING)
    N._notify_subscription_waiters!(partial_delivery; all=true)
    msgs = fetch(partial_task)
    @test length(msgs) == 1
    @test String(first(msgs)) == "payload"
    close(partial_psub)
end

@testitem "JetStream pull fetch isolates concurrent request deliveries" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    function fetch_replies(capture)
        replies = String[]
        for line in split(TestHelpers.capture_text(capture), "\r\n")
            parts = split(line)
            length(parts) >= 4 || continue
            parts[1] == "PUB" || continue
            occursin("CONSUMER.MSG.NEXT", parts[2]) || continue
            push!(replies, String(parts[3]))
        end
        replies
    end

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)
    TestHelpers.clear_capture!(capture)
    psub = N.PullSubscription(js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)

    try
        first_task = Threads.@spawn fetch(psub, 1; timeout=2.0, heartbeat=0)
        first_delivery = TestHelpers.wait_for_pull_delivery(psub)
        second_task = Threads.@spawn fetch(psub, 1; timeout=2.0, heartbeat=0)
        @test timedwait(1.0; pollint=0.001) do
            length(TestHelpers.active_pull_deliveries(psub)) == 2
        end != :timed_out
        second_delivery = only(filter(delivery -> delivery !== first_delivery,
                                      TestHelpers.active_pull_deliveries(psub)))

        N._dispatch_msg(client, Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.2.2.0.0",
                                    TestHelpers.bytes("second"); sid=second_delivery.sid))
        second_msgs = fetch(second_task)
        @test length(second_msgs) == 1
        @test String(first(second_msgs)) == "second"
        @test !istaskdone(first_task)

        N._dispatch_msg(client, Msg(first_delivery.subject, nothing, UInt8[];
                                    headers=Headers("Status" => ["404"], "Description" => ["No Messages"]),
                                    sid=first_delivery.sid))
        @test isempty(fetch(first_task))

        replies = fetch_replies(capture)
        @test length(unique(replies)) >= 2
        @test all(!endswith(reply, ".*") for reply in replies)
    finally
        close(psub)
    end
end

@testitem "JetStream pull fetch maps status controls" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    function run_fetch(headers::Headers; data=UInt8[])
        client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
        js = jetstream(client)
        psub = N.PullSubscription(js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)
        try
            task = Threads.@spawn fetch(psub, 1; timeout=1.0, heartbeat=0)
            delivery = TestHelpers.wait_for_pull_delivery(psub)
            subject = isempty(data) ? delivery.subject : "orders.created"
            N._dispatch_msg(client, Msg(subject, nothing, data; headers, sid=delivery.sid))
            TestHelpers.fetch_task_result(task)
        finally
            close(psub)
        end
    end

    @test isempty(run_fetch(Headers("Status" => ["404"], "Description" => ["No Messages"])))
    @test isempty(run_fetch(Headers("Status" => ["408"], "Description" => ["Request Timeout"])))
    @test isempty(run_fetch(Headers("Status" => ["409"], "Description" => ["Batch Completed"])))
    @test isempty(run_fetch(Headers("Status" => ["409"], "Description" => ["Message Size Exceeds MaxBytes"])))

    @test_throws JetStreamError run_fetch(Headers("Status" => ["400"], "Description" => ["Bad Request"]))
    @test_throws JetStreamError run_fetch(Headers("Status" => ["409"], "Description" => ["Consumer Deleted"]))
    @test_throws JetStreamError run_fetch(Headers("Status" => ["409"], "Description" => ["Leadership Change"]))
    @test_throws JetStreamError run_fetch(Headers("Status" => ["409"], "Description" => ["Server Shutdown"]))
    @test_throws JetStreamError run_fetch(Headers("Status" => ["423"], "Description" => ["Pin ID Mismatch"]))
    @test_throws NoRespondersError run_fetch(Headers("Status" => ["503"], "Description" => ["No Responders"]))

    msgs = run_fetch(Headers("Status" => ["409"], "Description" => ["Consumer Deleted"]); data=TestHelpers.bytes("payload"))
    @test length(msgs) == 1
    @test String(first(msgs)) == "payload"
end

@testitem "JetStream pull fetch uses and monitors idle heartbeats" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    js = jetstream(client)
    psub = N.PullSubscription(js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)
    task = Threads.@spawn fetch(psub, 1; timeout=1.0, heartbeat=0.02)
    delivery = TestHelpers.wait_for_pull_delivery(psub)
    N._dispatch_msg(client, Msg(delivery.subject, nothing, UInt8[];
                                headers=Headers("Status" => ["404"], "Description" => ["No Messages"]),
                                sid=delivery.sid))
    @test isempty(fetch(task))
    @test occursin("\"idle_heartbeat\":20000000", String(take!(client.write_io)))
    close(psub)

    timeout_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    timeout_js = jetstream(timeout_client)
    timeout_psub = N.PullSubscription(timeout_js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)
    try
        @test_throws JetStreamError fetch(timeout_psub, 1; timeout=0.12, heartbeat=0.02)
    finally
        close(timeout_psub)
    end
end

@testitem "JetStream pull fetch tracks pinned consumer ids" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    js = jetstream(client)
    psub = N.PullSubscription(js, "ORDERS", "WORKER", ReentrantLock(), ReentrantLock(), false, false)

    try
        task = Threads.@spawn fetch(psub, 1; timeout=1.0, heartbeat=0)
        delivery = TestHelpers.wait_for_pull_delivery(psub)
        N._dispatch_msg(client, Msg("orders.created", nothing, TestHelpers.bytes("payload");
                                    headers=Headers("Nats-Pin-Id" => ["pin-a"]),
                                    sid=delivery.sid))
        @test String(first(fetch(task))) == "payload"
        @test psub.pin_id == "pin-a"
        take!(client.write_io)

        task = Threads.@spawn fetch(psub, 1; timeout=1.0, heartbeat=0)
        delivery = TestHelpers.wait_for_pull_delivery(psub)
        N._dispatch_msg(client, Msg(delivery.subject, nothing, UInt8[];
                                    headers=Headers("Status" => ["404"], "Description" => ["No Messages"]),
                                    sid=delivery.sid))
        @test isempty(fetch(task))
        request = String(take!(client.write_io))
        @test occursin("\"id\":\"pin-a\"", request)
        @test !occursin("pin_id", request)

        task = Threads.@spawn fetch(psub, 1; timeout=1.0, heartbeat=0)
        delivery = TestHelpers.wait_for_pull_delivery(psub)
        N._dispatch_msg(client, Msg(delivery.subject, nothing, UInt8[];
                                    headers=Headers("Status" => ["423"], "Description" => ["Pin ID Mismatch"]),
                                    sid=delivery.sid))
        @test_throws JetStreamError TestHelpers.fetch_task_result(task)
        @test isnothing(psub.pin_id)
    finally
        close(psub)
    end
end

@testitem "JetStream subscription close is idempotent" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    js = jetstream(client)

    pull = N.PullSubscription(js, "S", "C", ReentrantLock(), ReentrantLock(), false, false)
    close(pull; timeout=0.1)
    close(pull; timeout=0.1)
    @test pull.closed

    push_core = subscribe(client, "_INBOX.push")
    push = N.PushSubscription(js, push_core, "S", "C", ReentrantLock(), false, false)
    close(push; timeout=0.1)
    close(push; timeout=0.1)
    @test push.closed
    @test push_core.closed
end

@testitem "JetStream ephemeral close retries server delete after transient failure" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream
    using JSON3

    const N = Natter

    function respond_next_request!(client, payload::AbstractString)
        result = timedwait(1.0; pollint=0.001) do
            @lock client.lock begin
                mux = client.request_mux
                !isnothing(mux) && !isempty(mux.waiters)
            end
        end
        result == :timed_out && return false
        subject, sid = @lock client.lock begin
            mux = client.request_mux
            token = first(keys(mux.waiters))
            "$(mux.prefix).$token", mux.sub.sid
        end
        N._dispatch_msg(client, Msg(subject, nothing, TestHelpers.bytes(payload); sid))
        true
    end

    function retry_delete_after_reconnect!(client, closer)
        err = TestHelpers.thrown_exception(closer)
        @test err isa CleanupError
        @test err.cause isa ConnectionReconnectingError

        @lock client.lock N._store_status_locked!(client, N.ConnectionStatus.CONNECTED)
        task = Threads.@spawn closer()
        @test respond_next_request!(client, JSON3.write(Dict("success" => true)))
        fetch(task)

        written = String(take!(client.write_io))
        @test occursin("\$JS.API.CONSUMER.DELETE.S.C", written)
    end

    opts = N.ConnectOptions(pending_size=0)
    pull_client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.RECONNECTING,
                                          write_io=IOBuffer())
    pull_js = jetstream(pull_client)
    pull = N.PullSubscription(pull_js, "S", "C", ReentrantLock(), ReentrantLock(), true, false)

    retry_delete_after_reconnect!(pull_client, () -> close(pull))
    @test pull.closed
    @test pull.server_deleted
    @test close(pull) === nothing
    @test String(take!(pull_client.write_io)) == ""

    push_client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.RECONNECTING,
                                          write_io=IOBuffer())
    push_js = jetstream(push_client)
    handler = N._JetStreamPushControlHandler()
    push_core = subscribe(push_client, "_INBOX.push"; _control_handler=handler)
    push = N.PushSubscription(push_js, push_core, "S", "C", ReentrantLock(), true,
                              false, nothing, handler)

    retry_delete_after_reconnect!(push_client, () -> close(push))
    @test push.closed
    @test push_core.closed
    @test push.server_deleted
    @test handler.consumer_deleted[]
    @test close(push) === nothing
    @test String(take!(push_client.write_io)) == ""
end

@testitem "JetStream hot handles carry concrete client and subscription types" setup=[TestHelpers] begin
    using Natter
    using Natter.JetStream

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    js = jetstream(client)
    sub = subscribe(client, "_INBOX.typed")
    msg = JetStreamMsg(Msg("_INBOX.typed", "\$JS.ACK.S.C.1.1.1.0.0", UInt8[]; sid=sub.sid), client)
    pull = N.PullSubscription(js, "S", "C", ReentrantLock(), ReentrantLock(), false, false)
    push = N.PushSubscription(js, sub, "S", "C", ReentrantLock(), false, false)

    @test fieldtype(typeof(msg), :_client) === typeof(client)
    @test fieldtype(typeof(js), :client) === typeof(client)
    @test fieldtype(typeof(pull), :active_deliveries) === Vector{typeof(sub)}
    @test fieldtype(typeof(push), :sub) === typeof(sub)
    @test only(Base.return_types(ack, Tuple{typeof(msg)})) === Nothing
    @test only(Base.return_types(ack_sync, Tuple{typeof(msg)})) === Msg
end
