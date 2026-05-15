using TestItems

@testitem "JetStream typed stream config serialization" begin
    using Dates
    using Natter

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
        priority_groups=["fast", "slow"],
        priority_policy=PriorityPolicy.PINNED_CLIENT,
        priority_timeout=4.0,
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
    @test payload["priority_policy"] == "pinned_client"
    @test payload["priority_timeout"] == 4_000_000_000
    @test startswith(payload["pause_until"], "2026-01-03T00:00:00")
end

@testitem "JetStream consumer filter config validation" begin
    using Natter

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
    @test_throws ArgumentError N._js_config_payload(ConsumerConfig(priority_groups=["bad group"]))
end

@testitem "JetStream raw dict config serialization matches typed config units" begin
    using Dates
    using Natter

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

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
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
        @test_throws ArgumentError stream_names(js; timeout=invalid_timeout)
        @test_throws ArgumentError consumer_list(js, "ORDERS"; timeout=invalid_timeout)
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
    @test_throws ArgumentError consumer_list(js, "ORDERS"; offset=-1)
    @test TestHelpers.capture_text(capture) == ""
    @test isempty(client.subscriptions)
end

@testitem "JetStream publish options serialize supported headers" setup=[TestHelpers] begin
    using Dates
    using Natter

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

    hdrs = N._js_publish_headers(nothing; schedule_at=DateTime(2026, 1, 2, 3, 4, 5))
    @test hdrs["Nats-Schedule"] == ["@at 2026-01-02T03:04:05.000Z"]

    TestHelpers.clear_capture!(capture)
    @test_throws ArgumentError js_publish(js, "orders.created", "payload"; stream="A", expected_stream="B")
    @test_throws ArgumentError js_publish(js, "orders.created", "payload"; expected_last_subject="orders.created")
    @test_throws ArgumentError js_publish(js, "orders.created", "payload"; ttl=0)
    @test_throws ArgumentError js_publish(js, "orders.created", "payload"; ttl=0.5)
    @test_throws ArgumentError js_publish(js, "orders.created", "payload"; schedule="x", schedule_every=1.0)
    @test_throws ArgumentError js_publish(js, "orders.created", "payload"; retry_attempts=-1)
    @test TestHelpers.capture_text(capture) == ""
end

@testitem "JetStream typed config response parsing" begin
    using Natter

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
        "mirror" => Dict("name" => "UPSTREAM", "external" => Dict("api" => "\$JS.domain.API")),
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
    @test stream.consumer_limits.inactive_threshold == 4.0

    consumer = N._consumer_config_from_payload(Dict{String,Any}(
        "name" => "worker",
        "deliver_policy" => "last_per_subject",
        "ack_policy" => "none",
        "ack_wait" => 5_000_000_000,
        "backoff" => [1_000_000_000, 2_000_000_000],
        "replay_policy" => "instant",
        "idle_heartbeat" => 250_000_000,
        "priority_policy" => "overflow",
        "priority_timeout" => 6_000_000_000,
        "priority_groups" => ["fast"],
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

    const N = Natter

    @test_throws MethodError StreamConfig(name="S", metadata=Dict{String,Any}("ok" => 1))
    @test_throws ArgumentError N._js_field_value(:metadata, Dict{String,Any}("ok" => 1))
end

@testitem "JetStream consumer filter config validation happens before request" setup=[TestHelpers] begin
    using Natter

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

@testitem "JetStream stream discovery validates subjects before request" setup=[TestHelpers] begin
    using Natter

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

@testitem "JetStream stream purge validates filter locally" setup=[TestHelpers] begin
    using Natter

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

    const N = Natter

    mutable struct JetStreamCallable
        subjects::Vector{String}
    end
    function (cb::JetStreamCallable)(msg::JetStreamMsg)
        push!(cb.subjects, msg.subject)
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

@testitem "JetStream terminal acks are not marked done while reconnecting" setup=[TestHelpers] begin
    using Natter

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

    first = @async ack_result()
    take!(capture.started)
    second = @async ack_result()
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

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
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

@testitem "JetStream push control dispatch filters heartbeats" setup=[TestHelpers] begin
    using Natter

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
    @test N._status_header(next(plain_sub; timeout=0.1)) == 100

    close(push_sub)
    close(plain_sub)
end

@testitem "JetStream push control dispatch maps lifecycle statuses" setup=[TestHelpers] begin
    using Natter

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

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    sub = subscribe(client, "_INBOX.push"; _control_handler=N._JetStreamPushControlHandler())
    flow_control = Msg("_INBOX.push", "_INBOX.fc", UInt8[];
                       headers=Headers("Status" => ["100"], "Description" => ["FlowControl Request"]),
                       sid=sub.sid)

    N._dispatch_msg(client, flow_control)

    expected = "PUB _INBOX.fc 0\r\n\r\n"
    @test !isready(sub.messages)
    @test client.pending_bytes == ncodeunits(expected)
    @test String(take!(client.pending)) == expected
    close(sub)
end

@testitem "JetStream push flow control waits for channel delivery" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    sub = subscribe(client, "_INBOX.push"; _control_handler=N._JetStreamPushControlHandler())
    data = Msg("_INBOX.push", "\$JS.ACK.S.C.1.1.1.0.0", TestHelpers.bytes("work");
               sid=sub.sid)
    flow_control = Msg("_INBOX.push", "_INBOX.fc", UInt8[];
                       headers=Headers("Status" => ["100"], "Description" => ["FlowControl Request"]),
                       sid=sub.sid)

    N._dispatch_msg(client, data)
    N._dispatch_msg(client, flow_control)

    @test isready(sub.messages)
    @test client.pending_bytes == 0
    @test String(next(sub; timeout=0.1)) == "work"

    expected = "PUB _INBOX.fc 0\r\n\r\n"
    @test client.pending_bytes == ncodeunits(expected)
    @test String(take!(client.pending)) == expected
    close(sub)
end

@testitem "JetStream push next returns ackable messages" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    js = jetstream(client)
    sub = subscribe(client, "_INBOX.push"; _control_handler=N._JetStreamPushControlHandler())
    psub = N.PushSubscription(js, sub, "S", "C", ReentrantLock(), false, false)

    try
        N._dispatch_msg(client, Msg("_INBOX.push", "\$JS.ACK.S.C.1.1.1.0.0", TestHelpers.bytes("work");
                                   sid=sub.sid))
        msg = next(psub; timeout=0.1)
        @test msg isa JetStreamMsg
        @test fieldtype(typeof(msg), :_client) === typeof(client)
        @test String(msg) == "work"
    finally
        close(psub)
    end
end

@testitem "JetStream push next rejects callback subscriptions" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    js = jetstream(client)
    sub = subscribe(client, "_INBOX.push"; callback=_ -> nothing,
                    _control_handler=N._JetStreamPushControlHandler())
    psub = N.PushSubscription(js, sub, "S", "C", ReentrantLock(), false, false)

    try
        err = TestHelpers.thrown_exception(() -> next(psub; timeout=0.1))
        @test err isa ArgumentError
        @test occursin("callback", err.msg)
        async_err = TestHelpers.thrown_exception(() -> fetch(next_async(psub; timeout=0.1)))
        @test async_err isa ArgumentError
    finally
        close(psub)
    end
end

@testitem "JetStream push flow control waits for callback delivery" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    started = Channel{String}(2)
    release = Channel{Bool}(1)
    sub = subscribe(client, "_INBOX.callback";
                    callback=msg -> begin
                        put!(started, String(msg))
                        String(msg) == "busy" && take!(release)
                    end,
                    _control_handler=N._JetStreamPushControlHandler())

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

        put!(release, true)
        expected = "PUB _INBOX.fc 0\r\n\r\n"
        @test timedwait(1.0; pollint=0.01) do
            client.pending_bytes == ncodeunits(expected)
        end != :timed_out
        @test String(take!(client.pending)) == expected

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

@testitem "JetStream push control dispatch does not invoke callbacks" setup=[TestHelpers] begin
    using Natter

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

    const N = Natter

    client = TestHelpers.fake_client()
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

    old_client = TestHelpers.fake_client()
    old_client.info.version = "2.9.22"
    @test_throws UnsupportedFeatureError N._consumer_request_payload(jetstream(old_client), "ORDERS", payload, "create")
    @test_throws UnsupportedFeatureError N._consumer_request_payload(jetstream(old_client), "ORDERS", payload, "update")
end

@testitem "JetStream durable bind-or-create binds after create conflict" setup=[TestHelpers] begin
    using Natter
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

    pull_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    pull_js = jetstream(pull_client)
    pull_payload = N._js_config_payload(ConsumerConfig(name="worker", durable_name="worker", filter_subject="orders.created"))
    pull_task = @async N._bind_or_create_consumer(pull_js, "ORDERS", "worker", pull_payload, Set(["name", "durable_name", "filter_subject"]))
    respond_next_request!(pull_client, missing)
    respond_next_request!(pull_client, conflict)
    respond_next_request!(pull_client, consumer_response())

    pull_info, pull_created = fetch(pull_task)
    @test !pull_created
    @test pull_info.name == "worker"

    push_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    push_js = jetstream(push_client)
    push_task = @async push_subscribe(push_js, "orders.created"; stream="ORDERS", durable="worker")
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

    client = TestHelpers.fake_client()
    js = jetstream(client)
    api_msg, api_seq, api_created = N._stream_message_from_api_payload(js, Dict{String,Any}(
        "subject" => "orders.created",
        "seq" => 7,
        "data" => "cGF5bG9hZA==",
        "time" => "2026-01-01T00:00:00.123456789Z",
    ))
    @test api_seq == 7
    @test api_created == DateTime(2026, 1, 1, 0, 0, 0, 123)
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
    msg = N._direct_message_response(js, "\$JS.API.DIRECT.GET.ORDERS", response)
    @test msg.subject == "orders.created"
    @test String(msg) == "payload"
    @test header(msg, "X-Test") == "ok"
    @test isnothing(header(msg, "Nats-Sequence"))
    info_msg, info_seq, info_created = N._direct_message_response_info(js, "\$JS.API.DIRECT.GET.ORDERS", response)
    @test info_msg.subject == "orders.created"
    @test info_seq == 7
    @test info_created == DateTime(2026, 1, 1)

    not_found = Msg("_INBOX.reply", nothing, UInt8[]; headers=Headers("Status" => ["404"], "Description" => ["no message found"]))
    @test_throws JetStreamError N._direct_message_response(js, "\$JS.API.DIRECT.GET.ORDERS", not_found)
    no_responders = Msg("_INBOX.reply", nothing, UInt8[]; headers=Headers("Status" => ["503"]))
    @test_throws NoRespondersError N._direct_message_response(js, "\$JS.API.DIRECT.GET.MISSING", no_responders)
end

@testitem "JetStream metadata" begin
    using Natter

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

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    js = jetstream(client)
    core_sub = subscribe(client, "_INBOX.pull")
    psub = N.PullSubscription(js, core_sub, "ORDERS", "WORKER", "_INBOX.pull", ReentrantLock(), ReentrantLock(), false, false)

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
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, priority=10, priority_group="workers")
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, min_pending=1)
    @test client.pending_bytes == 0

    close(psub)
    @test_throws ConnectionClosedError fetch(psub, 1; timeout=1.0)
    @test client.pending_bytes == 0

    underlying = subscribe(client, "_INBOX.underlying")
    underlying_psub = N.PullSubscription(js, underlying, "ORDERS", "WORKER", "_INBOX.underlying", ReentrantLock(), ReentrantLock(), false, false)
    close(underlying)
    @test_throws ConnectionClosedError fetch(underlying_psub, 1; timeout=1.0)
    @test client.pending_bytes == 0
end

@testitem "JetStream pull fetch serializes max bytes and no wait" setup=[TestHelpers] begin
    using Natter
    using JSON3

    const N = Natter

    function pull_request_payload(frame::AbstractString)
        header, rest = split(frame, "\r\n"; limit=2)
        parts = split(header)
        @test parts[1] == "PUB"
        len = parse(Int, parts[end])
        payload, trailer = split(rest, "\r\n"; limit=2)
        @test ncodeunits(payload) == len
        @test trailer == ""
        JSON3.read(payload)
    end

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    js = jetstream(client)
    core_sub = subscribe(client, "_INBOX.pull")
    take!(client.write_io)
    psub = N.PullSubscription(js, core_sub, "ORDERS", "WORKER", "_INBOX.pull", ReentrantLock(), ReentrantLock(), false, false)

    try
        N._dispatch_msg(client, Msg("_INBOX.pull", nothing, UInt8[];
                                    headers=Headers("Status" => ["404"], "Description" => ["No Messages"]),
                                    sid=core_sub.sid))
        @test isempty(fetch(psub, big(10); timeout=10.0, heartbeat=0, max_bytes=256, no_wait=true,
                            min_pending=4, min_ack_pending=5, priority_group="workers",
                            priority=2))
        payload = pull_request_payload(String(take!(client.write_io)))
        @test payload["batch"] == 10
        @test payload["max_bytes"] == 256
        @test payload["no_wait"] == true
        @test payload["expires"] == 9_000_000_000
        @test payload["min_pending"] == 4
        @test payload["min_ack_pending"] == 5
        @test payload["group"] == "workers"
        @test payload["priority"] == 2
    finally
        close(psub)
    end
end

@testitem "JetStream pull fetch expires server request before local timeout" setup=[TestHelpers] begin
    using Natter
    using JSON3

    const N = Natter

    function pull_request_payload(frame::AbstractString)
        header, rest = split(frame, "\r\n"; limit=2)
        parts = split(header)
        @test parts[1] == "PUB"
        len = parse(Int, parts[end])
        payload, trailer = split(rest, "\r\n"; limit=2)
        @test ncodeunits(payload) == len
        @test trailer == ""
        JSON3.read(payload)
    end

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    js = jetstream(client)
    core_sub = subscribe(client, "_INBOX.pull")
    take!(client.write_io)
    psub = N.PullSubscription(js, core_sub, "ORDERS", "WORKER", "_INBOX.pull", ReentrantLock(), ReentrantLock(), false, false)

    try
        N._dispatch_msg(client, Msg("_INBOX.pull", nothing, UInt8[];
                                    headers=Headers("Status" => ["404"], "Description" => ["No Messages"]),
                                    sid=core_sub.sid))
        @test isempty(fetch(psub, 1; timeout=10.0, heartbeat=0))
        default_payload = pull_request_payload(String(take!(client.write_io)))
        @test default_payload["batch"] == 1
        @test default_payload["expires"] == 9_000_000_000
        @test !haskey(default_payload, "idle_heartbeat")

        N._dispatch_msg(client, Msg("_INBOX.pull", nothing, UInt8[];
                                    headers=Headers("Status" => ["404"], "Description" => ["No Messages"]),
                                    sid=core_sub.sid))
        @test isempty(fetch(psub, 1; timeout=10.0, expires=2.5, heartbeat=0))
        explicit_payload = pull_request_payload(String(take!(client.write_io)))
        @test explicit_payload["expires"] == 2_500_000_000
    finally
        close(psub)
    end
end

@testitem "JetStream continuous pull validates inputs and excludes fetch" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)
    core_sub = subscribe(client, "_INBOX.pull.*")
    TestHelpers.clear_capture!(capture)
    psub = N.PullSubscription(js, core_sub, "ORDERS", "WORKER", "_INBOX.pull.*", ReentrantLock(), ReentrantLock(), false, false)

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
    @test_throws ArgumentError messages(psub; batch=1, priority=-1, priority_group="workers")

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

@testitem "JetStream continuous pull publishes outside stream state lock" setup=[TestHelpers] begin
    using Natter

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
        if t.enabled && !t.blocked
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
    core_sub = subscribe(client, "_INBOX.pull.*")
    TestHelpers.clear_capture!(transport.capture)

    transport.enabled = true
    psub = N.PullSubscription(js, core_sub, "ORDERS", "WORKER", "_INBOX.pull.*",
                              ReentrantLock(), ReentrantLock(), false, false)

    stream = messages(psub; batch=1, expires=1.0, heartbeat=0, stop_after=1)
    stream_done = Ref(:timed_out)
    try
        @test timedwait(1.0; pollint=0.001) do
            isready(transport.started)
        end != :timed_out

        close_task = @async close(stream)
        @test timedwait(0.2; pollint=0.001) do
            istaskdone(close_task)
        end != :timed_out
        @test fetch(close_task) === nothing
    finally
        release_transport!(transport)
        close(stream)
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

    const N = Natter

    function request_count(capture)
        length(collect(eachmatch(r"CONSUMER.MSG.NEXT", TestHelpers.capture_text(capture))))
    end

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)
    core_sub = subscribe(client, "_INBOX.pull.*")
    TestHelpers.clear_capture!(capture)
    psub = N.PullSubscription(js, core_sub, "ORDERS", "WORKER", "_INBOX.pull.*", ReentrantLock(), ReentrantLock(), false, false)

    stream = messages(psub; batch=2, expires=1.0, heartbeat=0, threshold_messages=1,
                      channel_size=4, stop_after=3)
    try
        @test timedwait(1.0; pollint=0.001) do
            request_count(capture) >= 1
        end != :timed_out
        N._dispatch_msg(client, Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.1.1.0.0", TestHelpers.bytes("one");
                                    sid=core_sub.sid))
        @test String(take!(stream)) == "one"
        @test timedwait(1.0; pollint=0.001) do
            request_count(capture) >= 2
        end != :timed_out

        N._dispatch_msg(client, Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.2.2.0.0", TestHelpers.bytes("two");
                                    sid=core_sub.sid))
        N._dispatch_msg(client, Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.3.3.0.0", TestHelpers.bytes("three");
                                    sid=core_sub.sid))
        @test String(take!(stream)) == "two"
        @test String(take!(stream)) == "three"
        wait(stream)
        @test !psub.active_stream
    finally
        close(stream)
        close(psub)
    end
end

@testitem "JetStream continuous pull refills from buffered capacity" setup=[TestHelpers] begin
    using Natter
    using JSON3

    const N = Natter

    function request_payloads(capture)
        [JSON3.read(m.match) for m in eachmatch(r"\{[^\r\n]+\}", TestHelpers.capture_text(capture))]
    end

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)
    core_sub = subscribe(client, "_INBOX.pull.*")
    TestHelpers.clear_capture!(capture)
    psub = N.PullSubscription(js, core_sub, "ORDERS", "WORKER", "_INBOX.pull.*", ReentrantLock(), ReentrantLock(), false, false)

    stream = messages(psub; batch=2, expires=1.0, heartbeat=0, threshold_messages=1,
                      channel_size=1, stop_after=2, min_pending=3,
                      min_ack_pending=4, priority_group="workers", priority=1)
    try
        @test timedwait(1.0; pollint=0.001) do
            length(request_payloads(capture)) >= 1
        end != :timed_out
        first_payload = first(request_payloads(capture))
        @test first_payload["batch"] == 1
        @test first_payload["min_pending"] == 3
        @test first_payload["min_ack_pending"] == 4
        @test first_payload["group"] == "workers"
        @test first_payload["priority"] == 1

        N._dispatch_msg(client, Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.1.1.0.0", TestHelpers.bytes("one");
                                    sid=core_sub.sid))
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

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)
    core_sub = subscribe(client, "_INBOX.pull.*")
    TestHelpers.clear_capture!(capture)
    psub = N.PullSubscription(js, core_sub, "ORDERS", "WORKER", "_INBOX.pull.*", ReentrantLock(), ReentrantLock(), false, false)
    received = Channel{String}(2)

    stream = consume(msg -> put!(received, String(msg)), psub;
                     batch=2, expires=1.0, heartbeat=0, channel_size=2, stop_after=2)
    try
        @test timedwait(1.0; pollint=0.001) do
            occursin("CONSUMER.MSG.NEXT", TestHelpers.capture_text(capture))
        end != :timed_out
        N._dispatch_msg(client, Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.1.1.0.0", TestHelpers.bytes("one");
                                    sid=core_sub.sid))
        N._dispatch_msg(client, Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.2.2.0.0", TestHelpers.bytes("two");
                                    sid=core_sub.sid))
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

    const N = Natter

    reconnecting_client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    reconnecting_js = jetstream(reconnecting_client)
    reconnecting_sub = subscribe(reconnecting_client, "_INBOX.reconnecting")
    reconnecting_psub = N.PullSubscription(reconnecting_js, reconnecting_sub, "ORDERS", "WORKER", "_INBOX.reconnecting", ReentrantLock(), ReentrantLock(), false, false)

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
    core_sub = subscribe(client, "_INBOX.pull")
    TestHelpers.clear_capture!(capture)
    psub = N.PullSubscription(js, core_sub, "ORDERS", "WORKER", "_INBOX.pull", ReentrantLock(), ReentrantLock(), false, false)

    fetch_task = @async fetch(psub, 1; timeout=5.0, heartbeat=0)
    @test timedwait(1.0; pollint=0.001) do
        occursin("CONSUMER.MSG.NEXT", TestHelpers.capture_text(capture))
    end != :timed_out
    @lock client.lock N._store_status_locked!(client, N.ConnectionStatus.RECONNECTING)
    N._notify_subscription_waiters!(core_sub; all=true)
    @test task_error(fetch_task) isa FetchDisconnectedError
    close(psub)

    terminal_capture = TestHelpers.WriteCapture()
    terminal_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=terminal_capture)
    terminal_js = jetstream(terminal_client)
    terminal_core_sub = subscribe(terminal_client, "_INBOX.terminal")
    TestHelpers.clear_capture!(terminal_capture)
    terminal_psub = N.PullSubscription(terminal_js, terminal_core_sub, "ORDERS", "WORKER", "_INBOX.terminal", ReentrantLock(), ReentrantLock(), false, false)

    terminal_task = @async fetch(terminal_psub, 1; timeout=5.0, heartbeat=0)
    @test timedwait(1.0; pollint=0.001) do
        occursin("CONSUMER.MSG.NEXT", TestHelpers.capture_text(terminal_capture))
    end != :timed_out
    @lock terminal_client.lock begin
        N._store_status_locked!(terminal_client, N.ConnectionStatus.DISCONNECTED)
    end
    @lock terminal_core_sub.lock begin
        terminal_core_sub.closed = true
        N._notify_subscription_waiters_locked(terminal_core_sub; all=true)
    end
    @test task_error(terminal_task) isa FetchDisconnectedError
    close(terminal_psub)

    partial_capture = TestHelpers.WriteCapture()
    partial_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=partial_capture)
    partial_js = jetstream(partial_client)
    partial_core_sub = subscribe(partial_client, "_INBOX.partial")
    TestHelpers.clear_capture!(partial_capture)
    partial_psub = N.PullSubscription(partial_js, partial_core_sub, "ORDERS", "WORKER", "_INBOX.partial", ReentrantLock(), ReentrantLock(), false, false)
    N._dispatch_msg(partial_client, Msg("_INBOX.partial", nothing, TestHelpers.bytes("payload"); sid=partial_core_sub.sid))

    partial_task = @async fetch(partial_psub, 2; timeout=5.0, heartbeat=0)
    @test timedwait(1.0; pollint=0.001) do
        occursin("CONSUMER.MSG.NEXT", TestHelpers.capture_text(partial_capture))
    end != :timed_out
    @lock partial_client.lock N._store_status_locked!(partial_client, N.ConnectionStatus.RECONNECTING)
    N._notify_subscription_waiters!(partial_core_sub; all=true)
    msgs = fetch(partial_task)
    @test length(msgs) == 1
    @test String(first(msgs)) == "payload"
    close(partial_psub)
end

@testitem "JetStream pull fetch correlates statuses to the active request" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    function fetch_reply(capture)
        line = first(split(TestHelpers.capture_text(capture), "\r\n"))
        parts = split(line)
        @test parts[1] == "PUB"
        String(parts[3])
    end

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    js = jetstream(client)
    core_sub = subscribe(client, "_INBOX.pull.*")
    TestHelpers.clear_capture!(capture)
    psub = N.PullSubscription(js, core_sub, "ORDERS", "WORKER", "_INBOX.pull.*", ReentrantLock(), ReentrantLock(), false, false)

    try
        N._dispatch_msg(client, Msg("_INBOX.pull.old404", nothing, UInt8[];
                                    headers=Headers("Status" => ["404"], "Description" => ["No Messages"]),
                                    sid=core_sub.sid))
        N._dispatch_msg(client, Msg("_INBOX.pull.old408", nothing, UInt8[];
                                    headers=Headers("Status" => ["408"], "Description" => ["Request Timeout"]),
                                    sid=core_sub.sid))
        N._dispatch_msg(client, Msg("_INBOX.pull.old409", nothing, UInt8[];
                                    headers=Headers("Status" => ["409"], "Description" => ["Batch Completed"]),
                                    sid=core_sub.sid))
        N._dispatch_msg(client, Msg("orders.created", "\$JS.ACK.ORDERS.WORKER.1.1.1.0.0", TestHelpers.bytes("payload");
                                    sid=core_sub.sid))

        msgs = fetch(psub, 1; timeout=0.2, heartbeat=0)
        @test length(msgs) == 1
        @test String(first(msgs)) == "payload"

        reply = fetch_reply(capture)
        @test startswith(reply, "_INBOX.pull.")
        @test reply != psub.deliver
        @test !endswith(reply, ".*")
    finally
        close(psub)
    end

    active_capture = TestHelpers.WriteCapture()
    active_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=active_capture)
    active_js = jetstream(active_client)
    active_core_sub = subscribe(active_client, "_INBOX.active.*")
    TestHelpers.clear_capture!(active_capture)
    active_psub = N.PullSubscription(active_js, active_core_sub, "ORDERS", "WORKER", "_INBOX.active.*", ReentrantLock(), ReentrantLock(), false, false)

    try
        fetch_task = @async fetch(active_psub, 1; timeout=1.0, heartbeat=0)
        @test timedwait(1.0; pollint=0.001) do
            occursin("CONSUMER.MSG.NEXT", TestHelpers.capture_text(active_capture))
        end != :timed_out
        reply = fetch_reply(active_capture)
        N._dispatch_msg(active_client, Msg(reply, nothing, UInt8[];
                                          headers=Headers("Status" => ["404"], "Description" => ["No Messages"]),
                                          sid=active_core_sub.sid))
        @test isempty(fetch(fetch_task))
    finally
        close(active_psub)
    end
end

@testitem "JetStream pull fetch maps status controls" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    function run_fetch(headers::Headers; data=UInt8[])
        client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
        js = jetstream(client)
        core_sub = subscribe(client, "_INBOX.pull")
        take!(client.write_io)
        psub = N.PullSubscription(js, core_sub, "ORDERS", "WORKER", "_INBOX.pull", ReentrantLock(), ReentrantLock(), false, false)
        N._dispatch_msg(client, Msg("_INBOX.pull", nothing, data; headers, sid=core_sub.sid))
        try
            fetch(psub, 1; timeout=0.1, heartbeat=0)
        finally
            close(psub)
        end
    end

    @test isempty(run_fetch(Headers("Status" => ["404"], "Description" => ["No Messages"])))
    @test isempty(run_fetch(Headers("Status" => ["408"], "Description" => ["Request Timeout"])))
    @test isempty(run_fetch(Headers("Status" => ["409"], "Description" => ["Batch Completed"])))

    @test_throws JetStreamError run_fetch(Headers("Status" => ["400"], "Description" => ["Bad Request"]))
    @test_throws JetStreamError run_fetch(Headers("Status" => ["409"], "Description" => ["Message Size Exceeds MaxBytes"]))
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

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    js = jetstream(client)
    core_sub = subscribe(client, "_INBOX.pull")
    take!(client.write_io)
    psub = N.PullSubscription(js, core_sub, "ORDERS", "WORKER", "_INBOX.pull", ReentrantLock(), ReentrantLock(), false, false)
    N._dispatch_msg(client, Msg("_INBOX.pull", nothing, UInt8[];
                                headers=Headers("Status" => ["404"], "Description" => ["No Messages"]),
                                sid=core_sub.sid))
    @test isempty(fetch(psub, 1; timeout=0.1, heartbeat=0.02))
    @test occursin("\"idle_heartbeat\":20000000", String(take!(client.write_io)))
    close(psub)

    timeout_client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    timeout_js = jetstream(timeout_client)
    timeout_sub = subscribe(timeout_client, "_INBOX.timeout")
    take!(timeout_client.write_io)
    timeout_psub = N.PullSubscription(timeout_js, timeout_sub, "ORDERS", "WORKER", "_INBOX.timeout", ReentrantLock(), ReentrantLock(), false, false)
    try
        @test_throws JetStreamError fetch(timeout_psub, 1; timeout=0.12, heartbeat=0.02)
    finally
        close(timeout_psub)
    end
end

@testitem "JetStream pull fetch tracks pinned consumer ids" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=IOBuffer())
    js = jetstream(client)
    core_sub = subscribe(client, "_INBOX.pull")
    take!(client.write_io)
    psub = N.PullSubscription(js, core_sub, "ORDERS", "WORKER", "_INBOX.pull", ReentrantLock(), ReentrantLock(), false, false)

    try
        N._dispatch_msg(client, Msg("_INBOX.pull", nothing, TestHelpers.bytes("payload");
                                    headers=Headers("Nats-Pin-Id" => ["pin-a"]),
                                    sid=core_sub.sid))
        @test String(first(fetch(psub, 1; timeout=0.1, heartbeat=0))) == "payload"
        @test psub.pin_id == "pin-a"
        take!(client.write_io)

        N._dispatch_msg(client, Msg("_INBOX.pull", nothing, UInt8[];
                                    headers=Headers("Status" => ["404"], "Description" => ["No Messages"]),
                                    sid=core_sub.sid))
        @test isempty(fetch(psub, 1; timeout=0.1, heartbeat=0))
        request = String(take!(client.write_io))
        @test occursin("\"id\":\"pin-a\"", request)
        @test !occursin("pin_id", request)

        N._dispatch_msg(client, Msg("_INBOX.pull", nothing, UInt8[];
                                    headers=Headers("Status" => ["423"], "Description" => ["Pin ID Mismatch"]),
                                    sid=core_sub.sid))
        @test_throws JetStreamError fetch(psub, 1; timeout=0.1, heartbeat=0)
        @test isnothing(psub.pin_id)
    finally
        close(psub)
    end
end

@testitem "JetStream subscription close is idempotent" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    js = jetstream(client)

    pull_core = subscribe(client, "_INBOX.pull")
    pull = N.PullSubscription(js, pull_core, "S", "C", "_INBOX.pull", ReentrantLock(), ReentrantLock(), false, false)
    close(pull)
    close(pull)
    @test pull.closed
    @test pull_core.closed

    push_core = subscribe(client, "_INBOX.push")
    push = N.PushSubscription(js, push_core, "S", "C", ReentrantLock(), false, false)
    close(push)
    close(push)
    @test push.closed
    @test push_core.closed
end

@testitem "JetStream hot handles carry concrete client and subscription types" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    js = jetstream(client)
    sub = subscribe(client, "_INBOX.typed")
    msg = JetStreamMsg(Msg("_INBOX.typed", "\$JS.ACK.S.C.1.1.1.0.0", UInt8[]; sid=sub.sid), client)
    pull = N.PullSubscription(js, sub, "S", "C", "_INBOX.typed", ReentrantLock(), ReentrantLock(), false, false)
    push = N.PushSubscription(js, sub, "S", "C", ReentrantLock(), false, false)

    @test fieldtype(typeof(msg), :_client) === typeof(client)
    @test fieldtype(typeof(js), :client) === typeof(client)
    @test fieldtype(typeof(pull), :sub) === typeof(sub)
    @test fieldtype(typeof(push), :sub) === typeof(sub)
    @test only(Base.return_types(ack, Tuple{typeof(msg)})) === Nothing
    @test only(Base.return_types(ack_sync, Tuple{typeof(msg)})) === Msg
end
