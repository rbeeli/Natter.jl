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
end

@testitem "JetStream typed config rejects invalid local metadata" begin
    using Natter

    const N = Natter

    @test_throws MethodError StreamConfig(name="S", metadata=Dict{String,Any}("ok" => 1))
    @test_throws ArgumentError N._js_field_value(:metadata, Dict{String,Any}("ok" => 1))
end

@testitem "JetStream direct get request validation and response lifting" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    @test N._stream_message_get_request(1, nothing, false) == Dict{String,Any}("seq" => 1)
    @test N._stream_message_get_request(nothing, "orders.created", false) == Dict{String,Any}("last_by_subj" => "orders.created")
    @test N._stream_message_get_request(2, "orders.created", true) == Dict{String,Any}("seq" => 2, "next_by_subj" => "orders.created")
    @test_throws ArgumentError N._stream_message_get_request(nothing, nothing, false)
    @test_throws ArgumentError N._stream_message_get_request(0, nothing, false)
    @test_throws ArgumentError N._stream_message_get_request(1, "orders.created", false)
    @test_throws ArgumentError N._stream_message_get_request(nothing, "orders.created", true)
    @test_throws ArgumentError N._stream_message_get_request(0, "orders.created", true)
    @test_throws ArgumentError N._stream_message_get_request(1, "orders.*", true)

    client = TestHelpers.fake_client()
    js = jetstream(client)
    response = Msg("_INBOX.reply", nothing, Vector{UInt8}(codeunits("payload"));
                   headers=Headers(
                       "Nats-Stream" => ["ORDERS"],
                       "Nats-Subject" => ["orders.created"],
                       "Nats-Sequence" => ["7"],
                       "Nats-Time-Stamp" => ["2026-01-01T00:00:00Z"],
                       "X-Test" => ["ok"],
                   ))
    msg = N._direct_message_response(js, "\$JS.API.DIRECT.GET.ORDERS", response)
    @test msg.subject == "orders.created"
    @test String(msg) == "payload"
    @test header(msg, "X-Test") == "ok"
    @test isnothing(header(msg, "Nats-Sequence"))

    not_found = Msg("_INBOX.reply", nothing, UInt8[]; headers=Headers("Status" => ["404"], "Description" => ["no message found"]))
    @test_throws JetStreamError N._direct_message_response(js, "\$JS.API.DIRECT.GET.ORDERS", not_found)
    no_responders = Msg("_INBOX.reply", nothing, UInt8[]; headers=Headers("Status" => ["503"]))
    @test_throws NoRespondersError N._direct_message_response(js, "\$JS.API.DIRECT.GET.MISSING", no_responders)
end

@testitem "JetStream metadata" begin
    using Natter

    meta = metadata(Msg("s", "\$JS.ACK.ORDERS.C1.2.10.4.123456789.7", UInt8[]))
    @test meta.stream == "ORDERS"
    @test meta.consumer == "C1"
    @test meta.delivered == 2
    @test meta.stream_sequence == 10
    @test meta.consumer_sequence == 4
    @test meta.timestamp_ns == 123456789
    @test meta.pending == 7

    meta2 = metadata(Msg("s", "\$JS.ACK._.acc.ORDERS.C1.3.11.5.987654321.8.rand", UInt8[]))
    @test meta2.domain == ""
    @test meta2.timestamp_ns == 987654321
end
