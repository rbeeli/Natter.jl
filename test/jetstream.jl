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

@testitem "JetStream push control dispatch filters heartbeats" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    push_sub = subscribe(client, "_INBOX.push"; _control_handler=N._JetStreamPushControlHandler())

    heartbeat = Msg("_INBOX.push", nothing, UInt8[];
                    headers=Headers("Status" => ["100"], "Description" => ["Idle Heartbeat"]),
                    client, sid=push_sub.sid)
    N._dispatch_msg(client, heartbeat)
    @test !isready(push_sub.messages)
    @test client.pending_bytes == 0

    plain_sub = subscribe(client, "_INBOX.plain")
    plain_heartbeat = Msg("_INBOX.plain", nothing, UInt8[];
                          headers=Headers("Status" => ["100"], "Description" => ["Idle Heartbeat"]),
                          client, sid=plain_sub.sid)
    N._dispatch_msg(client, plain_heartbeat)
    @test N._status_header(next(plain_sub; timeout=0.1)) == 100

    close(push_sub)
    close(plain_sub)
end

@testitem "JetStream push flow control replies internally" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    sub = subscribe(client, "_INBOX.push"; _control_handler=N._JetStreamPushControlHandler())
    flow_control = Msg("_INBOX.push", "_INBOX.fc", UInt8[];
                       headers=Headers("Status" => ["100"], "Description" => ["FlowControl Request"]),
                       client, sid=sub.sid)

    N._dispatch_msg(client, flow_control)

    expected = "PUB _INBOX.fc 0\r\n\r\n"
    @test !isready(sub.messages)
    @test client.pending_bytes == ncodeunits(expected)
    @test String(take!(client.pending)) == expected
    close(sub)
end

@testitem "JetStream push flow control reply failures use error callback" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    errors = Channel{Any}(1)
    opts = ConnectOptions(error_cb=err -> put!(errors, err))
    client = TestHelpers.fake_client(; opts, status=N.ConnectionStatus.RECONNECTING)
    sub = subscribe(client, "_INBOX.push"; _control_handler=N._JetStreamPushControlHandler())
    @lock client.lock client.status = N.ConnectionStatus.DISCONNECTED

    flow_control = Msg("_INBOX.push", "_INBOX.fc", UInt8[];
                       headers=Headers("Status" => ["100"], "Description" => ["FlowControl Request"]),
                       client, sid=sub.sid)
    N._dispatch_msg(client, flow_control)

    @test !isready(sub.messages)
    @test timedwait(1.0; pollint=0.01) do
        isready(errors)
    end != :timed_out
    @test take!(errors) isa ConnectionClosedError
    close(sub)
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
                    client, sid=sub.sid)
    flow_control = Msg("_INBOX.callback", "_INBOX.fc", UInt8[];
                       headers=Headers("Status" => ["100"], "Description" => ["FlowControl Request"]),
                       client, sid=sub.sid)
    N._dispatch_msg(client, heartbeat)
    N._dispatch_msg(client, flow_control)

    @test timedwait(0.1; pollint=0.01) do
        isready(received)
    end == :timed_out

    N._dispatch_msg(client, Msg("_INBOX.callback", nothing, TestHelpers.bytes("work"); client, sid=sub.sid))
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

@testitem "JetStream direct get request validation and response lifting" setup=[TestHelpers] begin
    using Natter
    using Dates

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
    @test_throws ArgumentError fetch(psub, 1; timeout=0.0)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=-1.0)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, expires=0.0)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, expires=-1.0)
    @test client.pending_bytes == 0
    @test_throws ArgumentError fetch(psub, 1; timeout=1.0, expires=2.0)
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
