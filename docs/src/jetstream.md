# JetStream

JetStream support is exposed through a `JetStreamContext` created from a connected client. Timeout values are positive finite seconds.

```julia
client = connect("nats://127.0.0.1:4222")
js = jetstream(client; timeout=5.0)
```

## Typed Configuration

Streams and consumers use typed Julia config structs. Optional fields default to `nothing`, so only explicitly set fields are sent to the server.
Typed configs validate known subject, name, queue, and numeric bounds locally before sending management requests.

```julia
stream_create(js, StreamConfig(
    name="ORDERS",
    subjects=["orders.*"],
    retention=RetentionPolicy.LIMITS,
    storage=StorageType.FILE,
    max_msgs=1_000_000,
    max_bytes=10 * 1024 * 1024 * 1024,
    allow_direct=true,
))
```

Consumer configs use the same pattern:

```julia
consumer_create(js, "ORDERS", ConsumerConfig(
    durable_name="orders-worker",
    ack_policy=AckPolicy.EXPLICIT,
    deliver_policy=DeliverPolicy.ALL,
    max_ack_pending=500,
    ack_wait=30.0,
))
```

`consumer_create` is strict create-only and `consumer_update` is strict update-only on servers that support consumer actions. Use `consumer_create_or_update` only when create-or-update upsert behavior is intentional.

Raw dictionaries are also accepted as an escape hatch for fields added by newer servers:

```julia
stream_update(js, Dict(
    "name" => "ORDERS",
    "subjects" => ["orders.*"],
    "metadata" => Dict("owner" => "orders-team"),
))
```

Known typed fields in raw dictionaries use the same Julia-side units as typed configs. Duration values such as `ack_wait`, `max_age`, and `backoff` are seconds and are serialized to the NATS wire format in nanoseconds.

## Publish

`js_publish` waits for a publish acknowledgement and returns a `PubAck`.

```julia
ack = js_publish(js, "orders.created", """{"id":1001}""";
    stream="ORDERS",
    msg_id="order-1001",
    expected_last_subject_sequence=42,
    ttl=300.0,
)

@info "stored" stream=ack.stream seq=ack.seq duplicate=ack.duplicate
```

Publish options map to JetStream headers for deduplication, optimistic concurrency, per-message TTL, and message schedules. `msg_id` enables server-side duplicate detection within the stream `duplicate_window`, which is the recommended way to suppress duplicate publishes caused by reconnect or retry ambiguity. TTL values are seconds and must be at least `1.0`. Use `expected_last_sequence`, `expected_last_subject_sequence`, `expected_last_subject`, `expected_last_msg_id`, `schedule_at`, `schedule_every`, `schedule`, `schedule_target`, `schedule_source`, `schedule_ttl`, and `schedule_timezone` as needed. `retry_attempts` and `retry_wait` retry publish requests that receive a no-responders status.

Publish independent messages concurrently with Julia tasks:

```julia
acks = Vector{PubAck}(undef, 2)

@sync begin
    @async acks[1] = js_publish(js, "orders.created", """{"id":1001}"""; stream="ORDERS")
    @async acks[2] = js_publish(js, "orders.created", """{"id":1002}"""; stream="ORDERS")
end
```

## Stream Management

```julia
info = stream_info(js, "ORDERS")
names = stream_names(js; subject="orders.created")
streams = stream_list(js)

stream_purge(js, "ORDERS"; filter_subject="orders.failed")
stream_delete(js, "ORDERS")
```

## Message Lookup And Direct Get

Use `stream_message_get` to read a stored message by sequence or by the latest message for a subject.

```julia
by_sequence = stream_message_get(js, "ORDERS"; seq=42)
latest_order = stream_message_get(js, "ORDERS"; subject="orders.created")
```

For streams created with `allow_direct=true`, direct get bypasses the normal management response envelope and returns the stored message directly.

```julia
fast = stream_message_get(js, "ORDERS"; seq=42, direct=true)
last = stream_message_get(js, "ORDERS"; subject="orders.created", direct=true)
```

`next_by_subject=true` asks for the next sequence at or after `seq` for a subject.

```julia
msg = stream_message_get(js, "ORDERS";
    seq=100,
    subject="orders.created",
    next_by_subject=true,
)
```

Delete a stored message by positive stream sequence:

```julia
deleted = stream_message_delete(js, "ORDERS", 42)
```

## Pull Consumers

Pull subscriptions create or bind a consumer and fetch batches on demand. Fetch calls on the same pull subscription are serialized. Durable or named consumers are bound when they already exist; any supplied config fields must match the existing consumer config. Pull subscription configs cannot set push delivery fields such as `deliver_subject` or `deliver_group`. Missing durable or named consumers are created strictly, and random ephemeral consumers are created strictly and deleted on close. Subscription setup accepts `timeout=...` for its JetStream API calls. Pull fetches return `JetStreamMsg` values, which carry the acknowledgement state needed by `ack`, `nak`, `in_progress`, and `term`.

```julia
sub = pull_subscribe(js, "orders.created";
    stream="ORDERS",
    durable="orders-workers",
    timeout=2.0,
    config=ConsumerConfig(
        ack_policy=AckPolicy.EXPLICIT,
        max_ack_pending=200,
    ),
)

for msg in fetch(sub, 10; timeout=2.0)
    try
        handle_order(String(msg.data))
        ack(msg)
    catch err
        nak(msg; delay=1.0)
    end
end
```

Fetch requests use a unique reply subject under the pull subscription inbox, so late terminal status messages from an older request are ignored by the next request. The server-side request expiration defaults to a value shorter than the caller `timeout` (10% shorter, capped at a 5 second margin), so a timed-out local wait does not leave a live server request that can deliver data into a later fetch. Fetch requests are not replayed after reconnect. If the connection is lost before any messages arrive, `fetch` throws `FetchDisconnectedError`; retry the fetch after reconnect. JetStream delivery remains at-least-once until messages are acknowledged, so handlers should be idempotent when duplicate effects matter. For fetches with effective `expires >= 10`, Natter requests JetStream idle heartbeats and reports missed heartbeats as `JetStreamError`. Use `heartbeat=0` to disable heartbeat monitoring or set a shorter positive heartbeat explicitly; `expires` must be shorter than `timeout` and at least twice the heartbeat.

Bounded fetches also support byte-limited and no-wait pull requests:

```julia
msgs = fetch(sub, 100; timeout=2.0, max_bytes=256 * 1024)
available = fetch(sub, 10; timeout=1.0, no_wait=true)
```

For long-running pull consumers, `messages` starts a threshold-refilled message stream. The returned `PullMessageStream` is iterable, supports `take!`, and should be closed when the worker exits. Only one active fetch or message stream may use a pull subscription at a time.

```julia
stream = messages(sub;
    batch=100,
    max_bytes=1024 * 1024,
    threshold_messages=50,
    threshold_bytes=512 * 1024,
    expires=30.0,
)

try
    for msg in stream
        process(String(msg))
        ack(msg)
    end
finally
    close(stream)
end
```

Use `consume` when a callback-oriented worker is a better fit:

```julia
worker = consume(sub; batch=100, expires=30.0) do msg
    process(String(msg))
    ack(msg)
end

close(worker)
```

To process a fetched batch concurrently, use a structured `@sync` boundary:

```julia
msgs = fetch(sub, 10; timeout=2.0)

@sync for msg in msgs
    @async begin
        try
            handle_order(String(msg.data))
            ack(msg)
        catch err
            nak(msg; delay=1.0)
        end
    end
end
```

Close ephemeral subscriptions when finished. Durable consumers remain on the server.

```julia
close(sub)
```

## Push Consumers

Push subscriptions deliver messages to a normal NATS subscription. Durable or named push consumers bind to existing consumers without updating server-side config; supplied config fields must match. Queue groups can be set with `queue` or `ConsumerConfig(deliver_group=...)`; when both are present, they must match. Binding an existing queue push consumer requires the `queue` keyword to match the consumer deliver group. If no durable or name is supplied for a queue push subscription, the queue group is used as the durable consumer name so additional subscribers join the same server-side consumer. Existing non-queue push consumers cannot be bound by a second subscription while the server reports them as push-bound; use a queue group when multiple subscribers should share one push consumer. Subscription setup accepts `timeout=...` for its JetStream API calls. Non-queue push consumers with idle heartbeats report missed heartbeats through `error_cb`, ignore heartbeat control messages, and reply only to flow-control requests. Push callbacks and channel-backed `next(sub)` calls receive `JetStreamMsg` values; callback-backed push subscriptions are callback-only. Callback push subscriptions auto-ack by default for acking consumers; `AckPolicy.NONE` consumers are not auto-acked. Set `manual_ack=true` when the callback will acknowledge messages itself.

```julia
sub = push_subscribe(js, "orders.created";
    stream="ORDERS",
    durable="orders-push",
    timeout=2.0,
    manual_ack=true,
    callback=msg -> begin
        handle_order(String(msg.data))
        ack(msg)
    end,
)
```

## Acknowledgements

```julia
ack(msg)
ack_sync(msg; timeout=1.0)
nak(msg; delay=2.0)
in_progress(msg)
term(msg)
```

Terminal acknowledgements (`ack`, `nak`, and `term`) are written through the active transport and are not queued for reconnect replay. If the client is reconnecting or the write fails before Natter can confirm the transport write, the call throws and the local message can be acknowledged again. `ack_sync` additionally waits for the server acknowledgement response.

Use `metadata(msg)` for JetStream delivery metadata on received consumer messages.
