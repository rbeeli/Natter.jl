# JetStream

JetStream adds persistence, acknowledgements, durable consumers, replay, and stream management. Create a `JetStreamContext` from a connected client:

```julia
client = connect("nats://127.0.0.1:4222")
js = jetstream(client; timeout=5.0)
```

## Create A Stream

Use typed Julia config structs for normal code. Only fields you set are sent to the server, common names, subjects, queues, and numeric bounds are validated locally, and typed fields must be reflected by the server response.

```julia
stream_create(js, StreamConfig(
    name="ORDERS",
    subjects=["orders.events.*"],
    retention=RetentionPolicy.LIMITS,
    storage=StorageType.FILE,
    max_msgs=1_000_000,
    max_bytes=10 * 1024 * 1024 * 1024,
    duplicate_window=120.0,
    allow_direct=true,
))
```

Raw dictionaries are available for newer server fields that are not typed yet. Dictionary fields are validated when Natter knows their semantics, and requested fields are verified against the server response:

```julia
stream_update(js, Dict(
    "name" => "ORDERS",
    "subjects" => ["orders.events.*"],
    "metadata" => Dict("owner" => "orders-team"),
))
```

Duration values in typed configs and known dictionary fields are seconds.

## Publish With Acknowledgement

`js_publish` waits for a server acknowledgement and returns `PubAck`.

```julia
ack = js_publish(js, "orders.events.created", """{"id":1001}""";
    stream="ORDERS",
    msg_id="order-1001",
)

@info "stored" stream=ack.stream seq=ack.seq duplicate=ack.duplicate
```

Use `msg_id` with the stream `duplicate_window` for retry-safe publishing. Optimistic constraints are also available:

```julia
js_publish(js, "orders.events.created", """{"id":1002}""";
    stream="ORDERS",
    msg_id="order-1002",
    expected_last_subject_sequence=42,
)
```

Publish independent messages concurrently with Julia tasks:

```julia
acks = Vector{PubAck}(undef, 2)

@sync begin
    @async acks[1] = js_publish(js, "orders.events.created", """{"id":1001}"""; stream="ORDERS")
    @async acks[2] = js_publish(js, "orders.events.created", """{"id":1002}"""; stream="ORDERS")
end
```

For high-volume publishing where you want protocol-level async acks, use `js_publish_future`. The context owns one reply subscription, tracks pending acks, and applies backpressure at `publish_future_max_pending`.

```julia
js = jetstream(client; timeout=5.0, publish_future_max_pending=512)

futures = [
    js_publish_future(js, "orders.events.created", """{"id":$id}""";
        stream="ORDERS",
        msg_id="order-$id",
    )
    for id in 1001:1100
]

js_publish_future_complete(js; timeout=5.0)
acks = fetch.(futures)
```

`fetch(future)` returns `PubAck` or throws the publish error for that message. Use `js_publish_future_pending(js)` to inspect the current pending count.

Pending `js_publish_future` futures are not replayed after a reconnect. If the connection enters reconnect, the context clears outstanding async publish futures with `ConnectionReconnectingError`; publish again after reconnect and use `msg_id` when duplicate effects matter. JetStream publish retries server `NoRespondersError` responses twice by default with a 250 ms wait; `retry_attempts` and `retry_wait` tune that behavior and only apply while the same connection generation is still active.

## Durable Pull Worker

Pull consumers are a good default for durable workers because the application controls batch size and acknowledgement.

```julia
worker = pull_subscribe(js, "orders.events.created";
    stream="ORDERS",
    durable="orders-workers",
    timeout=2.0,
    config=ConsumerConfig(
        ack_policy=AckPolicy.EXPLICIT,
        ack_wait=30.0,
        max_ack_pending=500,
    ),
)

for msg in fetch(worker, 25; timeout=2.0)
    try
        handle_order(String(msg))
        ack(msg)
    catch err
        @error "order failed" exception=err
        nak(msg; delay=2.0)
    end
end
```

Fetch options cover common pull request shapes:

```julia
msgs = fetch(worker, 100; timeout=2.0, max_bytes=512 * 1024)
available = fetch(worker, 10; timeout=1.0, no_wait=true)
```

For long-running workers, `messages` keeps a pull subscription refilled behind a bounded channel:

```julia
stream = messages(worker;
    batch=100,
    threshold_messages=50,
    expires=30.0,
    channel_size=100,
)

try
    for msg in stream
        handle_order(String(msg))
        ack(msg)
    end
finally
    close(stream)
end
```

Use `consume` for a callback worker:

```julia
worker_stream = consume(worker; batch=100, expires=30.0) do msg
    handle_order(String(msg))
    ack(msg)
end

close(worker_stream)
```

Only one active `fetch`, `messages`, or `consume` stream should use a pull subscription at a time.

## Push Consumers

Push consumers deliver through a NATS subscription. Callback subscriptions auto-ack by default for acking consumers; set `manual_ack=true` when the callback handles acknowledgements itself.

```julia
push = push_subscribe(js, "orders.events.created";
    stream="ORDERS",
    durable="orders-push",
    manual_ack=true,
    config=ConsumerConfig(
        ack_policy=AckPolicy.EXPLICIT,
        idle_heartbeat=10.0,
    ),
    callback=msg -> begin
        handle_order(String(msg))
        ack(msg)
    end,
)
```

For hot callbacks that finish with the message before returning, `borrowed=true` delivers a `BorrowedJetStreamMsg` with a borrowed byte view and still handles JetStream heartbeats and flow-control frames before user delivery.

```julia
push = push_subscribe(js, "orders.events.created";
    stream="ORDERS",
    borrowed=true,
    callback=msg -> process_order(msg.data),
)
```

Use a queue group when several push subscribers should share one server-side consumer:

```julia
push = push_subscribe(js, "orders.events.created";
    stream="ORDERS",
    durable="orders-push-workers",
    queue="orders-workers",
    callback=msg -> handle_order(String(msg)),
)
```

Ordered push consumers are ephemeral, no-ack consumers that reset automatically after sequence gaps or missed heartbeats. They do not support durable names, queue groups, or binding an existing consumer.

```julia
ordered = push_subscribe(js, "orders.events.created";
    stream="ORDERS",
    ordered=true,
    config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW),
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

`ack_sync` waits for a server reply. Use `in_progress` for work that may exceed `ack_wait`, and `term` when a message should not be redelivered.

Delivery metadata is available on JetStream messages:

```julia
md = metadata(msg)
@info "delivery" stream=md.stream sequence=md.stream_sequence pending=md.pending
```

## Inspect Stored Messages

Read stored messages by sequence or subject:

```julia
by_sequence = stream_message_get(js, "ORDERS"; seq=42)
latest_order = stream_message_get(js, "ORDERS"; subject="orders.events.created")

@info "stored message" sequence=by_sequence.seq created=by_sequence.created
```

For streams created with `allow_direct=true`, direct reads avoid the normal management response envelope:

```julia
fast = stream_message_get(js, "ORDERS"; seq=42, direct=true)
```

Delete a stored message by sequence:

```julia
stream_message_delete(js, "ORDERS", 42)
```

## Manage Streams And Consumers

```julia
info = stream_info(js, "ORDERS")
names = stream_names(js; subject="orders.events.created")
streams = stream_list(js)

for page in stream_list_pages(js)
    @info "stream page" offset=page.offset total=page.total count=length(page)
end

for consumer in consumer_list_iter(js, "ORDERS")
    @info "consumer" name=consumer.name
end

consumer_create(js, "ORDERS", ConsumerConfig(
    durable_name="audit-reader",
    ack_policy=AckPolicy.EXPLICIT,
    deliver_policy=DeliverPolicy.ALL,
))

consumer_info(js, "ORDERS", "audit-reader")
consumer_delete(js, "ORDERS", "audit-reader")
stream_purge(js, "ORDERS"; filter_subject="orders.events.failed")
```

`consumer_create` is strict create-only, `consumer_update` is strict update-only, and `consumer_create_or_update` is the explicit upsert API.
