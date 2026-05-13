# JetStream

JetStream support is exposed through a `JetStreamContext` created from a connected client.

```julia
client = connect("nats://127.0.0.1:4222")
js = jetstream(client; timeout=5.0)
```

## Typed Configuration

Streams and consumers use typed Julia config structs. Optional fields default to `nothing`, so only explicitly set fields are sent to the server.

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
    headers=Dict("Nats-Msg-Id" => "order-1001"),
)

@info "stored" stream=ack.stream seq=ack.seq duplicate=ack.duplicate
```

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

Delete a stored message by stream sequence:

```julia
deleted = stream_message_delete(js, "ORDERS", 42)
```

## Pull Consumers

Pull subscriptions create or bind a consumer and fetch batches on demand. Durable or named consumers are bound when they already exist; any supplied config fields must match the existing consumer config. Missing durable or named consumers are created strictly, and random ephemeral consumers are created strictly and deleted on close.

```julia
sub = pull_subscribe(js, "orders.created";
    stream="ORDERS",
    durable="orders-workers",
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

Push subscriptions deliver messages to a normal NATS subscription. Durable or named push consumers bind to existing consumers without updating server-side config; supplied config fields must match. Queue groups can be set with `queue` or `ConsumerConfig(deliver_group=...)`; when both are present, they must match. If no durable or name is supplied for a queue push subscription, the queue group is used as the durable consumer name so additional subscribers join the same server-side consumer. Set `manual_ack=true` when the callback will acknowledge messages itself.

```julia
sub = push_subscribe(js, "orders.created";
    stream="ORDERS",
    durable="orders-push",
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

Use `metadata(msg)` for JetStream delivery metadata on received consumer messages.
