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

Raw dictionaries are also accepted as an escape hatch for fields added by newer servers:

```julia
stream_update(js, Dict(
    "name" => "ORDERS",
    "subjects" => ["orders.*"],
    "metadata" => Dict("owner" => "orders-team"),
))
```

## Publish

`js_publish` waits for a publish acknowledgement and returns a `PubAck`.

```julia
ack = js_publish(js, "orders.created", """{"id":1001}""";
    stream="ORDERS",
    headers=Headers("Nats-Msg-Id" => ["order-1001"]),
)

@info "stored" stream=ack.stream seq=ack.seq duplicate=ack.duplicate
```

`publish_async` starts the publish in a Julia task and returns that task.

```julia
task = publish_async(js, "orders.created", "payload"; stream="ORDERS")
ack = fetch(task)
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

## Pull Consumers

Pull subscriptions create or bind a consumer and fetch batches on demand.

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

Close ephemeral subscriptions when finished. Durable consumers remain on the server.

```julia
close(sub)
```

## Push Consumers

Push subscriptions deliver messages to a normal NATS subscription. Set `manual_ack=true` when the callback will acknowledge messages itself.

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
