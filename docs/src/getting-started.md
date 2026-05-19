# Getting Started

## Install

In an application project:

```julia
using Pkg
Pkg.add(url="https://github.com/rbeeli/Natter.jl")
```

Then import the package:

```julia
using Natter
```

JetStream and KeyValue APIs live in submodules. Import them when using those sections:

```julia
using Natter.JetStream
using Natter.KeyValue
```

For this repository, instantiate the local project instead:

```bash
julia --project=. -e 'using Pkg; Pkg.instantiate()'
```

## Run NATS Locally

Core messaging:

```bash
docker run --rm -p 4222:4222 nats:2.11-alpine
```

JetStream and KeyValue:

```bash
docker run --rm -p 4222:4222 nats:2.11-alpine -js
```

## Connect

```julia
client = connect("nats://127.0.0.1:4222";
    name="orders-api",
    connect_timeout=2.0,
)
```

Use multiple URLs for failover:

```julia
client = connect([
    "nats://nats-a.internal:4222",
    "nats://nats-b.internal:4222",
])
```

Natter randomizes multi-server attempt order by default so client startups spread across the pool.

## Publish And Subscribe

Channel-style subscriptions are useful in scripts and tests:

```julia
sub = subscribe(client, "orders.created")

publish(client, "orders.created", "order-1001")
msg = next(sub; timeout=1.0)

@assert String(msg) == "order-1001"
close(sub)
```

Callbacks are the usual service style:

```julia
sub = subscribe(client, "orders.created") do msg
    @info "new order" id=String(msg)
end

publish(client, "orders.created", "order-1002")
flush(client)
```

Use queue groups when many workers should share one subject:

```julia
worker = subscribe(client, "orders.process"; queue="order-workers") do msg
    process_order(String(msg))
end
```

## Request Reply

```julia
service = subscribe(client, "orders.lookup"; callback_mode=:inline) do msg
    isnothing(msg.reply) && return
    respond(client, msg, lookup_order(String(msg)))
end

response = request(client, "orders.lookup", "order-1001"; timeout=1.0)
println(String(response))
```

## JetStream In One Minute

Start the server with `-js`, then create a stream and publish with acknowledgements:

```julia
js = jetstream(client)

stream_create(js, StreamConfig(
    name="ORDERS",
    subjects=["orders.events.*"],
    storage=StorageType.FILE,
    duplicate_window=120.0,
))

ack = js_publish(js, "orders.events.created", "order-1001";
    stream="ORDERS",
    msg_id="order-1001",
)
```

Create a durable pull worker:

```julia
worker = pull_subscribe(js, "orders.events.created";
    stream="ORDERS",
    durable="order-workers",
    config=ConsumerConfig(ack_policy=AckPolicy.EXPLICIT),
)

for msg in fetch(worker, 10; timeout=2.0)
    process_order(String(msg))
    ack(msg)
end
```

## KeyValue In One Minute

```julia
kv = kv_create(js, "settings"; history=5, direct=true)

revision = kv_put(kv, "checkout.currency", "CHF")
entry = kv_get(kv, "checkout.currency")

@assert entry.revision == revision
@assert String(entry) == "CHF"
```

## Shutdown

Use `drain` when shutdown should finish queued subscription work first. Use `close` for immediate teardown.

```julia
drain(client; timeout=10.0)
```

```julia
close(client)
```
