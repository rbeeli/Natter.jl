# Getting Started

## Add The Package

For a checked-out repository, activate the project and instantiate dependencies:

```bash
julia --project=. -e 'using Pkg; Pkg.instantiate()'
```

For application projects, add Natter.jl as a normal Julia dependency and import it with:

```julia
using Natter
```

## Run A Local Server

Most examples assume a local NATS server:

```bash
docker run --rm -p 4222:4222 nats:2.11-alpine
```

JetStream and KeyValue examples need JetStream enabled:

```bash
docker run --rm -p 4222:4222 nats:2.11-alpine -js
```

## Connect

```julia
using Natter

client = connect("nats://127.0.0.1:4222";
    name="orders-api",
    connect_timeout=2.0,
)
```

`connect` also accepts multiple URLs. The client tries servers in order and keeps discovered servers for reconnects.

```julia
client = connect([
    "nats://nats-a.internal:4222",
    "nats://nats-b.internal:4222",
    "nats://nats-c.internal:4222",
])
```

## Publish And Subscribe

```julia
sub = subscribe(client, "orders.created")

publish(client, "orders.created", "order-1001")
msg = next(sub; timeout=1.0)

@assert String(msg.data) == "order-1001"
close(sub)
```

Callbacks are run by a Julia task owned by the subscription:

```julia
sub = subscribe(client, "orders.created") do msg
    @info "new order" id=String(msg.data)
end
```

Run independent work concurrently with Julia tasks:

```julia
@sync begin
    @async publish(client, "orders.created", "order-1002")
    @async publish(client, "orders.created", "order-1003")
end

flush(client)
```

## Close Cleanly

Use `drain` when a service is shutting down and should finish in-flight messages first. Use `close` for immediate teardown.

```julia
drain(client; timeout=10.0)
```

```julia
close(client)
```
