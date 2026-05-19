# Natter.jl

Natter.jl is a pure Julia client for [NATS](https://nats.io). It is built for service code: connect once, publish and subscribe directly from Julia tasks, and let the client handle pings, reconnects, subscription replay, and cleanup.

## A First Client

```julia
using Natter

client = connect("nats://127.0.0.1:4222"; name="orders-api")

sub = subscribe(client, "orders.created") do msg
    @info "order created" id=String(msg)
end

publish(client, "orders.created", "order-1001")
flush(client)

drain(sub)
close(client)
```

For concurrent work, use Julia tasks around the normal direct calls:

```julia
@sync begin
    Threads.@spawn publish(client, "orders.created", "order-1002")
    Threads.@spawn publish(client, "orders.created", "order-1003")
end

flush(client)
```

## Common Use Cases

| Use case | Start here |
| :--- | :--- |
| Publish/subscribe, queue workers, request/reply, headers | [Core Messaging](core.md) |
| Persistent streams, durable workers, acknowledgements | [JetStream](jetstream.md) |
| Configuration, profiles, leases, and watched state | [KeyValue](keyvalue.md) |
| Reconnect behavior, production options, TLS and auth | [Reliability And TLS](reliability.md) |
| Copyable end-to-end snippets | [Examples](examples/index.md) |
| Full exported API summary | [Reference](reference.md) |

## Feature Snapshot

Natter.jl supports core NATS messaging, automatic reconnect, token/user-password/NKEY/JWT/`.creds` auth, TLS and mTLS, JetStream stream and consumer management, pull and push consumers, publish acknowledgements, message lookup, KeyValue buckets, optimistic writes, delete/purge operations, and watchers.

See [Feature Coverage](feature-coverage.md) for current support status and hardening notes.
