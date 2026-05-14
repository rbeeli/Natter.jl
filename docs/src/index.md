# Natter.jl - Pure Julia NATS client

Natter.jl is a Julia client for [NATS](https://nats.io). It provides ergonomic direct APIs for application code and uses Julia tasks internally for the reader loop, pings, reconnects, subscription callbacks, and explicit async handles.

The client is intended for long-running services:

- Core publish, subscribe, queue groups, headers, request/reply, flush, drain, and close.
- Background reconnect with subscription replay and bounded publish buffering.
- TLS, including TLS-first servers and optional certificate verification control.
- JetStream stream and consumer management with typed Julia configuration structs.
- Pull and push consumers, explicit acknowledgements, publish acknowledgements, message lookup, and direct get.
- KeyValue buckets with direct reads, history, keys, optimistic writes, deletes, purges, and watches.

## Quick Start

```julia
using Natter

client = connect("nats://127.0.0.1:4222")

sub = subscribe(client, "events.created") do msg
    @info "received event" subject=msg.subject data=String(msg.data)
end

publish(client, "events.created", "hello")
flush(client)

drain(sub)
close(client)
```

Use Julia tasks when your application needs concurrency:

```julia
@sync begin
    @async publish(client, "events.created", "a")
    @async publish(client, "events.created", "b")
end

flush(client)
```

## Where To Go Next

- [Getting Started](getting-started.md) covers installation, a local server, and the basic client lifecycle.
- [Core Messaging](core.md) explains publish/subscribe, request/reply, headers, queue groups, and draining.
- [JetStream](jetstream.md) covers streams, consumers, typed configs, direct get, and acknowledgements.
- [KeyValue](keyvalue.md) documents buckets, reads, writes, watches, and direct access.
- [Reliability And TLS](reliability.md) describes reconnect behavior, buffering, callbacks, and TLS options.
- [Examples](examples/basic-pub-sub.md) provide complete patterns for common application code.
