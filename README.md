# Natter.jl - Pure Julia NATS client

[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/rbeeli/Natter.jl/blob/main/LICENSE)
![Maintenance](https://img.shields.io/maintenance/yes/2026)
[![Documentation](https://img.shields.io/badge/docs-stable-blue.svg)](https://rbeeli.github.io/Natter.jl/)

Natter.jl is a Julia client for [NATS](https://nats.io). It provides ergonomic direct APIs for application code and uses Julia tasks internally for the reader loop, pings, reconnects, subscription callbacks, protocol futures, streams, and watchers. Application concurrency uses normal Julia `Threads.@spawn`/`@sync` task composition over Julia's async I/O model; Natter's internal tasks use Julia thread-pool scheduling where it helps keep control-plane work responsive.

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
    Threads.@spawn publish(client, "events.created", "a")
    Threads.@spawn publish(client, "events.created", "b")
end

flush(client)
```

## Where To Go Next

- [Getting Started](https://rbeeli.github.io/Natter.jl/getting-started) covers installation, a local server, and the basic client lifecycle.
- [Core Messaging](https://rbeeli.github.io/Natter.jl/core) explains publish/subscribe, request/reply, headers, queue groups, and draining.
- [JetStream](https://rbeeli.github.io/Natter.jl/jetstream) covers streams, consumers, typed configs, direct get, and acknowledgements.
- [KeyValue](https://rbeeli.github.io/Natter.jl/keyvalue) documents buckets, reads, writes, watches, and direct access.
- [Reliability And TLS](https://rbeeli.github.io/Natter.jl/reliability) describes reconnect behavior, buffering, callbacks, and TLS options.
- [Examples](https://rbeeli.github.io/Natter.jl/examples/connection-auth-tls) provide complete patterns for common application code.

## Performance Reports

Run the live-server performance report from the repository root:

```bash
docker run -d --rm --name natter-perf -p 4222:4222 nats:2.11-alpine -js
env JULIA_NUM_THREADS=2 julia --project=. benchmarks/run.jl
docker stop natter-perf
```

The `Performance` GitHub Actions workflow runs the same report on a schedule and on demand, then uploads Markdown and JSON artifacts for trend review.
