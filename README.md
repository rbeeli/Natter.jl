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

## Benchmarks

<!-- NATTER_BENCHMARK_TABLE_START -->
Benchmark parameters: `50000` messages, `5000` requests, `64` byte payload, `3` trials per client, URL `nats://127.0.0.1:4222`. Timed regions exclude startup, package loading, compilation/build time, dependency downloads, and benchmark warmup.

Benchmarks use each client's common high-level publish, subscribe, request, and flush APIs. Natter.jl callback dispatch uses `callback_mode=:inline` in this comparison; the Natter-only report also includes task-backed callback rows. The table reports best-of-`3` results: rates use the highest throughput, while durations and latencies use the lowest observed value.

Optimization modes: Natter.jl runs with `julia --startup-file=no -O3 --check-bounds=no -C native`; Go runs from an explicit `go build -trimpath` binary; Rust runs from `cargo build --release`; the NATS server uses the official `nats:2.11` image.

| Metric | Natter.jl | Go nats.go | Rust nats.rs | Python nats.py |
| :--- | ---: | ---: | ---: | ---: |
| Publish batch / queued msg/s | 4,858,044 | 5,424,487 | 4,929,742 | 923,915 |
| Publish + flush each msg/s | 29,623 | 22,837 | 128,290 | 22,879 |
| Callback dispatch inline batch msg/s | 2,783,611 | 3,180,066 | 2,682,412 | 265,970 |
| Request/reply req/s | 18,868 | 11,748 | 17,372 | 9,727 |
| Request p50 latency ms | 0.046 | 0.054 | 0.049 | 0.090 |
| Request p95 latency ms | 0.115 | 0.223 | 0.116 | 0.171 |

Generated from benchmark JSON artifacts at `2026-05-19T22:31:28.363`.
<!-- NATTER_BENCHMARK_TABLE_END -->
