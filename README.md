# Natter.jl - Native Julia NATS client

[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/rbeeli/Natter.jl/blob/main/LICENSE)
![Maintenance](https://img.shields.io/maintenance/yes/2026)
[![Documentation](https://img.shields.io/badge/docs-stable-blue.svg)](https://rbeeli.github.io/Natter.jl/)

Natter.jl is an independent Julia client for the [NATS](https://nats.io) messaging system.

## Status

This is an initial production-oriented implementation. Core NATS is the first supported
surface; common JetStream and KeyValue APIs are included and will be expanded against
real-server integration tests.

See [`FEATURES_COVERAGE.md`](FEATURES_COVERAGE.md) for the current support matrix.

Full package documentation lives in [`docs/src`](docs/src), is built with
DocumenterVitepress, and is hosted at
<https://rbeeli.github.io/Natter.jl/dev/> after the documentation workflow runs.

```bash
julia --project=docs -e 'using Pkg; Pkg.develop(PackageSpec(path=pwd())); Pkg.instantiate()'
julia --project=docs docs/make.jl
```

## API Model

The public API is synchronous/convenience-first: `connect`, `publish`, `request`,
`next`, `flush`, `drain`, and JetStream management calls block until their operation
completes or times out.

Internally, each client uses Julia tasks for the reader loop, ping loop, reconnect
loop, and subscription callbacks. Subscriptions can be consumed either by calling
`next(sub; timeout=...)` or by passing a callback to `subscribe`.

Automatic reconnect is enabled by default. After a transient disconnect, the client
reconnects in the background, replays subscriptions, and flushes buffered publishes
without requiring application code to recreate the client.

JetStream stream and consumer management accepts typed Julia configs. Use
`StreamConfig(name="ORDERS", subjects=["orders.*"], storage=StorageType.FILE)` and
`ConsumerConfig(durable_name="worker", ack_policy=AckPolicy.EXPLICIT)` for local
validation and idiomatic enum values. Raw dictionaries remain available as an
escape hatch for fields added by newer servers.

Streams created with `allow_direct=true` can use direct message access through
`stream_message_get(js, "ORDERS"; seq=1, direct=true)`. KeyValue buckets created
or opened with direct access enabled use direct reads automatically in `kv_get`.

TLS is enabled with `tls://` URLs or TLS options. `tls://` performs the TLS handshake
before reading server INFO by default; pass `tls_first=false` only for deployments
that use INFO-first TLS upgrade. Certificate verification is enabled by default; pass
`tls_verify=false` only when connecting to a trusted endpoint where verification is
intentionally disabled.

Core publishes issued while reconnecting are buffered and replayed after reconnect.
If the transport fails after the server has accepted part of a write, replay can
produce duplicate delivery. Use application idempotency or JetStream publish
expectations/message IDs when exactly-once effects matter.

`close(client)` is conservative for application cleanup paths: transport, task, and
callback cleanup failures are reported through `error_cb` and warnings by default.
Use `close(client; throw_errors=true)` when teardown failures should be raised.

## Example

```julia
using Natter

client = connect("nats://localhost:4222")

sub = subscribe(client, "events.created") do msg
    @info "received" subject=msg.subject data=String(msg.data)
end

publish(client, "events.created", "hello")
flush(client)
drain(sub)
close(client)
```

## Tests

Tests are defined with `TestItems.jl` and run through `TestItemRunner.jl`.
Unit tests do not require a server:

```bash
julia --project=. -e 'using Pkg; Pkg.test()'
```

Integration tests require a real `nats-server`. JetStream and KeyValue integration
coverage is enabled separately:

```bash
docker run --rm -p 4222:4222 nats:2.11-alpine -js
NATTER_RUN_INTEGRATION=true NATTER_RUN_JETSTREAM=true julia --project=. -e 'using Pkg; Pkg.test()'
```
