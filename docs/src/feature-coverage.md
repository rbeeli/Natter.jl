# Feature Coverage

This page tracks what Natter.jl supports today and what is intentionally still outside the first implementation.

Statuses:

- `Supported`: implemented for the normal production path and tested where practical.
- `Partial`: usable for common paths, with remaining hardening or API coverage work.
- `Planned`: intended for future work.
- `Not planned`: intentionally excluded for now.

## Core NATS

| Feature | Status | Notes |
| :--- | :--- | :--- |
| TCP transport | Supported | Plain `nats://` connections. |
| TLS transport | Supported | TLS-first, INFO-first upgrade, CA path, certificate verification controls, and client cert/key paths. |
| WebSocket transport | Not Planned | `ws://` and `wss://` are not implemented. |
| Connect handshake | Supported | Client name, no echo, server INFO parsing, TLS requirements, and typed auth. |
| Authentication | Supported | Token, user/password, NKEY, JWT, `.creds`, callback auth, and URL userinfo for token/user-password. |
| PUB / HPUB / SUB / UNSUB | Supported | Core publish, headers, queue groups, unsubscribe limits, local subject validation, replayable publish mode, queued writer publishes, and direct publish mode. |
| MSG / HMSG parsing | Supported | Core data and headers, with `callback_mode=:inline` for callback-only hot subscribers. Inline frames use a lock-free SID snapshot and release oversized parser buffers after delivery. |
| Wildcards | Supported | Subscription wildcards are validated; publish subjects reject wildcards. |
| Request/reply | Supported | Shared inbox mux, timeouts, reconnect-aware in-flight waiters, and `NoRespondersError` when supported by the server. |
| Flush and ping | Supported | `flush`/`ping` use a server round trip. |
| Drain and close | Supported | Subscription/client drain and deterministic close paths. |
| Reconnect | Supported | Automatic reconnect, optional initial-connect retry, randomized server-pool attempts with ordered opt-out, discovered servers, subscription and request mux replay, bounded publish replay, lifecycle events, delay callbacks, and bounded live-server chaos CI are implemented. Broader cluster chaos and auth failover scenarios remain as additional hardening coverage. |
| Publish replay | Supported | Core publishes use `PublishMode.REPLAYABLE` by default and are replayed while they remain in the client buffer and have not been handed to the transport. Ambiguous write failures start reconnect but are not replayed automatically; use `pending_size=0` to disable replay buffering, or JetStream `msg_id` for durable idempotent publish paths. |
| Slow consumer handling | Supported | Per-subscription pending limits report `SlowConsumerError`. |
| Runtime inspection | Supported | `status`, `stats(client)`, `stats(sub)`, and `connected_url`. |
| Task concurrency | Supported | Direct calls compose with Julia `Threads.@spawn`/`@sync`; blocking calls accept cooperative cancellation tokens. JetStream publish has a protocol async publisher with `JetStreamPublishFuture`. |

## JetStream

| Feature | Status | Notes |
| :--- | :--- | :--- |
| Context | Supported | `JetStream.jetstream(client)` or `jetstream(client)` after `using Natter.JetStream`. |
| Publish ack | Supported | `js_publish` returns `PubAck` and exposes common dedupe, optimistic constraint, TTL, schedule, and retry options, including two default `NoRespondersError` retries. `js_publish_future` uses protocol-level async publish with pending ack accounting, backpressure, timed/cancellable future waits, per-message ack/error futures, configurable no-responder retries, and explicit pending-future clearing on reconnect rather than replay. |
| Stream management | Supported | Create, update, info, list, names, page and iterator listing, purge, delete. |
| Stream config | Supported | `StreamConfig`, nested helpers, and raw `Dict` payloads. Create/update checks that requested fields, including explicit false, zero, and empty values, are reflected by the server response. Unknown raw fields tolerate additional nested defaults returned by the server. |
| Message lookup | Supported | Sequence, last-by-subject, next-by-subject, direct get with validated metadata headers, stored-message sequence/timestamp metadata, and message delete. |
| Consumer management | Supported | Create, create-or-update, update, info, list, page and iterator listing, delete. |
| Consumer config | Supported | `ConsumerConfig` and raw `Dict` payloads. Create/update checks that requested fields, including explicit false, zero, and empty values, are reflected by the server response. Unknown raw fields tolerate additional nested defaults returned by the server. |
| Pull consumers | Supported | Durable/named bind, ephemeral create/delete, bounded close cleanup, bounded `fetch`, `max_bytes`, `no_wait`, heartbeats, timed/cancellable `messages` reads, `consume`, and priority request fields. |
| Push consumers | Partial | Durable/ephemeral push, ordered ephemeral push, bounded close cleanup, queue groups, callbacks, borrowed callbacks, manual/auto ack, flow control replies, heartbeat reporting, and bounded reconnect chaos coverage. Additional long-duration push chaos remains. |
| Acknowledgements | Supported | `ack`, `ack_sync`, `nak`, `in_progress`, `term`. |
| Metadata | Supported | Delivery metadata parsing. |
| Ordered consumers | Supported | Public ordered ephemeral push consumers use `push_subscribe(...; ordered=true)`, with flow control, heartbeats, and automatic reset after sequence gaps. Durable names, queue groups, and binding existing consumers are intentionally rejected. |
| Object Store | Not Planned | Not implemented. |
| Services/Micro | Not Planned | Not implemented. |
| Stream templates and admin APIs | Not planned | Legacy or administrative APIs are not prioritized for client v1. |

## KeyValue

| Feature | Status | Notes |
| :--- | :--- | :--- |
| Bucket lifecycle | Supported | Create, open, delete, status. |
| Bucket options | Supported | History, TTL, max bytes, max value size, storage, replicas, direct reads, compression, metadata, and delete-marker TTL. |
| Get/put/create/update | Supported | Typed entries, optimistic revision checks, per-key TTL, and typed conflict errors. |
| Delete/purge | Supported | Delete markers, purge markers, guarded delete/purge, and marker cleanup. |
| History and keys | Partial | Common paths implemented; broader large-bucket stress coverage remains. |
| Watch | Partial | Channel and callback watchers, filters, updates-only, history, ignore deletes, metadata-only, resume revision, bounded close cleanup, ordered-consumer recovery, and bounded reconnect chaos coverage. Broader large-watch stress remains. |
| Direct get | Supported | `direct=true` buckets use direct reads by default. |
| Task concurrency | Supported | KeyValue operations are direct blocking calls that compose with normal Julia tasks and cooperative cancellation where applicable. |

## Observability And Errors

| Feature | Status | Notes |
| :--- | :--- | :--- |
| Typed errors | Supported | Core, connection, auth, permission, slow consumer, JetStream, fetch, and KeyValue errors. |
| Lifecycle callbacks | Supported | `event_cb`, `error_cb`, and `reconnect_delay_cb`. |
| Stats | Partial | Opt-in message, byte, reconnect, error, and dropped-message counters via `record_stats=true`. |
| Structured logging facade | Planned | Use callbacks and normal Julia logging for now. |

## Test Modes

The normal package test suite is server-free:

```bash
julia --project=. -e 'using Pkg; Pkg.test()'
```

Real-server tests are opt-in:

```bash
env NATTER_RUN_INTEGRATION=true NATTER_RUN_JETSTREAM=true julia --project=. -e 'using Pkg; Pkg.test()'
```

Bounded live-server chaos tests run in CI and can be enabled locally:

```bash
env NATTER_RUN_INTEGRATION=true NATTER_RUN_JETSTREAM=true NATTER_RUN_CHAOS=true julia --project=. -e 'using Pkg; Pkg.test()'
```

Longer reconnect stress coverage is intended for scheduled/manual CI:

```bash
env NATTER_RUN_INTEGRATION=true NATTER_RUN_JETSTREAM=true NATTER_RUN_STRESS=true julia --project=. -e 'using Pkg; Pkg.test()'
```

Performance snapshots are reported separately from correctness tests:

```bash
just --justfile benchmarks/justfile with-server
```

The `Performance` GitHub Actions workflow runs the Natter report against a live server on a schedule and by manual dispatch. It uses production Julia flags, the standard server image, five trials, and median aggregation. It uploads Markdown and JSON artifacts covering hot-path allocations, direct publish throughput, buffered batch publish throughput, publish-plus-flush throughput, callback dispatch, request/reply latency, concurrent publish throughput, and reconnect recovery. Each benchmark runs warmup work before its timed region, so startup, package loading, JIT compilation, and benchmark warmup are outside the reported timings. The workflow is a reporting job rather than a strict performance gate; compare reports across matching publish semantics and similar runner, Julia, thread, server, and payload configurations.
