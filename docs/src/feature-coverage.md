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
| PUB / HPUB / SUB / UNSUB | Supported | Core publish, headers, queue groups, unsubscribe limits, and local subject validation. |
| MSG / HMSG parsing | Supported | Core data and headers. |
| Wildcards | Supported | Subscription wildcards are validated; publish subjects reject wildcards. |
| Request/reply | Supported | Shared inbox mux, timeouts, and `NoRespondersError` when supported by the server. |
| Flush and ping | Supported | `flush`/`ping` use a server round trip. |
| Drain and close | Supported | Subscription/client drain and deterministic close paths. |
| Reconnect | Supported | Automatic reconnect, optional initial-connect retry, randomized server-pool attempts with ordered opt-out, discovered servers, subscription replay, bounded publish replay, lifecycle events, delay callbacks, and bounded live-server chaos CI are implemented. Broader cluster chaos and auth failover scenarios remain as additional hardening coverage. |
| Publish replay | Partial | Buffered core publishes are replayed best-effort and can duplicate after ambiguous network failures. Use JetStream `msg_id` for durable idempotent publish paths. |
| Slow consumer handling | Supported | Per-subscription pending limits report `SlowConsumerError`. |
| Runtime inspection | Supported | `status`, `stats`, and `connected_url`. |
| Async handles | Supported | Task-backed `_async` helpers return `NatterTask`; blocking calls accept cooperative cancellation tokens and async handles rethrow `CancelledError` from cancelled operations. JetStream publish has a protocol async publisher with `JetStreamPublishFuture`. |

## JetStream

| Feature | Status | Notes |
| :--- | :--- | :--- |
| Context | Supported | `jetstream(client)`. |
| Publish ack | Supported | `js_publish` returns `PubAck` and exposes common dedupe, optimistic constraint, TTL, schedule, and retry options, including two default `NoRespondersError` retries. `js_publish_async` uses protocol-level async publish with pending ack accounting, backpressure, completion waiting, per-message ack/error futures, configurable no-responder retries, and explicit pending-future clearing on reconnect rather than replay. |
| Stream management | Supported | Create, update, info, list, names, purge, delete. |
| Typed stream config | Supported | `StreamConfig` plus nested helpers and raw `Dict` escape hatch. Typed create/update checks that requested fields, including explicit false, zero, and empty values, are reflected by the server response. |
| Message lookup | Supported | Sequence, last-by-subject, next-by-subject, direct get, and message delete. |
| Consumer management | Supported | Create, create-or-update, update, info, list, delete. |
| Typed consumer config | Supported | `ConsumerConfig` plus raw `Dict` escape hatch. Typed create/update checks that requested fields, including explicit false, zero, and empty values, are reflected by the server response. |
| Pull consumers | Supported | Durable/named bind, ephemeral create/delete, bounded `fetch`, `max_bytes`, `no_wait`, heartbeats, `messages`, `consume`, and priority request fields. |
| Push consumers | Partial | Durable/ephemeral push, ordered ephemeral push, queue groups, callbacks, manual/auto ack, flow control replies, heartbeat reporting, and bounded reconnect chaos coverage. Additional long-duration push chaos remains. |
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
| Watch | Partial | Channel and callback watchers, filters, updates-only, history, ignore deletes, metadata-only, resume revision, ordered-consumer recovery, and bounded reconnect chaos coverage. Broader large-watch stress remains. |
| Direct get | Supported | `direct=true` buckets use direct reads by default. |
| Async handles | Supported | Bucket, key, history, keys, watch, and close helpers. |

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
