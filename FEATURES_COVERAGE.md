# Natter.jl Feature Coverage

Statuses:

- `Supported`: implemented for the normal production path and covered by unit or integration tests where practical.
- `Partial`: implemented for common paths, with documented gaps.
- `Planned`: intended for future work.
- `Not planned`: intentionally excluded.
- `Ignored as esoteric`: uncommon administrative surface that is documented but not prioritized.

The normal test suite is server-free. Setting `NATTER_RUN_INTEGRATION=true`
executes real `nats-server` tests; setting `NATTER_RUN_JETSTREAM=true` also
executes JetStream and KeyValue tests.

## Core NATS

| Feature | Status | Notes |
|---|---:|---|
| TCP transport | Supported | Plain `nats://` connections. |
| TLS transport | Partial | CA/client cert/key options are available; broad platform verification still needs integration hardening. |
| WebSocket transport | Planned | `ws://` and `wss://` are not implemented yet. |
| INFO / CONNECT / PING / PONG | Supported | Includes async INFO updates and ping-based flush. |
| PUB / HPUB / SUB / UNSUB | Supported | Headers and queue groups included. |
| Wildcard subscriptions | Supported | `*` and terminal `>` subscriptions are validated. Publish subjects reject wildcards. |
| MSG / HMSG parser | Supported | Streaming line/payload parser written from scratch. |
| Request/reply | Supported | Uses per-request inboxes; multiplexed request inbox optimization is planned. |
| No responders | Supported | Status `503` replies become `NoRespondersError`. |
| Flush | Supported | Implemented as a ping/pong round trip. |
| Drain | Supported | Subscription and client drain are implemented with timeout and covered by real-server tests. |
| Close | Supported | Closes tasks and transport without further reconnects. |
| Reconnect | Partial | Automatic reconnect is enabled by default with server pool, discovered URLs, backoff, subscription replay, pending buffer restore-on-failure, and generation-bound background tasks. Core publishes are replayed after reconnect; duplicate delivery is possible if a transport fails after partial server acceptance. Cluster chaos coverage needs expansion. |
| Lame Duck Mode | Supported | Async INFO with `ldm=true` triggers reconnect. |
| Max payload enforcement | Supported | Uses server `max_payload` from INFO. |
| Slow consumer handling | Supported | Per-subscription pending message/byte limits raise `SlowConsumerError` through `error_cb`. |
| Typed errors | Supported | Common connection, protocol, auth, buffer, timeout, slow consumer, and JetStream errors. |
| Optimized request inbox multiplexing | Planned | Current requests use one inbox subscription per request. |

## Auth And Security

| Feature | Status | Notes |
|---|---:|---|
| Token auth | Supported | URL or option. |
| Username/password auth | Supported | URL or options. |
| NKEY/JWT credentials | Planned | Important for secure deployments, but not in the initial implementation. |
| `.creds` file parsing | Planned | Depends on NKEY/JWT signing support. |
| TLS first handshake | Supported | `tls://` uses TLS before INFO by default. `tls_first=true` enables it for other URLs and `tls_first=false` preserves INFO-first TLS upgrade behavior when needed. |
| TLS certificate verification control | Supported | Verification is enabled by default. Use `tls_verify=false` to intentionally disable peer certificate verification. |
| WebSocket custom headers | Planned | Depends on WebSocket transport. |

## JetStream

| Feature | Status | Notes |
|---|---:|---|
| JetStream context | Supported | `jetstream(client)` creates a context. |
| Publish with ack | Supported | Parses `PubAck` and API errors. |
| Async publish | Partial | Returns Julia `Task`; pending-window management is basic. |
| Typed stream configs | Supported | `StreamConfig` covers current stream schema fields with typed nested structs, enums, duration conversion, and raw `Dict` escape hatch. |
| Stream create/update/info/list/names/purge/delete | Supported | Common management APIs, including list pagination. Stream info returns typed config plus raw response. |
| Message get/delete | Supported | Common API path. |
| Typed consumer configs | Supported | `ConsumerConfig` covers current consumer schema fields with typed enums, duration conversion, local validation, and raw `Dict` escape hatch. |
| Consumer create/update/info/list/delete | Supported | Common management APIs, including newer named consumer subjects and legacy durable subjects. Consumer info returns typed config plus raw response. |
| Pull subscribe/fetch | Partial | Common batch fetch works; fetches on one pull subscription are serialized and ephemeral consumers are deleted on close. |
| Push subscribe | Partial | Common durable/ephemeral delivery works; returned subscriptions are closable and ephemeral consumers are deleted on close. Ordered reset/flow-control coverage is still limited. |
| Manual ack APIs | Supported | `ack`, `ack_sync`, `nak`, `in_progress`, and `term`. |
| JetStream message metadata | Supported | V1 and domain-aware V2 ack metadata parsing. |
| Ordered consumers | Partial | Common setup exists; automatic recovery needs more chaos testing. |
| Flow control / heartbeats | Partial | Control messages are recognized in common paths; full push-consumer coverage is planned. |
| Object Store | Planned | Not implemented in the first milestone. |
| Direct get | Supported | `stream_message_get(...; direct=true)` supports sequence, last-by-subject, and next-by-subject direct reads. KeyValue stores remember `allow_direct` and use direct reads automatically. |
| Stream templates | Ignored as esoteric | Legacy/rare API surface. |
| Leader stepdown / peer removal | Ignored as esoteric | Administrative APIs are not prioritized for client v1. |

## KeyValue

| Feature | Status | Notes |
|---|---:|---|
| Create/open/delete bucket | Supported | Built on stream APIs and covered by real-server tests. |
| Get/put/create/update/delete/purge | Supported | Revision checks use expected-last-subject-sequence headers; common paths covered by real-server tests. |
| Keys/history | Partial | Common paths implemented with temporary consumer cleanup and looped fetches. |
| Watch/watchall | Partial | Callback watcher implemented with a closable push subscription; default watch delivers last value per subject plus updates. |
| Direct get | Supported | `kv_create(...; direct=true)` enables direct reads, `kv_open` detects existing direct buckets, and `kv_get` uses direct access by default when available. |

## Services / Micro

| Feature | Status | Notes |
|---|---:|---|
| Service framework | Planned | Not implemented in the first milestone. |
| Endpoint discovery/stats | Planned | Depends on service framework. |

## Observability

| Feature | Status | Notes |
|---|---:|---|
| Client stats | Partial | Basic message/byte/reconnect counters. |
| Status callbacks | Supported | Connected, disconnected, reconnected, closed, and error callbacks. |
| Structured logging | Planned | Current implementation uses callbacks and exceptions rather than a logging facade. |

## Explicitly Missing From Initial Coverage

- WebSocket transport and WebSocket custom headers.
- NKEY, JWT, and `.creds` authentication.
- Object Store.
- Services/micro framework.
- Full ordered-consumer automatic reset behavior.
- Full JetStream flow-control and heartbeat coverage for push consumers.
- Publish-async pending windows and error aggregation comparable to mature clients.
- Cluster chaos tests covering server pool rotation, discovered routes, and failover under load.
