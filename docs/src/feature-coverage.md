# Feature Coverage

This page is the canonical feature coverage tracker for Natter.jl. It includes implementation notes, test coverage expectations, partial support, and intentionally omitted edge cases.

Statuses:

- `Supported`: implemented for the normal production path and covered by unit or integration tests where practical.
- `Partial`: implemented for common paths, with documented gaps.
- `Planned`: intended for future work.
- `Not planned`: intentionally excluded.
- `Ignored as esoteric`: uncommon administrative surface that is documented but not prioritized.

The normal test suite is server-free. Setting `NATTER_RUN_INTEGRATION=true` executes real `nats-server` tests; setting `NATTER_RUN_JETSTREAM=true` also executes JetStream and KeyValue tests.

## Core NATS

| Feature | Status | Notes |
| :--- | :--- | :--- |
| TCP transport | Supported | Plain `nats://` connections. |
| TLS transport | Partial | CA/client cert/key options are available; broad platform verification still needs integration hardening. |
| WebSocket transport | Planned | `ws://` and `wss://` are not implemented yet; planned only if there is clear demand. |
| Connect handshake | Supported | Includes typed token, user/password, NKEY, JWT, `.creds`, and callback authentication, client name, no echo, TLS requirements, and server INFO parsing. |
| INFO / CONNECT / PING / PONG | Supported | Includes async INFO updates and ping-based flush. |
| PUB / HPUB / SUB / UNSUB | Supported | Headers are negotiated from server INFO before advertising or emitting HPUB. Queue groups are included. Outbound writes use a size- and latency-bound buffered flusher, with explicit flush/ping forcing a transport flush. |
| Wildcard subscriptions | Supported | `*` and terminal `>` subscriptions are validated. Publish subjects reject wildcards. |
| MSG / HMSG parser | Supported | Streaming line/payload parser written from scratch. |
| Request/reply | Supported | Uses a shared request inbox mux with per-request waiters and maps negotiated no-responder status to `NoRespondersError`. |
| Direct public API and task handles | Supported | Direct calls are task-friendly; `_async` helpers return `NatterTask` for explicit handle-oriented code, and `fetch(handle)` returns the sync result or throws the same operation error that the synchronous API would throw. |
| No responders | Supported | Advertised only when server INFO reports header support; status `503` replies become `NoRespondersError`. |
| Flush | Supported | Drains buffered outbound writes and uses a ping/pong round trip for synchronization. |
| Drain | Supported | Subscription and client drain are implemented with one overall timeout deadline, report cleanup failures, and are covered by unit and real-server tests. |
| Close | Supported | Closes tasks and transport without further reconnects. |
| Reconnect | Partial | Automatic reconnect is enabled by default with server pool, discovered URLs with stale-route pruning, backoff, subscription replay, pending buffer restore-on-failure, generation-bound background tasks, typed connection events, and optional reconnect-delay override callbacks. Publish replay is best-effort and should be treated as at-least-once for retained frames, not exactly-once: duplicate delivery is possible if a transport fails after partial server acceptance, and large frames that bypass the write buffer are not retained after a successful direct socket write. Use JetStream `msg_id` deduplication or idempotent application handling when duplicate effects are unacceptable. Real-server tests cover same-server reconnect and multi-URL failover through a local proxy; multi-node cluster chaos and auth failover coverage still need expansion. |
| Discovered servers | Supported | Server-discovered URLs are retained for reconnects, and stale discovered routes are pruned on subsequent INFO updates. |
| Lame Duck Mode | Supported | Async INFO with `ldm=true` triggers reconnect. |
| Server errors | Supported | Permission violations are reported without reconnecting; repeated auth failures abort reconnect with auth-specific errors. |
| Max payload enforcement | Supported | Uses server `max_payload` from INFO. |
| Slow consumer handling | Supported | Per-subscription pending message/byte limits raise `SlowConsumerError` through `error_cb`. |
| Typed errors | Supported | Common connection, protocol, auth, permission, buffer, timeout, slow consumer, and JetStream errors. |
| Optimized request inbox multiplexing | Supported | Requests share one wildcard inbox subscription per client and clean waiters on timeout, reconnect, and close. |

## TLS And Security

| Feature | Status | Notes |
| :--- | :--- | :--- |
| Token auth | Supported | `TokenAuth` or URL userinfo when `auth=NoAuth()`. Option tokens are stored in redacted wipeable buffers, and token values are redacted from `ConnectOptions` display including URL userinfo. |
| Username/password auth | Supported | `UserPassAuth` or URL userinfo when `auth=NoAuth()`. Option passwords are stored in redacted wipeable buffers, and users/passwords are redacted from `ConnectOptions` display including URL userinfo. |
| NKEY auth | Supported | `NKeyAuth` implements CONNECT nonce signing for public user NKEY plus callback and user seed-backed NKEY auth. NKEY seeds are stored in redacted wipeable buffers, path-loaded seed input buffers are wiped after CONNECT fields are derived, non-user NKEY prefixes are rejected locally, and unit tests cover CONNECT fields; CI real-server coverage runs through `NATTER_NKEY_AUTH_URL` and `NATTER_NKEY_AUTH_SEED`. |
| JWT credentials | Supported | `JwtAuth` and `CredentialsAuth` implement CONNECT nonce signing for user JWT plus seed/callback and user credentials, including standard decorated `.creds` content. JWT and inline credentials are stored in redacted wipeable buffers, path-loaded auth input buffers are wiped after CONNECT fields are derived, and CONNECT serialization and `.creds` parsing are unit-tested; CI real-server coverage runs with generated NSC credentials through `NATTER_JWT_AUTH_URL` and `NATTER_JWT_AUTH_CREDENTIALS_PATH`. |
| Dynamic auth callback | Supported | `CallbackAuth` receives `AuthRequest` after server `INFO` on initial connect and reconnect and must return a concrete static auth value. Callback return validation and request context are unit-tested. |
| `.creds` file parsing | Supported | `CredentialsAuth` extracts the user JWT and NKEY seed from standard decorated credentials content without retaining the whole file as an immutable string. |
| TLS first handshake | Supported | `tls://` uses TLS before INFO by default. `tls_first=true` enables it for other URLs and `tls_first=false` preserves INFO-first TLS upgrade behavior when needed. |
| INFO-first TLS upgrade | Supported | Use `tls_first=false` for deployments that require it. |
| CA and client certificates | Supported | `tls_ca_path`, `tls_cert_path`, and `tls_key_path`. |
| TLS certificate verification control | Supported | Verification is enabled by default. IP-literal hosts match `iPAddress` subject alternative names, `tls_server_name` overrides SNI and certificate-name validation, and `tls_verify=false` intentionally disables peer certificate verification. |
| WebSocket custom headers | Planned | Depends on WebSocket transport. |

## JetStream

| Feature | Status | Notes |
| :--- | :--- | :--- |
| JetStream context | Supported | `jetstream(client)` creates a context. |
| Publish with ack | Supported | Parses `PubAck` and API errors. `js_publish` exposes first-class options for msg-id, expected stream/sequence/msg-id/subject, per-message TTL, schedules, and no-responders retry. |
| Task handle helpers | Supported | JetStream management, publish, message get/delete, consumers, fetch, close, and ack operations have `_async` helpers returning `NatterTask`. |
| Typed stream configs | Supported | `StreamConfig` covers current stream schema fields with typed nested structs, enums, duration conversion, local subject/name/numeric validation, and raw `Dict` escape hatch using the same duration units for known fields. |
| Raw config dictionaries | Supported | Escape hatch for newer server fields. |
| Stream create/update/info/list/names/purge/delete | Supported | Common management APIs, including list pagination. Stream info returns typed config plus typed stream state. |
| Message get/delete | Supported | Common API path, including sequence, last-by-subject, next-by-subject, and direct reads. |
| Typed consumer configs | Supported | `ConsumerConfig` covers current consumer schema fields with typed enums, duration conversion, local subject/name/queue/numeric validation, and raw `Dict` escape hatch using the same duration units for known fields. |
| Consumer create/update/info/list/delete | Supported | Common management APIs, including newer named consumer subjects and legacy durable subjects. `consumer_create` and `consumer_update` use strict server actions when supported; `consumer_create_or_update` is the explicit upsert API. Consumer info returns typed config, counters, sequence cursors, and push-bound state. |
| Pull subscribe/fetch/consume | Supported | Durable and named consumers bind without mutation, validate supplied config fields, and recover from a concurrent matching strict-create conflict by binding; random ephemerals are strictly created and deleted on close. Bounded batch fetch, `max_bytes`, `no_wait`, idle heartbeat monitoring, threshold-refilled `messages`, callback `consume`, priority scheduling request fields, per-request reply correlation, expiration handling, reconnect-disconnect reporting, and JetStream status/control error mapping are covered. Continuous stream refill tracks buffered/requested client messages and bytes instead of only outstanding server request counters, and caps new pulls by `channel_size`, `max_bytes`, and `stop_after`. Only one active fetch or stream may use a pull subscription at a time. Pull requests are not replayed after reconnect; callers retry bounded fetches after `FetchDisconnectedError`, while streams surface errors through `wait(stream)`/`fetch(stream)`. |
| Push subscribe | Partial | Common durable/ephemeral delivery works; durable and named consumers bind without mutation, validate supplied config fields, and recover from a concurrent matching strict-create conflict by rebinding to the existing deliver subject. Active non-queue push consumers reject a second bind; existing queue consumers require an explicit matching `queue`; queue groups can be supplied through `queue` or `deliver_group` for new consumers. Queue groups, callbacks, and manual or automatic acknowledgement are supported. Push status/control messages are handled internally, flow control is answered only for flow-control requests, consumer deletion/leadership statuses are reported through `error_cb`, and returned subscriptions are closable. Random ephemeral consumers are deleted on close. |
| Manual ack APIs | Supported | `ack`, `ack_sync`, `nak`, `in_progress`, and `term`. |
| JetStream message metadata | Supported | V1 and domain-aware V2 ack metadata parsing. |
| Ordered consumers | Partial | Public ordered-consumer helpers are still absent. KeyValue watch uses internal ordered ephemeral push consumers with flow control, heartbeat monitoring, sequence-gap detection, and automatic reset from the next stream sequence. |
| Flow control / heartbeats | Supported | Pull fetch consumes status/control messages and monitors requested idle heartbeats. Non-queue push consumers hide idle heartbeats, report missed heartbeats through `error_cb`, and answer flow-control requests internally. |
| Object Store | Planned | Not implemented in the first milestone. |
| Direct get | Supported | `stream_message_get(...; direct=true)` supports sequence, last-by-subject, and next-by-subject direct reads. KeyValue stores remember `allow_direct` and use direct reads automatically. |
| Stream templates | Ignored as esoteric | Legacy/rare API surface. |
| Leader stepdown / peer removal | Ignored as esoteric | Administrative APIs are not prioritized for client v1. |

## KeyValue

| Feature | Status | Notes |
| :--- | :--- | :--- |
| Create/open/delete/status bucket | Supported | Built on stream APIs and covered by real-server tests. `kv_create` exposes history, bucket TTL, max bucket bytes, max value size, storage, replicas, direct reads, compression, metadata, and delete-marker TTL; history is locally limited to 1 through 64. `kv_status` exposes values, history, TTL, bytes, storage, replicas, direct-read support, and backing stream info. Bucket operations accept per-call timeouts. |
| Get/put/create/update/delete/purge | Supported | `kv_get` returns typed entries with key, value, revision, created timestamp, delta, and operation, including server TTL marker reasons. Put/create/update return the new KV revision. Revision checks use expected-last-subject-sequence headers, including create after delete/purge markers and guarded delete/purge operations. Put/create/update support per-key TTL headers; purge supports purge-marker TTL and marker cleanup. Common paths are covered by real-server tests. Missing/deleted keys and optimistic-write conflicts raise KV-specific errors. Key operations accept per-call timeouts. |
| Keys/history | Partial | Common paths implemented with temporary consumer cleanup and looped fetches; history returns typed entries including delete and purge operations. Keys and history operations use one per-call timeout budget across consumer setup and fetch pagination. |
| Watch/watchall | Partial | Callback and `KeyValueWatcher` channel APIs deliver typed entries through closable ordered push consumers. Default watch delivers last value per subject plus updates and sends an initial-done sentinel on channel watchers. Watch supports multiple filters, updates-only, history, ignore-deletes, metadata-only delivery, resume revision, and per-call setup timeout. Watch consumers use flow control, heartbeat monitoring, sequence-gap detection, and automatic reset from the next stream sequence. Broad chaos-tested recovery is still pending. |
| Direct get | Supported | `kv_create(...; direct=true)` enables direct reads, `kv_open` detects existing direct buckets, and `kv_get` uses direct access by default when available. |
| Task handle helpers | Supported | Bucket, status, key, history, keys, watch, and watcher close operations have `_async` helpers returning `NatterTask`. |

## Services / Micro

| Feature | Status | Notes |
| :--- | :--- | :--- |
| Service framework | Planned | Not implemented in the first milestone. |
| Endpoint discovery/stats | Planned | Depends on service framework. |

## Observability

| Feature | Status | Notes |
| :--- | :--- | :--- |
| Client stats | Partial | Basic message/byte/reconnect counters. |
| Status callbacks | Supported | Connected, disconnected, reconnected, closed, and error callbacks. |
| Structured logging | Planned | Current implementation uses callbacks and exceptions rather than a logging facade. |

## Explicitly Missing From Initial Coverage

- WebSocket transport and WebSocket custom headers.
- Object Store.
- Services/micro framework.
- Public ordered-consumer helpers outside KeyValue watch.
- Dedicated JetStream publish batching and pending-window APIs beyond `NatterTask` concurrency and reconnect buffering.
- Cluster chaos tests covering discovered route churn, auth failover, and failover under load.
