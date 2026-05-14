# Feature Coverage

This page summarizes the supported surface. The repository root `FEATURES_COVERAGE.md` remains the detailed tracking file for implementation notes and intentionally omitted edge cases.

## Core NATS

| Feature | Status | Notes |
| :--- | :--- | :--- |
| Connect handshake | Supported | Includes token, user/password, NKEY, JWT, and `.creds` authentication fields, client name, no echo, TLS requirements, and server INFO parsing. |
| Publish and subscribe | Supported | Includes wildcards, queue groups, INFO-negotiated headers, max payload checks, size- and latency-bound buffered outbound writes, and per-subscription pending limits. |
| Request reply | Supported | Uses a shared request inbox mux and maps negotiated no-responder status to `NoRespondersError`. |
| Direct APIs and task handles | Supported | Direct calls are task-friendly; `_async` helpers return `NatterTask` for explicit handle-oriented code, and `fetch(handle)` returns the sync result or throws the same operation error that the synchronous API would throw. |
| Flush and ping | Supported | Drains buffered outbound writes and uses PING/PONG synchronization. |
| Drain and close | Supported | Drains subscriptions and reports cleanup failures. |
| Automatic reconnect | Partial | Reconnects in the background, replays subscriptions, and flushes bounded pending publishes on a best-effort basis. Retained publishes can be delivered more than once after ambiguous transport failures; large direct-write frames are not retained after a successful socket write. Use JetStream `msg_id` deduplication or idempotent handlers for duplicate-sensitive effects. Real-server coverage includes same-server reconnect and multi-URL failover; multi-node cluster chaos and auth failover need more coverage before treating this as fully hardened cluster behavior. |
| Discovered servers | Supported | Server-discovered URLs are retained for reconnects, and stale discovered routes are pruned on subsequent INFO updates. |
| Server errors | Supported | Permission violations are reported without reconnecting; repeated auth failures abort reconnect with auth-specific errors. |
| Slow consumer handling | Supported | Drops over-limit messages and reports `SlowConsumerError`. |
| WebSocket transport | Not implemented | Planned only if there is clear demand. |

## TLS And Security

| Feature | Status | Notes |
| :--- | :--- | :--- |
| TLS-first handshake | Supported | `tls://` defaults to TLS before INFO. |
| INFO-first TLS upgrade | Supported | Use `tls_first=false` for deployments that require it. |
| CA and client certificates | Supported | `tls_ca_path`, `tls_cert_path`, and `tls_key_path`. |
| Disable certificate verification | Supported | `tls_verify=false`; intended only for trusted environments. |
| nkeys and JWT credentials | Supported | Supports public NKEY plus signing callback, seed-backed NKEY auth, user JWT plus seed/callback, and `.creds` content or file parsing. |

## JetStream

| Feature | Status | Notes |
| :--- | :--- | :--- |
| Typed stream configs | Supported | Mirrors known stream schema fields with Julia structs and EnumX enums, plus local subject/name/numeric validation. |
| Typed consumer configs | Supported | Mirrors known consumer schema fields with Julia structs and EnumX enums, plus local subject/name/queue/numeric validation. |
| Raw config dictionaries | Supported | Escape hatch for newer server fields. |
| Stream CRUD and list APIs | Supported | Includes pagination. |
| Consumer CRUD and list APIs | Supported | Includes server-version-aware create subjects, strict create/update actions, and explicit create-or-update upsert. |
| Publish acknowledgements | Supported | `js_publish` returns `PubAck` and exposes first-class headers for msg-id, expected stream/sequence/msg-id/subject, per-message TTL, schedules, and no-responders retry. |
| Task handle helpers | Supported | Management, publish, message get/delete, consumer, fetch, close, and ack operations have `_async` helpers. |
| Pull consumers | Partial | Durable and named consumers bind without mutation and recover from concurrent matching strict-create conflicts; missing consumers and random ephemerals are strictly created. Batch fetch, per-request reply correlation, expiration handling, idle heartbeat monitoring, reconnect-disconnect reporting, and JetStream status/control error mapping are covered. Fetches on one pull subscription are serialized, and fetch requests are not replayed after reconnect; callers retry after `FetchDisconnectedError`. |
| Push consumers | Supported | Durable and named consumers bind without mutation and recover from concurrent matching strict-create conflicts by rebinding to the existing deliver subject; active non-queue push consumers reject a second bind, and existing queue consumers require an explicit matching queue. Queue groups, callbacks, and manual or automatic acknowledgement are supported. Non-queue push consumers also support idle heartbeat filtering, missed-heartbeat reporting, flow-control replies, and lifecycle status reporting. |
| Message acknowledgements | Supported | `ack`, `ack_sync`, `nak`, `in_progress`, and `term`. |
| Message get | Supported | Sequence, last by subject, next by subject, and direct get. |
| Ordered consumers | Not implemented | Can be added later if needed. |
| Object store | Not implemented | Outside the current high-value surface. |

## KeyValue

| Feature | Status | Notes |
| :--- | :--- | :--- |
| Bucket create/open/delete/status | Supported | Backed by JetStream streams; `kv_create` exposes history, bucket TTL, max bucket bytes, max value size, storage, replicas, direct reads, compression, metadata, and delete-marker TTL. History is locally limited to 1 through 64. |
| Put/create/update | Supported | Includes optimistic revision checks, create after delete/purge markers, per-key TTL headers, and KV-specific conflict errors. |
| Get by latest or revision | Supported | Returns typed entries with key, value, revision, created timestamp, delta, and operation. Direct reads are used when bucket direct access is enabled; missing and deleted keys raise KV-specific errors. |
| Delete and purge | Supported | Uses KeyValue operation headers, optional expected-revision checks, purge marker TTL, and delete-marker cleanup. |
| History and keys | Supported | Implemented through pull consumers; history returns typed entries. |
| Watch | Supported | Implemented through push consumers with callback or `KeyValueWatcher` channel APIs. Supports multiple filters, updates-only, history, ignore-deletes, metadata-only delivery, resume revision, and an initial-done sentinel. |
| Task handle helpers | Supported | Bucket, status, key, history, keys, and watch operations have `_async` helpers. |
