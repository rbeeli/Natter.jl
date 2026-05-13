# Feature Coverage

This page summarizes the supported surface. The repository root `FEATURES_COVERAGE.md` remains the detailed tracking file for implementation notes and intentionally omitted edge cases.

## Core NATS

| Feature | Status | Notes |
| :--- | :--- | :--- |
| Connect handshake | Supported | Includes authentication fields, client name, no echo, TLS requirements, and server INFO parsing. |
| Publish and subscribe | Supported | Includes wildcards, queue groups, INFO-negotiated headers, max payload checks, buffered outbound writes, and per-subscription pending limits. |
| Request reply | Supported | Uses a shared request inbox mux and maps negotiated no-responder status to `NoRespondersError`. |
| Direct APIs and task handles | Supported | Direct calls are task-friendly; `_async` helpers return `NatterTask` for explicit handle-oriented code. |
| Flush and ping | Supported | Drains buffered outbound writes and uses PING/PONG synchronization. |
| Drain and close | Supported | Drains subscriptions and reports cleanup failures. |
| Automatic reconnect | Partial | Reconnects in the background, replays subscriptions, and flushes bounded pending publishes. Real-server coverage includes same-server reconnect and multi-URL failover; multi-node cluster chaos, discovered-route churn, and auth failover need more coverage before treating this as fully hardened cluster behavior. |
| Discovered servers | Partial | Server-discovered URLs are retained for reconnects; discovered-route churn under cluster changes still needs real-server hardening. |
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
| nkeys and JWT credentials | Not implemented | Authentication currently covers token and user/password credentials. |

## JetStream

| Feature | Status | Notes |
| :--- | :--- | :--- |
| Typed stream configs | Supported | Mirrors known stream schema fields with Julia structs and EnumX enums. |
| Typed consumer configs | Supported | Mirrors known consumer schema fields with Julia structs and EnumX enums. |
| Raw config dictionaries | Supported | Escape hatch for newer server fields. |
| Stream CRUD and list APIs | Supported | Includes pagination. |
| Consumer CRUD and list APIs | Supported | Includes server-version-aware create subjects, strict create/update actions, and explicit create-or-update upsert. |
| Publish acknowledgements | Supported | `js_publish` returns `PubAck`. |
| Task handle helpers | Supported | Management, publish, message get/delete, consumer, fetch, close, and ack operations have `_async` helpers. |
| Pull consumers | Supported | Durable and named consumers bind without mutation and recover from concurrent matching strict-create conflicts; missing consumers and random ephemerals are strictly created. Batch fetch, per-request reply correlation, expiration handling, idle heartbeat monitoring, reconnect-disconnect reporting, and JetStream status/control error mapping are covered. |
| Push consumers | Supported | Durable and named consumers bind without mutation and recover from concurrent matching strict-create conflicts by rebinding to the existing deliver subject; active non-queue push consumers reject a second bind, and existing queue consumers require an explicit matching queue. Queue groups, callbacks, and manual or automatic acknowledgement are supported. Non-queue push consumers also support idle heartbeat filtering, missed-heartbeat reporting, flow-control replies, and lifecycle status reporting. |
| Message acknowledgements | Supported | `ack`, `ack_sync`, `nak`, `in_progress`, and `term`. |
| Message get | Supported | Sequence, last by subject, next by subject, and direct get. |
| Ordered consumers | Not implemented | Can be added later if needed. |
| Object store | Not implemented | Outside the current high-value surface. |

## KeyValue

| Feature | Status | Notes |
| :--- | :--- | :--- |
| Bucket create/open/delete/status | Supported | Backed by JetStream streams; `kv_status` exposes values, history, TTL, storage, replicas, direct-read support, and the backing stream info. |
| Put/create/update | Supported | Includes optimistic revision checks, create after delete/purge markers, and KV-specific conflict errors. |
| Get by latest or revision | Supported | Returns typed entries with key, value, revision, created timestamp, delta, and operation. Direct reads are used when bucket direct access is enabled; missing and deleted keys raise KV-specific errors. |
| Delete and purge | Supported | Uses KeyValue operation headers. |
| History and keys | Supported | Implemented through pull consumers; history returns typed entries. |
| Watch | Supported | Implemented through push consumers and typed entry callbacks. |
| Task handle helpers | Supported | Bucket, status, key, history, keys, and watch operations have `_async` helpers. |
