# Feature Coverage

This page summarizes the supported surface. The repository root `FEATURES_COVERAGE.md` remains the detailed tracking file for implementation notes and intentionally omitted edge cases.

## Core NATS

| Feature | Status | Notes |
| --- | --- | --- |
| Connect handshake | Supported | Includes authentication fields, client name, no echo, TLS requirements, and server INFO parsing. |
| Publish and subscribe | Supported | Includes wildcards, queue groups, headers, max payload checks, and per-subscription pending limits. |
| Request reply | Supported | Uses temporary inbox subscriptions and maps no-responder status to `NoRespondersError`. |
| Flush and ping | Supported | Uses PING/PONG synchronization. |
| Drain and close | Supported | Drains subscriptions and reports cleanup failures. |
| Automatic reconnect | Supported | Reconnects in the background, replays subscriptions, and flushes bounded pending publishes. |
| Discovered servers | Supported | Server-discovered URLs are retained for reconnects. |
| Slow consumer handling | Supported | Drops over-limit messages and reports `SlowConsumerError`. |
| WebSocket transport | Not implemented | Planned only if there is clear demand. |

## TLS And Security

| Feature | Status | Notes |
| --- | --- | --- |
| TLS-first handshake | Supported | `tls://` defaults to TLS before INFO. |
| INFO-first TLS upgrade | Supported | Use `tls_first=false` for deployments that require it. |
| CA and client certificates | Supported | `tls_ca_path`, `tls_cert_path`, and `tls_key_path`. |
| Disable certificate verification | Supported | `tls_verify=false`; intended only for trusted environments. |
| nkeys and JWT credentials | Not implemented | Authentication currently covers token and user/password credentials. |

## JetStream

| Feature | Status | Notes |
| --- | --- | --- |
| Typed stream configs | Supported | Mirrors known stream schema fields with Julia structs and EnumX enums. |
| Typed consumer configs | Supported | Mirrors known consumer schema fields with Julia structs and EnumX enums. |
| Raw config dictionaries | Supported | Escape hatch for newer server fields. |
| Stream CRUD and list APIs | Supported | Includes pagination. |
| Consumer CRUD and list APIs | Supported | Includes server-version-aware create subjects. |
| Publish acknowledgements | Supported | `js_publish` returns `PubAck`. |
| Pull consumers | Supported | Durable and ephemeral consumers, batch fetch, and expiration handling. |
| Push consumers | Supported | Queue groups, callbacks, and manual or automatic acknowledgement. |
| Message acknowledgements | Supported | `ack`, `ack_sync`, `nak`, `in_progress`, and `term`. |
| Message get | Supported | Sequence, last by subject, next by subject, and direct get. |
| Ordered consumers | Not implemented | Can be added later if needed. |
| Object store | Not implemented | Outside the current high-value surface. |

## KeyValue

| Feature | Status | Notes |
| --- | --- | --- |
| Bucket create/open/delete | Supported | Backed by JetStream streams. |
| Put/create/update | Supported | Includes optimistic revision checks. |
| Get by latest or revision | Supported | Direct reads are used when bucket direct access is enabled. |
| Delete and purge | Supported | Uses KeyValue operation headers. |
| History and keys | Supported | Implemented through pull consumers. |
| Watch | Supported | Implemented through push consumers. |
