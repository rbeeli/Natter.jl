# Reference

This page summarizes the public API. Optional keyword defaults are documented in the guide pages when behavior matters. Timeout keyword values are positive finite seconds.

## Core Types

| Type | Purpose |
| :--- | :--- |
| `Client` | Active client connection with background reader, ping, and reconnect tasks. |
| `ConnectOptions` | Immutable keyword-backed connection configuration, including typed authentication, TLS verification and server-name control, reconnect, buffering, write timeout, parser, subscription, and close callback limits. |
| `ConnectionStatus` | EnumX status namespace: `DISCONNECTED`, `CONNECTING`, `CONNECTED`, `RECONNECTING`, `DRAINING`, `CLOSED`. |
| `ConnectionEventKind` | EnumX event namespace: `CONNECTED`, `DISCONNECTED`, `RECONNECT_ATTEMPT`, `RECONNECT_DELAY`, `RECONNECTED`, `DISCOVERED_SERVERS`, `TERMINAL_DISCONNECT`, `CLOSED`. |
| `ConnectionEvent` | Lifecycle event passed to `event_cb`, with status, server, URL, attempt, delay, error, and generation fields. |
| `NoAuth`, `TokenAuth`, `UserPassAuth`, `NKeyAuth`, `JwtAuth`, `CredentialsAuth`, `CallbackAuth` | Typed connection authentication values passed as `auth=...` to `connect` or `ConnectOptions`. |
| `AuthRequest` | Context passed to `CallbackAuth`, including server, URL, nonce, INFO, attempt, and reconnect flag. |
| `NatterTask` | Explicit async operation handle. Use `fetch(handle)` when code intentionally starts an operation and joins it later. Failed handles throw the same operation error that the synchronous API would throw. |
| `Msg` | Core received message data with `subject`, `reply`, and byte-vector `data`; use `header(msg, key)` or `headers(msg)` for headers. |
| `Headers` | Case-insensitive dictionary of header names to `Vector{String}` values; publish and request APIs also accept dictionaries with string or vector values and pair iterators. Outbound header names must be valid NATS/HTTP token field names. Mixed-case duplicates are merged under one entry, with the first inserted field spelling used for iteration and serialization. |
| `PublishFrame` | Validated, immutable prepared core publish frame. Use `prepare_publish` when the same subject, reply, headers, and payload are published repeatedly on a hot path. |
| `Subscription` | Core subscription handle. |
| `Stats` | Snapshot of message, byte, reconnect, error, and drop counters. |

## Core Functions

| Function | Purpose |
| :--- | :--- |
| `connect(url_or_urls=nothing; kwargs...)` | Connect to one or more servers. |
| `publish(client, subject, data=nothing; reply=nothing, headers=nothing)` | Publish a core message; header publishes require server INFO header support. Reconnect replay is best-effort and can duplicate retained frames after ambiguous transport failures. |
| `prepare_publish(subject, data=nothing; reply=nothing, headers=nothing)` | Validate and serialize publish metadata once, returning a `PublishFrame` for repeated `publish(client, frame)` calls. |
| `publish(client, frame::PublishFrame)` | Publish a prepared frame without repeating subject, reply, payload, or header serialization work. Server capability and size checks still run against the active client. |
| `subscribe(client, subject; queue=nothing, callback=nothing, max_msgs=0, ...)` | Create a core subscription; positive `max_msgs` closes after that many total messages. Per-subscription pending limits must be positive. |
| `next(sub; timeout=1.0)` | Wait for the next message from a non-callback subscription. |
| `request(client, subject, data=nothing; timeout=1.0, headers=nothing)` | Send a request and wait for one response; request headers require server INFO header support. |
| `flush(client; timeout=10.0)` | Wait until the server has processed previous commands. |
| `ping(client; timeout=10.0)` | Alias for `flush`. |
| `unsubscribe(sub; max_msgs=0)` | Unsubscribe immediately or after `max_msgs` additional messages. `max_msgs` must be non-negative. |
| `drain(sub; timeout=...)` | Unsubscribe and wait for queued callback work within one deadline. |
| `drain(client; timeout=...)` | Drain subscriptions and flush within one shared deadline, then close the client. |
| `close(client; throw_errors=false, callback_timeout=nothing)` | Close transports, tasks, and subscriptions. Active callbacks are waited on without interruption for `callback_timeout` or `close_callback_timeout`. |
| `new_inbox(client; prefix=...)` | Generate a reply inbox subject. |
| `header(msg, key)` | Return the first header value by case-insensitive key lookup or `nothing`. |
| `headers(msg)` | Return a copy of all message headers. |
| `status(client)` | Return current `ConnectionStatus`. |
| `stats(client)` | Return a `Stats` snapshot. |
| `connected_url(client)` | Return the current server URL or `nothing`. |

## Core Task Handle Helpers

| Function | Purpose |
| :--- | :--- |
| `connect_async(url_or_urls=nothing; kwargs...)` | Start `connect` and return `NatterTask`. |
| `publish_async(client, subject, data=nothing; kwargs...)` | Start core `publish` and return `NatterTask`. |
| `publish_async(client, frame::PublishFrame)` | Start prepared-frame publish and return `NatterTask`. |
| `subscribe_async(client, subject; kwargs...)` | Start `subscribe` and return `NatterTask`; callback-first style is supported. |
| `next_async(sub; timeout=1.0)` | Start `next` and return `NatterTask`. |
| `request_async(client, subject, data=nothing; kwargs...)` | Start `request` and return `NatterTask`. |
| `flush_async(client; timeout=10.0)` | Start `flush` and return `NatterTask`. |
| `ping_async(client; timeout=10.0)` | Start `ping` and return `NatterTask`. |
| `unsubscribe_async(sub; max_msgs=0)` | Start `unsubscribe` and return `NatterTask`. |
| `drain_async(client_or_sub; timeout=...)` | Start `drain` and return `NatterTask`. |
| `close_async(client_or_sub_or_watcher; kwargs...)` | Start `close` and return `NatterTask`. |

## JetStream Types

| Type | Purpose |
| :--- | :--- |
| `JetStreamContext` | JetStream API context for a client. |
| `StreamConfig` | Typed stream configuration with local subject, name, and numeric validation. Raw `Dict` configs use the same seconds-based duration conversion for known stream fields. |
| `ConsumerConfig` | Typed consumer configuration with local subject, name, queue, and numeric validation. Raw `Dict` configs use the same seconds-based duration conversion for known consumer fields. |
| `StreamLostData` | Typed lost-message summary nested under stream state. |
| `StreamState` | Typed stream state counters, sequence bounds, subjects, deletion, and lost-data summary. |
| `StreamInfo` | Stream info response with typed config and typed state. |
| `ConsumerSequenceInfo` | Typed consumer/stream sequence cursor in consumer info responses. |
| `ConsumerInfo` | Consumer info response with typed config, counters, sequence cursors, and push-bound state. |
| `PubAck` | Publish acknowledgement with stream, sequence, duplicate, and domain. |
| `JetStreamMsg` | JetStream consumer message data plus acknowledgement state for `ack`, `nak`, `in_progress`, and `term`. |
| `PullSubscription` | Pull consumer handle. |
| `PushSubscription` | Push consumer handle. |

## JetStream Enums

| Enum Namespace | Values |
| :--- | :--- |
| `RetentionPolicy` | `LIMITS`, `INTEREST`, `WORK_QUEUE` |
| `StorageType` | `FILE`, `MEMORY` |
| `DiscardPolicy` | `OLD`, `NEW` |
| `StoreCompression` | `NONE`, `S2` |
| `PersistMode` | `DEFAULT`, `ASYNC` |
| `AckPolicy` | `NONE`, `ALL`, `EXPLICIT` |
| `DeliverPolicy` | `ALL`, `LAST`, `NEW`, `BY_START_SEQUENCE`, `BY_START_TIME`, `LAST_PER_SUBJECT` |
| `ReplayPolicy` | `INSTANT`, `ORIGINAL` |
| `PriorityPolicy` | `NONE`, `OVERFLOW`, `PINNED_CLIENT` |

## JetStream Functions

| Function | Purpose |
| :--- | :--- |
| `jetstream(client; prefix="$JS.API", timeout=5.0)` | Create a JetStream context. |
| `js_publish(js, subject, data=nothing; timeout=..., stream=nothing, headers=nothing, msg_id=nothing, expected_last_sequence=nothing, expected_last_subject_sequence=nothing, expected_last_subject=nothing, expected_last_msg_id=nothing, ttl=nothing, schedule=nothing, schedule_at=nothing, schedule_every=nothing, retry_attempts=0, retry_wait=0.25)` | Publish and wait for a `PubAck`; first-class options map to JetStream publish headers. Use `msg_id` with the stream duplicate window to suppress duplicate publishes caused by reconnect or retry ambiguity. |
| `js_publish_async(js, subject, data=nothing; kwargs...)` | Start `js_publish` and return `NatterTask`. |
| `publish_async(js, subject, data=nothing; kwargs...)` | Alias for `js_publish_async`. |
| `stream_create(js, config)` | Create a stream from `StreamConfig` or `Dict`. |
| `stream_update(js, config)` | Update a stream. |
| `stream_info(js, name)` | Fetch stream info. |
| `stream_list(js; offset=0)` | List streams, following pagination. |
| `stream_names(js; subject=nothing)` | List stream names, optionally filtered by subject. |
| `stream_purge(js, name; filter_subject=nothing, keep=nothing)` | Purge a stream or subject subset, optionally keeping recent messages. |
| `stream_delete(js, name)` | Delete a stream. |
| `stream_message_get(js, stream; seq=nothing, subject=nothing, direct=false, next_by_subject=false)` | Read a stored message. Sequence lookups require a positive sequence. |
| `stream_message_delete(js, stream, seq)` | Delete one stored message by positive sequence. |
| `consumer_create(js, stream, config)` | Strictly create a consumer from `ConsumerConfig` or `Dict`. |
| `consumer_create_or_update(js, stream, config)` | Explicitly create or update a consumer. |
| `consumer_update(js, stream, config)` | Strictly update an existing consumer. |
| `consumer_info(js, stream, consumer)` | Fetch consumer info. |
| `consumer_list(js, stream; offset=0)` | List consumers for a stream. |
| `consumer_delete(js, stream, consumer)` | Delete a consumer. |
| `pull_subscribe(js, subject; stream=nothing, durable=nothing, config=ConsumerConfig(), timeout=...)` | Create or bind a pull subscription without mutating existing consumers. Configs with `deliver_subject` or `deliver_group` are rejected because they describe push delivery. |
| `push_subscribe(js, subject; stream=nothing, durable=nothing, queue=nothing, callback=nothing, manual_ack=false, config=ConsumerConfig(), timeout=...)` | Create or bind a push subscription without mutating existing consumers. Existing queue consumers require an explicit matching `queue`. Callback subscriptions receive `JetStreamMsg` and auto-ack unless `manual_ack=true` or the consumer uses `AckPolicy.NONE`; channel-backed subscriptions use `next(psub)` for `JetStreamMsg` delivery. |
| `fetch(psub, batch=1; timeout=..., expires=<shorter than timeout>, heartbeat=nothing)` | Fetch a batch of `JetStreamMsg` values from a pull subscription; fetches on one subscription are serialized. Each request uses a unique reply subject and ignores stale terminal statuses from older requests. The default server expiration is shorter than the local timeout; explicit `expires` values must also be shorter than `timeout`. Pull requests are not replayed after reconnect; `FetchDisconnectedError` is thrown if the connection drops before any messages arrive, and callers should retry after reconnect. Long fetches request and monitor idle heartbeats by default; pass `heartbeat=0` to disable them. |
| `ack`, `ack_sync`, `nak`, `in_progress`, `term` | Acknowledge or control redelivery for `JetStreamMsg` values. Terminal acks are not queued during reconnect and remain retryable if the send fails. |
| `metadata(msg)` | Parse JetStream delivery metadata. |

## JetStream Task Handle Helpers

| Function | Purpose |
| :--- | :--- |
| `stream_create_async(js, config)` | Start `stream_create` and return `NatterTask`. |
| `stream_update_async(js, config)` | Start `stream_update` and return `NatterTask`. |
| `stream_info_async(js, name)` | Start `stream_info` and return `NatterTask`. |
| `stream_list_async(js; offset=0)` | Start `stream_list` and return `NatterTask`. |
| `stream_names_async(js; subject=nothing)` | Start `stream_names` and return `NatterTask`. |
| `stream_purge_async(js, name; filter_subject=nothing, keep=nothing)` | Start `stream_purge` and return `NatterTask`. |
| `stream_delete_async(js, name)` | Start `stream_delete` and return `NatterTask`. |
| `stream_message_get_async(js, stream; kwargs...)` | Start `stream_message_get` and return `NatterTask`. |
| `stream_message_delete_async(js, stream, seq)` | Start `stream_message_delete` and return `NatterTask`. |
| `consumer_create_async(js, stream, config)` | Start `consumer_create` and return `NatterTask`. |
| `consumer_create_or_update_async(js, stream, config)` | Start `consumer_create_or_update` and return `NatterTask`. |
| `consumer_update_async(js, stream, config)` | Start `consumer_update` and return `NatterTask`. |
| `consumer_info_async(js, stream, consumer)` | Start `consumer_info` and return `NatterTask`. |
| `consumer_list_async(js, stream; offset=0)` | Start `consumer_list` and return `NatterTask`. |
| `consumer_delete_async(js, stream, consumer)` | Start `consumer_delete` and return `NatterTask`. |
| `pull_subscribe_async(js, subject; kwargs...)` | Start `pull_subscribe` and return `NatterTask`. |
| `push_subscribe_async(js, subject; kwargs...)` | Start `push_subscribe` and return `NatterTask`. |
| `fetch_async(psub, batch=1; kwargs...)` | Start pull `fetch` and return `NatterTask`. |
| `ack_async`, `ack_sync_async`, `nak_async`, `in_progress_async`, `term_async` | Start `JetStreamMsg` acknowledgement operations and return `NatterTask`. |

## KeyValue Functions

`KeyValueEntry` contains `bucket`, `key`, `value`, `revision`, `created`, `delta`, and `operation`.
`operation` uses the `KeyValueOperation` enum namespace: `PUT`, `DELETE`, and `PURGE`.
`KeyValueStatus` contains `bucket`, `stream`, `values`, `history`, `ttl`, `bytes`, `storage`, `replicas`, `direct`, and the backing `stream_info`.
`KeyValueWatcher` wraps a push subscription. Channel-backed watchers yield `KeyValueEntry` values and the `KV_WATCH_INITIAL_DONE` sentinel.

| Function | Purpose |
| :--- | :--- |
| `kv_create(js, bucket; history=1, ttl=nothing, max_bytes=-1, max_value_size=-1, storage="file", replicas=1, direct=false, compression=nothing, metadata=nothing, limit_marker_ttl=nothing, timeout=...)` | Create a bucket. Durations are seconds; `history` is limited to 1 through 64. |
| `kv_open(js, bucket; timeout=...)` | Open an existing bucket. |
| `kv_delete_bucket(kv; timeout=...)` | Delete a bucket. |
| `kv_status(kv; timeout=...)` | Return bucket status and stream-backed configuration. |
| `kv_get(kv, key; revision=nothing, direct=nothing, timeout=...)` | Get the latest value or a revision as `KeyValueEntry`. |
| `kv_put(kv, key, value; revision=nothing, ttl=nothing, timeout=...)` | Put a value, optionally expecting a revision or setting a per-key TTL; returns the new revision. |
| `kv_create_key(kv, key, value; ttl=nothing, timeout=...)` | Put a value only if the key is absent or currently deleted; returns the new revision. |
| `kv_update(kv, key, value, revision; ttl=nothing, timeout=...)` | Put a value only if the key is at `revision`; returns the new revision. |
| `kv_delete(kv, key; revision=nothing, timeout=...)` | Mark a key deleted, optionally only if the key is at `revision`. |
| `kv_purge(kv, key; revision=nothing, ttl=nothing, timeout=...)` | Purge a key with rollup, optionally only if the key is at `revision`; `ttl` expires the purge marker. |
| `kv_purge_deletes(kv; older_than=1800.0, timeout=...)` | Remove delete and purge markers, keeping recent markers when `older_than` is positive. |
| `kv_history(kv, key; batch=256, timeout=...)` | Return historical `KeyValueEntry` values for a key. |
| `kv_keys(kv; timeout=...)` | Return active keys. |
| `kv_watch(kv; key=">", keys=nothing, history=false, updates_only=false, ignore_deletes=false, meta_only=false, resume_revision=nothing, timeout=...)` | Watch bucket updates with a `KeyValueWatcher` and sentinel channel. |
| `kv_watch(callback, kv; kwargs...)` | Watch bucket updates with a callback. |

## KeyValue Task Handle Helpers

| Function | Purpose |
| :--- | :--- |
| `kv_create_async(js, bucket; kwargs...)` | Start `kv_create` and return `NatterTask`. |
| `kv_open_async(js, bucket; kwargs...)` | Start `kv_open` and return `NatterTask`. |
| `kv_delete_bucket_async(kv; kwargs...)` | Start `kv_delete_bucket` and return `NatterTask`. |
| `kv_status_async(kv; kwargs...)` | Start `kv_status` and return `NatterTask`. |
| `kv_get_async(kv, key; kwargs...)` | Start `kv_get` and return `NatterTask`. |
| `kv_put_async(kv, key, value; kwargs...)` | Start `kv_put` and return `NatterTask`; `fetch` returns the new revision. |
| `kv_create_key_async(kv, key, value; kwargs...)` | Start `kv_create_key` and return `NatterTask`; `fetch` returns the new revision. |
| `kv_update_async(kv, key, value, revision; kwargs...)` | Start `kv_update` and return `NatterTask`; `fetch` returns the new revision. |
| `kv_delete_async(kv, key; kwargs...)` | Start `kv_delete` and return `NatterTask`. |
| `kv_purge_async(kv, key; kwargs...)` | Start `kv_purge` and return `NatterTask`. |
| `kv_purge_deletes_async(kv; kwargs...)` | Start `kv_purge_deletes` and return `NatterTask`. |
| `kv_history_async(kv, key; kwargs...)` | Start `kv_history` and return `NatterTask`. |
| `kv_keys_async(kv; kwargs...)` | Start `kv_keys` and return `NatterTask`. |
| `kv_watch_async(kv; kwargs...)`, `kv_watch_async(callback, kv; kwargs...)` | Start `kv_watch` and return `NatterTask`. |
| `close_async(watcher::KeyValueWatcher)` | Close a key-value watcher in a `NatterTask`. |

## Errors

Public error types derive from `NatterError`: `TimeoutError`, `NoRespondersError`, `ConnectionClosedError`, `ConnectionReconnectingError`, `ConnectionDrainingError`, `ProtocolError`, `AuthenticationError`, `AuthorizationError`, `AuthenticationExpiredError`, `AuthenticationRevokedError`, `AccountAuthenticationExpiredError`, `PermissionViolationError`, `NoServersError`, `MaxPayloadError`, `OutboundBufferLimitError`, `SlowConsumerError`, `JetStreamError`, `FetchDisconnectedError`, `KeyValueError`, `UnsupportedFeatureError`, and `CleanupError`.

KeyValue-specific errors derive from `KeyValueError`: `KeyValueKeyNotFoundError`, `KeyValueKeyDeletedError`, `KeyValueWrongRevisionError`, and `KeyValueKeyExistsError`.
