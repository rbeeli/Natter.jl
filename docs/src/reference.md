# Reference

This page summarizes the exported API. Guide pages show recommended usage; this page is for signatures, return types, and option names.

Conventions:

- Timeouts and durations are seconds.
- Status enums are namespaced, for example `ConnectionStatus.CONNECTED` and `AckPolicy.EXPLICIT`.
- Payload inputs may be strings, byte vectors, or `nothing`; encode structured values explicitly.
- `String(msg)` works for `Msg`, `BorrowedMsg`, `JetStreamMsg`, `BorrowedJetStreamMsg`, and `KeyValueEntry`.
- Blocking operations accept `cancel_token=cancellation_token(source)` where cancellation is useful.
- Most `_async` helpers return `NatterTask`; `js_publish_async` returns `JetStreamPublishFuture`. `fetch(handle)` returns the operation result or rethrows the original error.

## Core Types

| Type | Purpose |
| :--- | :--- |
| `Client` | Active connection with reader, ping, reconnect, request, and subscription state. |
| `ConnectOptions` | Immutable connection configuration built by `connect` or directly. |
| `ConnectionStatus` | `DISCONNECTED`, `CONNECTING`, `CONNECTED`, `RECONNECTING`, `DRAINING`, `CLOSED`. |
| `ConnectionEventKind` | Lifecycle event kind for `event_cb`. |
| `ConnectionEvent` | Event passed to `event_cb`; includes kind, status, URL, attempt, delay, error, and generation. |
| `Stats` | Snapshot of message, byte, reconnect, error, and dropped-message counters. |
| `Msg` | Owned core message with `subject`, `reply`, byte-vector `data`, and optional headers. |
| `BorrowedMsg` | Callback-only core message whose `data` is a borrowed byte view valid only for the callback call. |
| `Headers` | Case-insensitive header dictionary of `String => Vector{String}`. |
| `PublishFrame` | Prepared core publish frame from `prepare_publish`. |
| `Subscription` | Core subscription handle. |
| `NatterTask` | Explicit task handle returned by task-backed `_async` helpers. |
| `CancellationSource`, `CancellationToken` | Cooperative cancellation source and token for blocking operations. |

## Authentication Types

| Type | Use |
| :--- | :--- |
| `NoAuth()` | No NATS authentication. |
| `TokenAuth(token)` | Token auth. |
| `UserPassAuth(user, password)` | Username/password auth. |
| `NKeyAuth(; seed=...)`, `NKeyAuth(; seed_path=...)` | NKEY auth with local seed signing. |
| `NKeyAuth(; nkey=..., signature_cb=...)` | NKEY auth with external signing. |
| `JwtAuth(; jwt=..., seed=...)`, `JwtAuth(; jwt_path=..., seed_path=...)` | User JWT auth. |
| `CredentialsAuth(; path=...)`, `CredentialsAuth(credentials)` | Standard decorated `.creds` auth. |
| `CallbackAuth(f)` | Select auth after server `INFO`; `f(::AuthRequest)` returns a concrete auth value. |
| `AuthRequest` | Callback context with server, URL, nonce, server info, attempt, and reconnect flag. |

## Connect Options

`connect(url_or_urls=nothing; kwargs...)` accepts one URL, a vector/tuple of URLs, or no URL for `nats://localhost:4222`. Multi-server pools are randomized by default for initial connect and reconnect; set `randomize_servers=false` when ordered failover is required.

| Option | Default | Purpose |
| :--- | :--- | :--- |
| `randomize_servers` | `true` | Shuffle configured server attempts for initial connect and reconnect. |
| `name` | `nothing` | Human-readable connection name. |
| `auth` | `NoAuth()` | One typed authentication value. |
| `no_echo` | `false` | Do not receive this connection's own publishes. |
| `connect_timeout` | `2.0` | Socket and handshake timeout. |
| `ping_interval` | `120.0` | Keepalive interval. |
| `max_outstanding_pings` | `2` | Missed keepalives before reconnect. |
| `allow_reconnect` | `true` | Enable automatic reconnect. |
| `retry_on_initial_connect` | `false` | Retry startup connection failures using the reconnect wait and attempt settings. |
| `reconnect_wait` | `0.5` | Initial reconnect wait. |
| `reconnect_max_wait` | `5.0` | Maximum reconnect wait. |
| `reconnect_jitter` | `0.1` | Added reconnect jitter. |
| `max_reconnect_attempts` | `-1` | `-1` means unlimited reconnect and initial-retry attempts. |
| `pending_size` | `2 MiB` | Buffered publish bytes retained for reconnect replay; `0` disables replay buffering. |
| `read_buffer_size` | `64 KiB` | Inbound socket read buffer used by the protocol parser. |
| `read_buffer_shrink_threshold` | `256 KiB` | Parser buffer capacity above this size is released after oversized consumed frames. |
| `write_buffer_size` | `32 KiB` | Buffered write threshold; `0` disables buffering. |
| `direct_write_threshold` | `256 KiB` | Direct publishes at or below this size are written as one contiguous frame; larger direct publishes avoid the payload copy. |
| `write_buffer_latency` | `0.001` | Maximum small-write coalescing delay. |
| `write_timeout` | `10.0` | Maximum write/flush block time. |
| `record_stats` | `false` | Enable client counters returned by `stats(client)`. |
| `sub_pending_msgs_limit` | `1024` | Default per-subscription queued message limit. |
| `sub_pending_bytes_limit` | `128 MiB` | Default per-subscription queued byte limit. |
| `drain_timeout` | `30.0` | Default `drain` timeout. |
| `close_callback_timeout` | `5.0` | Wait for active callbacks during close. |
| `inbox_prefix` | `"_INBOX"` | Prefix for generated inbox subjects. |
| `error_cb` | warning callback | Receives background, callback, and cleanup errors. |
| `event_cb` | no-op | Receives `ConnectionEvent` lifecycle events. |
| `reconnect_delay_cb` | no-op | Optional reconnect delay override. |

TLS options:

| Option | Purpose |
| :--- | :--- |
| `tls_required` | Require TLS even for `nats://` URLs. |
| `tls_first` | Force TLS-first or INFO-first behavior. |
| `tls_verify` | Verify server certificates; default `true`. |
| `tls_server_name` | Override SNI and certificate name verification. |
| `tls_ca_path` | CA bundle/path. |
| `tls_cert_path`, `tls_key_path` | Client certificate and key for mTLS. |

Parser/resource limits:

| Option | Default |
| :--- | :--- |
| `max_control_line` | `16 KiB` |
| `max_inbound_payload` | `64 MiB` |
| `max_header_bytes` | `64 KiB` |
| `max_stale_pong_waiters` | `1024` |

## Core Functions

| Function | Returns | Use |
| :--- | :--- | :--- |
| `connect(url_or_urls=nothing; kwargs...)` | `Client` | Connect to NATS. |
| `publish(client, subject, data=nothing; reply=nothing, headers=nothing, buffer_on_reconnect=true, direct_write=false)` | `nothing` | Publish a core message. `direct_write=true` bypasses the client write buffer; `buffer_on_reconnect=false` avoids per-call replay snapshots. |
| `prepare_publish(subject, data=nothing; reply=nothing, headers=nothing)` | `PublishFrame` | Validate and serialize a reusable publish frame. Payloads are `nothing`, strings, or byte vectors. |
| `publish(client, frame::PublishFrame; buffer_on_reconnect=true, direct_write=false)` | `nothing` | Publish a prepared frame. |
| `subscribe(client, subject; queue=nothing, callback=nothing, borrowed=false, max_msgs=0, pending_msgs_limit=..., pending_bytes_limit=...)` | `Subscription` | Create a subscription. `borrowed=true` requires a callback and delivers `BorrowedMsg` inline from the reader task. |
| `subscribe(callback, client, subject; kwargs...)` | `Subscription` | Callback-first form. |
| `next(sub; timeout=1.0)` | `Msg` | Wait for a message on a non-callback subscription. |
| `unsubscribe(sub; max_msgs=0)` | `nothing` | Unsubscribe now or after more messages. |
| `close(sub)` | `nothing` | Alias for immediate unsubscribe. |
| `request(client, subject, data=nothing; timeout=1.0, headers=nothing)` | `Msg` | Send a request and wait for one reply. Payloads are `nothing`, strings, or byte vectors. |
| `flush(client; timeout=10.0)` | `nothing` | Wait for a server round trip. |
| `ping(client; timeout=10.0)` | `nothing` | Alias for `flush`. |
| `drain(sub; timeout=...)` | `nothing` | Unsubscribe and wait for queued callback work. |
| `drain(client; timeout=...)` | `nothing` | Drain subscriptions, flush, and close. |
| `close(client; throw_errors=false, callback_timeout=nothing)` | `nothing` | Close the client. |
| `new_inbox(client; prefix=...)` | `String` | Generate an inbox subject. |
| `header(msg, key)` | `Union{String,Nothing}` | First header value by case-insensitive key. |
| `headers(msg)` | `Headers` | Copy all message headers. |
| `status(client)` | `ConnectionStatus.T` | Current connection status. |
| `stats(client)` | `Stats` | Counter snapshot; byte counters include payload and header bytes. |
| `connected_url(client)` | `Union{String,Nothing}` | Current server URL. |

Core async helpers: `connect_async`, `publish_async`, `subscribe_async`, `unsubscribe_async`, `next_async`, `request_async`, `flush_async`, `ping_async`, `drain_async`, and `close_async`.

Cancellation helpers: `CancellationSource()`, `cancellation_token(source)`, `cancel!(source)`, and `iscancelled(token)`. Cancelled operations throw `CancelledError`; `_async` helpers rethrow the same error from `fetch(handle)`.

## JetStream Types

| Type | Purpose |
| :--- | :--- |
| `JetStreamContext` | JetStream API context from `jetstream(client)`. |
| `StreamConfig` | Typed stream configuration. |
| `ConsumerConfig` | Typed consumer configuration. |
| `StreamInfo`, `StreamState`, `StreamLostData` | Typed stream info responses. |
| `ConsumerInfo`, `ConsumerSequenceInfo` | Typed consumer info responses. |
| `PubAck` | Publish acknowledgement. |
| `StoredMsg` | Stored stream message with message fields plus `seq` and `created`. |
| `JetStreamPage` | One paged management response with `items`, `offset`, `total`, and `limit`. |
| `JetStreamPublishFuture` | Future returned by protocol-level async JetStream publish. |
| `AbstractJetStreamMsg` | Shared supertype for ackable JetStream consumer messages. |
| `JetStreamMsg` | Consumer message with acknowledgement state. |
| `BorrowedJetStreamMsg` | Push-callback JetStream message whose bytes are borrowed for the callback call. |
| `PullSubscription`, `PullMessageStream`, `PushSubscription` | Consumer handles. |

Enums:

| Namespace | Values |
| :--- | :--- |
| `RetentionPolicy` | `LIMITS`, `INTEREST`, `WORK_QUEUE` |
| `StorageType` | `FILE`, `MEMORY` |
| `DiscardPolicy` | `OLD`, `NEW` |
| `StoreCompression` | `NONE`, `S2` |
| `PersistMode` | `DEFAULT`, `ASYNC` |
| `AckPolicy` | `NONE`, `ALL`, `EXPLICIT` |
| `DeliverPolicy` | `ALL`, `LAST`, `NEW`, `BY_START_SEQUENCE`, `BY_START_TIME`, `LAST_PER_SUBJECT` |
| `ReplayPolicy` | `INSTANT`, `ORIGINAL` |
| `PriorityPolicy` | `NONE`, `OVERFLOW`, `PINNED_CLIENT`, `PRIORITIZED` |

Stream config helpers: `Placement`, `ExternalStreamSource`, `SubjectTransform`, `StreamSource`, `StreamConsumerLimits`, and `RePublish`.

## JetStream Functions

| Function | Returns | Use |
| :--- | :--- | :--- |
| `jetstream(client; prefix="$JS.API", timeout=5.0, publish_async_max_pending=256)` | `JetStreamContext` | Create a context. |
| `js_publish(js, subject, data=nothing; kwargs...)` | `PubAck` | Publish and wait for an ack. |
| `js_publish_async(js, subject, data=nothing; kwargs...)` | `JetStreamPublishFuture` | Publish with the context async publisher and return an ack future. Pending futures are failed, not replayed, on reconnect. |
| `fetch(future::JetStreamPublishFuture)` | `PubAck` | Wait for the async publish ack or rethrow its error. |
| `js_publish_async_pending(js)` | `Int` | Count async publishes still waiting for acks. |
| `js_publish_async_complete(js; timeout=...)` | `nothing` | Wait until all pending async publishes on the context complete. |
| `stream_create(js, config; timeout=...)` | `StreamInfo` | Create a stream. |
| `stream_update(js, config; timeout=...)` | `StreamInfo` | Update a stream. |
| `stream_info(js, name; timeout=...)` | `StreamInfo` | Fetch stream info. |
| `stream_list(js; offset=0, timeout=...)` | `Vector{StreamInfo}` | List streams. |
| `stream_list_page(js; offset=0, timeout=...)` | `JetStreamPage{StreamInfo}` | Fetch one stream list page. |
| `stream_list_pages(js; offset=0, timeout=...)`, `stream_list_iter(js; offset=0, timeout=...)` | iterator | Iterate stream pages or stream infos lazily. |
| `stream_names(js; subject=nothing, offset=0, timeout=...)` | `Vector{String}` | List stream names. |
| `stream_names_page(js; subject=nothing, offset=0, timeout=...)` | `JetStreamPage{String}` | Fetch one stream-name page. |
| `stream_names_pages(js; subject=nothing, offset=0, timeout=...)`, `stream_names_iter(js; subject=nothing, offset=0, timeout=...)` | iterator | Iterate stream-name pages or names lazily. |
| `stream_purge(js, name; filter_subject=nothing, keep=nothing, timeout=...)` | `Bool` | Purge a stream or subject subset. |
| `stream_delete(js, name; timeout=...)` | `Bool` | Delete a stream. |
| `stream_message_get(js, stream; seq=nothing, subject=nothing, direct=false, next_by_subject=false, timeout=...)` | `StoredMsg` | Read a stored message with `seq` and `created` metadata. |
| `stream_message_delete(js, stream, seq; timeout=...)` | `Bool` | Delete one stored message. |
| `consumer_create(js, stream, config; timeout=...)` | `ConsumerInfo` | Strict create. |
| `consumer_create_or_update(js, stream, config; timeout=...)` | `ConsumerInfo` | Explicit upsert. |
| `consumer_update(js, stream, config; timeout=...)` | `ConsumerInfo` | Strict update. |
| `consumer_info(js, stream, consumer; timeout=...)` | `ConsumerInfo` | Fetch consumer info. |
| `consumer_list(js, stream; offset=0, timeout=...)` | `Vector{ConsumerInfo}` | List consumers. |
| `consumer_list_page(js, stream; offset=0, timeout=...)` | `JetStreamPage{ConsumerInfo}` | Fetch one consumer list page. |
| `consumer_list_pages(js, stream; offset=0, timeout=...)`, `consumer_list_iter(js, stream; offset=0, timeout=...)` | iterator | Iterate consumer pages or consumer infos lazily. |
| `consumer_delete(js, stream, consumer; timeout=...)` | `Bool` | Delete a consumer. |
| `pull_subscribe(js, subject; stream=nothing, durable=nothing, config=ConsumerConfig(), timeout=...)` | `PullSubscription` | Create or bind a pull consumer. |
| `fetch(psub, batch=1; timeout=..., expires=..., heartbeat=nothing, max_bytes=nothing, no_wait=false, min_pending=nothing, min_ack_pending=nothing, priority_group=nothing, priority=nothing, cancel_token=nothing)` | `Vector{JetStreamMsg}` | Fetch a bounded batch. |
| `messages(psub; batch=100, max_bytes=nothing, expires=30.0, heartbeat=nothing, threshold_messages=nothing, threshold_bytes=nothing, channel_size=batch, stop_after=nothing, min_pending=nothing, min_ack_pending=nothing, priority_group=nothing, priority=nothing)` | `PullMessageStream` | Start a bounded refill stream. |
| `consume(callback, psub; kwargs...)` | `PullMessageStream` | Run a callback over `messages`. |
| `push_subscribe(js, subject; stream=nothing, durable=nothing, queue=nothing, callback=nothing, manual_ack=false, config=ConsumerConfig(), timeout=..., ordered=false, borrowed=false)` | `PushSubscription` | Create or bind a push consumer, or create an ordered ephemeral push consumer with `ordered=true`. `borrowed=true` is callback-only and delivers `BorrowedJetStreamMsg`. |
| `next(psub::PushSubscription; timeout=1.0)` | `JetStreamMsg` | Read from a channel-backed push subscription. |
| `ack(msg; cancel_token=nothing)`, `ack_sync(msg; timeout=1.0, cancel_token=nothing)`, `nak(msg; delay=nothing, cancel_token=nothing)`, `in_progress(msg; cancel_token=nothing)`, `term(msg; cancel_token=nothing)` | `nothing` or `Msg` | Acknowledge or control redelivery for `AbstractJetStreamMsg` values. |
| `metadata(msg)` | `MsgMetadata` | Parse JetStream delivery metadata. |
| `close(psub; timeout=...)`, `close(push; timeout=...)`, `close(stream)` | `nothing` | Close consumer/message handles. Subscription close timeouts bound server cleanup. |

Typed `StreamConfig`, typed `ConsumerConfig`, and raw dictionary create/update calls verify that requested fields are reflected in the server response, including explicit false, zero, and empty values. Raw dictionary configs remain available for fields outside Natter's typed API, but they are not a weaker verification escape hatch. For unknown raw fields, Natter requires requested nested values to be present and tolerates additional nested defaults returned by the server.

JetStream task-backed async helpers mirror management, subscribe, fetch, timeout-aware close, and acknowledgement functions, including acknowledgement kwargs and borrowed messages: `stream_*_async`, `consumer_*_async`, `pull_subscribe_async`, `push_subscribe_async`, `fetch_async`, `ack_async`, `ack_sync_async`, `nak_async`, `in_progress_async`, `term_async`, and `js_publish_async_complete_async`. `js_publish_async` is different: it is the protocol async publisher and returns `JetStreamPublishFuture`.

## KeyValue Types

| Type | Purpose |
| :--- | :--- |
| `KeyValue` | Bucket handle. |
| `KeyValueEntry` | Bucket entry with `bucket`, `key`, `value`, `revision`, `created`, `delta`, and `operation`. |
| `KeyValueOperation` | `PUT`, `DELETE`, `PURGE`. |
| `KeyValueStatus` | Bucket status and backing stream info. |
| `KeyValueWatcher` | Watch handle with `updates` channel and `take!(watcher)`. |
| `KV_WATCH_INITIAL_DONE` | Sentinel emitted by channel watchers after the initial snapshot. |

## KeyValue Functions

| Function | Returns | Use |
| :--- | :--- | :--- |
| `kv_create(js, bucket; history=1, ttl=nothing, max_bytes=-1, max_value_size=-1, storage="file", replicas=1, direct=false, compression=nothing, metadata=nothing, limit_marker_ttl=nothing, timeout=...)` | `KeyValue` | Create a bucket. |
| `kv_open(js, bucket; timeout=...)` | `KeyValue` | Open an existing bucket. |
| `kv_delete_bucket(kv; timeout=...)` | `Bool` | Delete the bucket. |
| `kv_status(kv; timeout=...)` | `KeyValueStatus` | Inspect bucket state. |
| `kv_get(kv, key; revision=nothing, direct=nothing, timeout=...)` | `KeyValueEntry` | Read a key. |
| `kv_put(kv, key, value; revision=nothing, ttl=nothing, timeout=...)` | `Int` | Put a value and return the new revision. |
| `kv_create_key(kv, key, value; ttl=nothing, timeout=...)` | `Int` | Put only if absent or deleted. |
| `kv_update(kv, key, value, revision; ttl=nothing, timeout=...)` | `Int` | Put only at the expected revision. |
| `kv_delete(kv, key; revision=nothing, timeout=...)` | `nothing` | Mark a key deleted. |
| `kv_purge(kv, key; revision=nothing, ttl=nothing, timeout=...)` | `nothing` | Purge prior values for a key. |
| `kv_purge_deletes(kv; older_than=1800.0, timeout=...)` | `nothing` | Remove old delete and purge markers. Use `older_than < 0` to remove all markers regardless of age. |
| `kv_history(kv, key; batch=256, timeout=...)` | `Vector{KeyValueEntry}` | Read key history. |
| `kv_keys(kv; timeout=...)` | `Vector{String}` | List active keys. |
| `kv_watch(kv; key=">", keys=nothing, history=false, updates_only=false, ignore_deletes=false, meta_only=false, resume_revision=nothing, channel_size=256, notify_initial_done=false, timeout=...)` | `KeyValueWatcher` | Watch with a channel. |
| `kv_watch(callback, kv; kwargs...)` | `KeyValueWatcher` | Watch with a callback. |
| `close(watcher; timeout=...)` | `nothing` | Close a watcher and bound server cleanup. |

KeyValue async helpers: `kv_create_async`, `kv_open_async`, `kv_delete_bucket_async`, `kv_status_async`, `kv_get_async`, `kv_put_async`, `kv_create_key_async`, `kv_update_async`, `kv_delete_async`, `kv_purge_async`, `kv_purge_deletes_async`, `kv_history_async`, `kv_keys_async`, `kv_watch_async`, and `close_async(watcher; timeout=...)`.

## Errors

All public error types derive from `NatterError`.

Core and connection errors: `TimeoutError`, `NoRespondersError`, `ConnectionClosedError`, `ConnectionReconnectingError`, `ConnectionDrainingError`, `ProtocolError`, `AuthenticationError`, `AuthorizationError`, `AuthenticationExpiredError`, `AuthenticationRevokedError`, `AccountAuthenticationExpiredError`, `PermissionViolationError`, `NoServersError`, `MaxPayloadError`, `OutboundBufferLimitError`, `SlowConsumerError`, `UnsupportedFeatureError`, and `CleanupError`.

JetStream errors: `JetStreamError`, `FetchDisconnectedError`, and `ConsumerSequenceMismatchError`.

KeyValue errors: `KeyValueError`, `KeyValueKeyNotFoundError`, `KeyValueKeyDeletedError`, `KeyValueWrongRevisionError`, and `KeyValueKeyExistsError`.
