# Reference

This page summarizes the public API. Optional keyword defaults are documented in the guide pages when behavior matters.

## Core Types

| Type | Purpose |
| :--- | :--- |
| `Client` | Active client connection with background reader, ping, and reconnect tasks. |
| `ConnectOptions` | Keyword-backed connection configuration. |
| `ConnectionStatus` | EnumX status namespace: `DISCONNECTED`, `CONNECTING`, `CONNECTED`, `RECONNECTING`, `DRAINING`, `CLOSED`. |
| `NatterTask` | Explicit async operation handle. Use `fetch(handle)` when code intentionally starts an operation and joins it later. |
| `Msg` | Received message with `subject`, `reply`, `data`, `headers`, and acknowledgement state. |
| `Headers` | Alias for received headers, `Dict{String,Vector{String}}`; publish and request APIs also accept dictionaries with string or vector values and pair iterators. Header keys preserve source casing, while `header(msg, key)` lookup is case-insensitive. |
| `Subscription` | Core subscription handle. |
| `Stats` | Snapshot of message, byte, reconnect, error, and drop counters. |

## Core Functions

| Function | Purpose |
| :--- | :--- |
| `connect(url_or_urls=nothing; kwargs...)` | Connect to one or more servers. |
| `publish(client, subject, data=nothing; reply=nothing, headers=nothing)` | Publish a core message; header publishes require server INFO header support. |
| `subscribe(client, subject; queue=nothing, callback=nothing, max_msgs=0, ...)` | Create a core subscription. |
| `next(sub; timeout=1.0)` | Wait for the next message from a subscription. |
| `request(client, subject, data=nothing; timeout=1.0, headers=nothing)` | Send a request and wait for one response; request headers require server INFO header support. |
| `flush(client; timeout=10.0)` | Wait until the server has processed previous commands. |
| `ping(client; timeout=10.0)` | Alias for `flush`. |
| `unsubscribe(sub; max_msgs=0)` | Unsubscribe immediately or after `max_msgs` additional messages. |
| `drain(sub; timeout=...)` | Unsubscribe and wait for queued callback work. |
| `drain(client; timeout=...)` | Drain subscriptions, flush, and close the client. |
| `close(client; throw_errors=false)` | Close transports, tasks, subscriptions, and callbacks. |
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
| `subscribe_async(client, subject; kwargs...)` | Start `subscribe` and return `NatterTask`; callback-first style is supported. |
| `next_async(sub; timeout=1.0)` | Start `next` and return `NatterTask`. |
| `request_async(client, subject, data=nothing; kwargs...)` | Start `request` and return `NatterTask`. |
| `flush_async(client; timeout=10.0)` | Start `flush` and return `NatterTask`. |
| `ping_async(client; timeout=10.0)` | Start `ping` and return `NatterTask`. |
| `unsubscribe_async(sub; max_msgs=0)` | Start `unsubscribe` and return `NatterTask`. |
| `drain_async(client_or_sub; timeout=...)` | Start `drain` and return `NatterTask`. |
| `close_async(client_or_sub; kwargs...)` | Start `close` and return `NatterTask`. |

## JetStream Types

| Type | Purpose |
| :--- | :--- |
| `JetStreamContext` | JetStream API context for a client. |
| `StreamConfig` | Typed stream configuration. Raw `Dict` configs use the same seconds-based duration conversion for known stream fields. |
| `ConsumerConfig` | Typed consumer configuration. Raw `Dict` configs use the same seconds-based duration conversion for known consumer fields. |
| `StreamInfo` | Stream info response with typed config, state, and raw data. |
| `ConsumerInfo` | Consumer info response with typed config, push-bound state, and raw data. |
| `PubAck` | Publish acknowledgement with stream, sequence, duplicate, and domain. |
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
| `js_publish(js, subject, data=nothing; timeout=..., stream=nothing, headers=nothing)` | Publish and wait for a `PubAck`. |
| `js_publish_async(js, subject, data=nothing; kwargs...)` | Start `js_publish` and return `NatterTask`. |
| `publish_async(js, subject, data=nothing; kwargs...)` | Alias for `js_publish_async`. |
| `stream_create(js, config)` | Create a stream from `StreamConfig` or `Dict`. |
| `stream_update(js, config)` | Update a stream. |
| `stream_info(js, name)` | Fetch stream info. |
| `stream_list(js; offset=0)` | List streams, following pagination. |
| `stream_names(js; subject=nothing)` | List stream names, optionally filtered by subject. |
| `stream_purge(js, name; filter_subject=nothing)` | Purge a stream or subject subset. |
| `stream_delete(js, name)` | Delete a stream. |
| `stream_message_get(js, stream; seq=nothing, subject=nothing, direct=false, next_by_subject=false)` | Read a stored message. |
| `stream_message_delete(js, stream, seq)` | Delete one stored message. |
| `consumer_create(js, stream, config)` | Strictly create a consumer from `ConsumerConfig` or `Dict`. |
| `consumer_create_or_update(js, stream, config)` | Explicitly create or update a consumer. |
| `consumer_update(js, stream, config)` | Strictly update an existing consumer. |
| `consumer_info(js, stream, consumer)` | Fetch consumer info. |
| `consumer_list(js, stream; offset=0)` | List consumers for a stream. |
| `consumer_delete(js, stream, consumer)` | Delete a consumer. |
| `pull_subscribe(js, subject; stream=nothing, durable=nothing, config=ConsumerConfig())` | Create or bind a pull subscription without mutating existing consumers. |
| `push_subscribe(js, subject; stream=nothing, durable=nothing, queue=nothing, callback=nothing, manual_ack=false, config=ConsumerConfig())` | Create or bind a push subscription without mutating existing consumers. Existing queue consumers require an explicit matching `queue`. Callback subscriptions auto-ack unless `manual_ack=true` or the consumer uses `AckPolicy.NONE`. |
| `fetch(psub, batch=1; timeout=..., expires=timeout, heartbeat=nothing)` | Fetch a batch from a pull subscription. Each request uses a unique reply subject and ignores stale terminal statuses from older requests. Pull requests are not replayed after reconnect; `FetchDisconnectedError` is thrown if the connection drops before any messages arrive. Long fetches request and monitor idle heartbeats by default; pass `heartbeat=0` to disable them. |
| `ack`, `ack_sync`, `nak`, `in_progress`, `term` | Acknowledge or control redelivery for JetStream messages. |
| `metadata(msg)` | Parse JetStream delivery metadata. |

## JetStream Task Handle Helpers

| Function | Purpose |
| :--- | :--- |
| `stream_create_async(js, config)` | Start `stream_create` and return `NatterTask`. |
| `stream_update_async(js, config)` | Start `stream_update` and return `NatterTask`. |
| `stream_info_async(js, name)` | Start `stream_info` and return `NatterTask`. |
| `stream_list_async(js; offset=0)` | Start `stream_list` and return `NatterTask`. |
| `stream_names_async(js; subject=nothing)` | Start `stream_names` and return `NatterTask`. |
| `stream_purge_async(js, name; filter_subject=nothing)` | Start `stream_purge` and return `NatterTask`. |
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
| `ack_async`, `ack_sync_async`, `nak_async`, `in_progress_async`, `term_async` | Start acknowledgement operations and return `NatterTask`. |

## KeyValue Functions

`KeyValueEntry` contains `bucket`, `key`, `value`, `revision`, `created`, `delta`, `operation`, and the underlying `msg`.
`operation` uses the `KeyValueOperation` enum namespace: `PUT`, `DELETE`, and `PURGE`.
`KeyValueStatus` contains `bucket`, `stream`, `values`, `history`, `ttl`, `bytes`, `storage`, `replicas`, `direct`, and the backing `stream_info`.

| Function | Purpose |
| :--- | :--- |
| `kv_create(js, bucket; history=1, storage="file", replicas=1, direct=false)` | Create a bucket. |
| `kv_open(js, bucket)` | Open an existing bucket. |
| `kv_delete_bucket(kv)` | Delete a bucket. |
| `kv_status(kv)` | Return bucket status and stream-backed configuration. |
| `kv_get(kv, key; revision=nothing, direct=nothing)` | Get the latest value or a revision as `KeyValueEntry`. |
| `kv_put(kv, key, value; revision=nothing)` | Put a value, optionally expecting a revision. |
| `kv_create_key(kv, key, value)` | Put a value only if the key is absent or currently deleted. |
| `kv_update(kv, key, value, revision)` | Put a value only if the key is at `revision`. |
| `kv_delete(kv, key)` | Mark a key deleted. |
| `kv_purge(kv, key)` | Purge a key with rollup. |
| `kv_history(kv, key; batch=256)` | Return historical `KeyValueEntry` values for a key. |
| `kv_keys(kv)` | Return active keys. |
| `kv_watch(callback, kv; key=">", history=false)` | Watch bucket updates as `KeyValueEntry` values with a push subscription. |

## KeyValue Task Handle Helpers

| Function | Purpose |
| :--- | :--- |
| `kv_create_async(js, bucket; kwargs...)` | Start `kv_create` and return `NatterTask`. |
| `kv_open_async(js, bucket)` | Start `kv_open` and return `NatterTask`. |
| `kv_delete_bucket_async(kv)` | Start `kv_delete_bucket` and return `NatterTask`. |
| `kv_status_async(kv)` | Start `kv_status` and return `NatterTask`. |
| `kv_get_async(kv, key; kwargs...)` | Start `kv_get` and return `NatterTask`. |
| `kv_put_async(kv, key, value; kwargs...)` | Start `kv_put` and return `NatterTask`. |
| `kv_create_key_async(kv, key, value)` | Start `kv_create_key` and return `NatterTask`. |
| `kv_update_async(kv, key, value, revision)` | Start `kv_update` and return `NatterTask`. |
| `kv_delete_async(kv, key)` | Start `kv_delete` and return `NatterTask`. |
| `kv_purge_async(kv, key)` | Start `kv_purge` and return `NatterTask`. |
| `kv_history_async(kv, key; batch=256)` | Start `kv_history` and return `NatterTask`. |
| `kv_keys_async(kv)` | Start `kv_keys` and return `NatterTask`. |
| `kv_watch_async(callback, kv; kwargs...)` | Start `kv_watch` and return `NatterTask`. |

## Errors

Public error types derive from `NatterError`: `TimeoutError`, `NoRespondersError`, `ConnectionClosedError`, `ConnectionReconnectingError`, `ConnectionDrainingError`, `ProtocolError`, `AuthenticationError`, `AuthorizationError`, `AuthenticationExpiredError`, `AuthenticationRevokedError`, `AccountAuthenticationExpiredError`, `PermissionViolationError`, `NoServersError`, `MaxPayloadError`, `OutboundBufferLimitError`, `SlowConsumerError`, `JetStreamError`, `FetchDisconnectedError`, `KeyValueError`, `UnsupportedFeatureError`, and `CleanupError`.

KeyValue-specific errors derive from `KeyValueError`: `KeyValueKeyNotFoundError`, `KeyValueKeyDeletedError`, `KeyValueWrongRevisionError`, and `KeyValueKeyExistsError`.
