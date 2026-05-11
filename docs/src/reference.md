# Reference

This page summarizes the public API. Optional keyword defaults are documented in the guide pages when behavior matters.

## Core Types

| Type | Purpose |
| --- | --- |
| `Client` | Active client connection with background reader, ping, and reconnect tasks. |
| `ConnectOptions` | Keyword-backed connection configuration. |
| `ConnectionStatus` | EnumX status namespace: `DISCONNECTED`, `CONNECTING`, `CONNECTED`, `RECONNECTING`, `DRAINING`, `CLOSED`. |
| `Msg` | Received message with `subject`, `reply`, `data`, `headers`, and acknowledgement state. |
| `Headers` | Alias for `Dict{String,Vector{String}}`. |
| `Subscription` | Core subscription handle. |
| `Stats` | Snapshot of message, byte, reconnect, error, and drop counters. |

## Core Functions

| Function | Purpose |
| --- | --- |
| `connect(url_or_urls=nothing; kwargs...)` | Connect to one or more servers. |
| `publish(client, subject, data=nothing; reply=nothing, headers=nothing)` | Publish a core message. |
| `subscribe(client, subject; queue=nothing, callback=nothing, max_msgs=0, ...)` | Create a core subscription. |
| `next(sub; timeout=1.0)` | Wait for the next message from a subscription. |
| `request(client, subject, data=nothing; timeout=1.0, headers=nothing)` | Send a request and wait for one response. |
| `flush(client; timeout=10.0)` | Wait until the server has processed previous commands. |
| `ping(client; timeout=10.0)` | Alias for `flush`. |
| `unsubscribe(sub; max_msgs=0)` | Unsubscribe immediately or after `max_msgs` additional messages. |
| `drain(sub; timeout=...)` | Unsubscribe and wait for queued callback work. |
| `drain(client; timeout=...)` | Drain subscriptions, flush, and close the client. |
| `close(client; throw_errors=false)` | Close transports, tasks, subscriptions, and callbacks. |
| `new_inbox(client; prefix=...)` | Generate a reply inbox subject. |
| `header(msg, key)` | Return the first header value or `nothing`. |
| `headers(msg)` | Return a copy of all message headers. |
| `status(client)` | Return current `ConnectionStatus`. |
| `stats(client)` | Return a `Stats` snapshot. |
| `connected_url(client)` | Return the current server URL or `nothing`. |

## JetStream Types

| Type | Purpose |
| --- | --- |
| `JetStreamContext` | JetStream API context for a client. |
| `StreamConfig` | Typed stream configuration. |
| `ConsumerConfig` | Typed consumer configuration. |
| `StreamInfo` | Stream info response with typed config, state, and raw data. |
| `ConsumerInfo` | Consumer info response with typed config and raw data. |
| `PubAck` | Publish acknowledgement with stream, sequence, duplicate, and domain. |
| `PullSubscription` | Pull consumer handle. |
| `PushSubscription` | Push consumer handle. |

## JetStream Enums

| Enum Namespace | Values |
| --- | --- |
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
| --- | --- |
| `jetstream(client; prefix="$JS.API", timeout=5.0)` | Create a JetStream context. |
| `js_publish(js, subject, data=nothing; timeout=..., stream=nothing, headers=nothing)` | Publish and wait for a `PubAck`. |
| `publish_async(js, subject, data=nothing; kwargs...)` | Start a JetStream publish in a Julia task. |
| `stream_create(js, config)` | Create a stream from `StreamConfig` or `Dict`. |
| `stream_update(js, config)` | Update a stream. |
| `stream_info(js, name)` | Fetch stream info. |
| `stream_list(js; offset=0)` | List streams, following pagination. |
| `stream_names(js; subject=nothing)` | List stream names, optionally filtered by subject. |
| `stream_purge(js, name; filter_subject=nothing)` | Purge a stream or subject subset. |
| `stream_delete(js, name)` | Delete a stream. |
| `stream_message_get(js, stream; seq=nothing, subject=nothing, direct=false, next_by_subject=false)` | Read a stored message. |
| `stream_message_delete(js, stream, seq)` | Delete one stored message. |
| `consumer_create(js, stream, config)` | Create a consumer from `ConsumerConfig` or `Dict`. |
| `consumer_update(js, stream, config)` | Update a consumer. |
| `consumer_info(js, stream, consumer)` | Fetch consumer info. |
| `consumer_list(js, stream; offset=0)` | List consumers for a stream. |
| `consumer_delete(js, stream, consumer)` | Delete a consumer. |
| `pull_subscribe(js, subject; stream=nothing, durable=nothing, config=ConsumerConfig())` | Create a pull subscription. |
| `push_subscribe(js, subject; stream=nothing, durable=nothing, queue=nothing, callback=nothing, manual_ack=false, config=ConsumerConfig())` | Create a push subscription. |
| `fetch(psub, batch=1; timeout=..., expires=timeout)` | Fetch a batch from a pull subscription. |
| `ack`, `ack_sync`, `nak`, `in_progress`, `term` | Acknowledge or control redelivery for JetStream messages. |
| `metadata(msg)` | Parse JetStream delivery metadata. |

## KeyValue Functions

| Function | Purpose |
| --- | --- |
| `kv_create(js, bucket; history=1, storage="file", replicas=1, direct=false)` | Create a bucket. |
| `kv_open(js, bucket)` | Open an existing bucket. |
| `kv_delete_bucket(kv)` | Delete a bucket. |
| `kv_get(kv, key; revision=nothing, direct=nothing)` | Get the latest value or a revision. |
| `kv_put(kv, key, value; revision=nothing)` | Put a value, optionally expecting a revision. |
| `kv_create_key(kv, key, value)` | Put a value only if the key is absent. |
| `kv_update(kv, key, value, revision)` | Put a value only if the key is at `revision`. |
| `kv_delete(kv, key)` | Mark a key deleted. |
| `kv_purge(kv, key)` | Purge a key with rollup. |
| `kv_history(kv, key; batch=256)` | Return historical messages for a key. |
| `kv_keys(kv)` | Return active keys. |
| `kv_watch(callback, kv; key=">", history=false)` | Watch bucket updates with a push subscription. |

## Errors

Public error types derive from `NatterError`: `TimeoutError`, `NoRespondersError`, `ConnectionClosedError`, `ConnectionReconnectingError`, `ConnectionDrainingError`, `ProtocolError`, `AuthorizationError`, `NoServersError`, `MaxPayloadError`, `OutboundBufferLimitError`, `SlowConsumerError`, `JetStreamError`, `UnsupportedFeatureError`, and `CleanupError`.
