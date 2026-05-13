module Natter

using Base64
using Dates
using EnumX
using JSON3
using MbedTLS
using Random
using Sockets
using URIs

import Base: close, fetch, flush

export Client, ConnectOptions, ConnectionStatus, NatterTask
export Msg, Headers, Subscription, Stats
export connect, close, drain, flush, ping, publish, subscribe, unsubscribe, request, next, new_inbox
export connect_async, close_async, drain_async, flush_async, ping_async, publish_async, subscribe_async, unsubscribe_async, request_async, next_async
export header, headers, status, stats, connected_url
export JetStreamContext, StreamInfo, ConsumerInfo, PubAck, PullSubscription, PushSubscription
export RetentionPolicy, StorageType, DiscardPolicy, StoreCompression, PersistMode
export AckPolicy, DeliverPolicy, ReplayPolicy, PriorityPolicy
export Placement, ExternalStreamSource, SubjectTransform, StreamSource, StreamConsumerLimits, RePublish
export StreamConfig, ConsumerConfig
export jetstream, js_publish, js_publish_async
export stream_create, stream_update, stream_info, stream_list, stream_names, stream_purge, stream_delete
export stream_message_get, stream_message_delete
export stream_create_async, stream_update_async, stream_info_async, stream_list_async, stream_names_async, stream_purge_async, stream_delete_async
export stream_message_get_async, stream_message_delete_async
export consumer_create, consumer_create_or_update, consumer_update, consumer_info, consumer_list, consumer_delete
export consumer_create_async, consumer_create_or_update_async, consumer_update_async, consumer_info_async, consumer_list_async, consumer_delete_async
export pull_subscribe, push_subscribe, fetch
export pull_subscribe_async, push_subscribe_async, fetch_async
export ack, ack_sync, nak, in_progress, term, metadata
export ack_async, ack_sync_async, nak_async, in_progress_async, term_async
export KeyValue, kv_create, kv_open, kv_delete_bucket, kv_get, kv_put, kv_create_key, kv_update, kv_delete, kv_purge, kv_history, kv_keys, kv_watch
export kv_create_async, kv_open_async, kv_delete_bucket_async, kv_get_async, kv_put_async, kv_create_key_async, kv_update_async
export kv_delete_async, kv_purge_async, kv_history_async, kv_keys_async, kv_watch_async
export NatterError, TimeoutError, NoRespondersError, ConnectionClosedError, ConnectionReconnectingError
export ConnectionDrainingError, ProtocolError, AuthorizationError, NoServersError, MaxPayloadError
export OutboundBufferLimitError, SlowConsumerError, JetStreamError, UnsupportedFeatureError, CleanupError

const CRLF = "\r\n"
const CRLF_BYTES = UInt8[0x0d, 0x0a]
const DEFAULT_URL = "nats://localhost:4222"
const DEFAULT_INBOX_PREFIX = "_INBOX"
const EMPTY_BYTES = UInt8[]
const CLIENT_VERSION = "0.1.0"
const NUID_ALPHABET = collect("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
const DEFAULT_MAX_CONTROL_LINE = 16 * 1024
const DEFAULT_MAX_INBOUND_PAYLOAD = 64 * 1024 * 1024
const DEFAULT_MAX_HEADER_BYTES = 64 * 1024

include("errors.jl")
include("types.jl")
include("protocol.jl")
include("connection.jl")
include("core.jl")
include("jetstream_types.jl")
include("jetstream.jl")
include("keyvalue.jl")
include("async.jl")

end # module
