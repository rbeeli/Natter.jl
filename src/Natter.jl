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

export Client, ConnectOptions, ConnectionStatus
export Msg, Headers, Subscription, Stats
export connect, close, drain, flush, ping, publish, subscribe, unsubscribe, request, next, new_inbox
export header, headers, status, stats, connected_url
export JetStreamContext, StreamInfo, ConsumerInfo, PubAck, PullSubscription, PushSubscription
export RetentionPolicy, StorageType, DiscardPolicy, StoreCompression, PersistMode
export AckPolicy, DeliverPolicy, ReplayPolicy, PriorityPolicy
export Placement, ExternalStreamSource, SubjectTransform, StreamSource, StreamConsumerLimits, RePublish
export StreamConfig, ConsumerConfig
export jetstream, js_publish, publish_async
export stream_create, stream_update, stream_info, stream_list, stream_names, stream_purge, stream_delete
export stream_message_get, stream_message_delete
export consumer_create, consumer_update, consumer_info, consumer_list, consumer_delete
export pull_subscribe, push_subscribe, fetch
export ack, ack_sync, nak, in_progress, term, metadata
export KeyValue, kv_create, kv_open, kv_delete_bucket, kv_get, kv_put, kv_create_key, kv_update, kv_delete, kv_purge, kv_history, kv_keys, kv_watch
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

include("errors.jl")
include("types.jl")
include("protocol.jl")
include("connection.jl")
include("core.jl")
include("jetstream_types.jl")
include("jetstream.jl")
include("keyvalue.jl")

end # module
