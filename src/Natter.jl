module Natter

using Base64
using Dates
using EnumX
using FunctionWrappers
using JSON3
using MbedTLS
using Random
using Sockets
using URIs
using libsodium_jll

import Base: close, fetch, flush

export JetStream, KeyValue
export Client, ConnectOptions, ConnectionStatus, ConnectionEventKind, ConnectionEvent
export CancellationSource, CancellationToken, cancellation_token, cancel!, iscancelled
export AbstractAuth, NoAuth, TokenAuth, UserPassAuth, NKeyAuth, JwtAuth, CredentialsAuth, CallbackAuth, AuthRequest
export Msg, BorrowedMsg, Headers, PublishFrame, Subscription, Stats, SubscriptionStats
export connect, close, drain, flush, ping, publish, respond, prepare_publish, subscribe, unsubscribe, request, next, new_inbox
export header, headers, status, stats, connected_url
export NatterError, TimeoutError, CancelledError, NoRespondersError, ConnectionClosedError, ConnectionReconnectingError
export ConnectionDrainingError, ProtocolError, AuthenticationError, AuthorizationError
export AuthenticationExpiredError, AuthenticationRevokedError, AccountAuthenticationExpiredError, PermissionViolationError
export NoServersError, MaxPayloadError
export OutboundBufferLimitError, SlowConsumerError, ConsumerSequenceMismatchError, JetStreamError, FetchDisconnectedError, KeyValueError, KeyValueKeyNotFoundError
export KeyValueKeyDeletedError, KeyValueWrongRevisionError, KeyValueKeyExistsError, UnsupportedFeatureError, CleanupError

const CRLF = "\r\n"
const CRLF_BYTES = UInt8[0x0d, 0x0a]
const DEFAULT_URL = "nats://localhost:4222"
const DEFAULT_INBOX_PREFIX = "_INBOX"
const EMPTY_BYTES = UInt8[]
const CLIENT_VERSION = string(pkgversion(@__MODULE__))
const NUID_ALPHABET = collect("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
const DEFAULT_READ_BUFFER_SIZE = 64 * 1024
const DEFAULT_WRITE_BUFFER_SIZE = 32 * 1024
const DEFAULT_WRITE_TIMEOUT = 10.0
const DEFAULT_MAX_CONTROL_LINE = 16 * 1024
const DEFAULT_MAX_INBOUND_PAYLOAD = 64 * 1024 * 1024
const DEFAULT_MAX_HEADER_BYTES = 64 * 1024
const DEFAULT_DIRECT_WRITE_THRESHOLD = 256 * 1024
const _MAX_TIMER_DELAY_SECONDS = prevfloat(Float64(typemax(UInt64)) / 1000)

include("errors.jl")
include("tasks.jl")
include("cancellation.jl")
include("secrets.jl")
include("types.jl")
include("auth.jl")
include("protocol.jl")
include("connection.jl")
include("core.jl")
include("jetstream_types.jl")
include("jetstream.jl")
include("keyvalue.jl")
include("submodules.jl")

end # module
