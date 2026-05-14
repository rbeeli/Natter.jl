# Core Messaging

Core messaging covers the NATS protocol without JetStream persistence.

## Connect Options

`connect(url_or_urls; kwargs...)` builds a client and performs the server handshake. Common options are:

| Option | Default | Purpose |
| :--- | :--- | :--- |
| `name` | `nothing` | Human-readable connection name sent to the server. |
| `token`, `user`, `password` | `nothing` | Token or user/password authentication credentials. |
| `nkey`, `nkey_seed`, `nkey_seed_path` | `nothing` | NKEY authentication. A seed can derive the public NKEY and sign the server nonce. |
| `jwt`, `jwt_path`, `credentials`, `credentials_path` | `nothing` | User JWT and `.creds` authentication. |
| `signature_cb` | `nothing` | Callback for custom nonce signing; return the raw 64-byte Ed25519 signature. |
| `no_echo` | `false` | Prevent this connection from receiving its own publishes. |
| `connect_timeout` | `2.0` | Socket and handshake timeout in seconds. |
| `ping_interval` | `120.0` | Background keepalive interval in seconds. |
| `max_outstanding_pings` | `2` | Missed keepalive PINGs allowed before reconnecting. |
| `allow_reconnect` | `true` | Enable automatic reconnect after transient failures. |
| `reconnect_wait`, `reconnect_max_wait`, `reconnect_jitter` | `0.5`, `5.0`, `0.1` | Reconnect backoff timing in seconds. |
| `max_reconnect_attempts` | `-1` | Maximum reconnect loop attempts; `-1` means unlimited. |
| `pending_size` | `2 MiB` | Maximum outbound publish data retained for reconnect replay, including reconnect-time publishes and buffered connected publishes. |
| `write_buffer_size` | `32 KiB` | Outbound write buffer size for coalescing small writes. Set to `0` to disable it; publish frames at or above this size bypass the buffer. |
| `write_buffer_latency` | `0.001` | Maximum background flusher delay in seconds for coalescing non-threshold buffered writes. Set to `0` to yield and flush as soon as possible. |
| `write_timeout` | `10.0` | Maximum seconds a transport write or flush may block before the active transport is closed and the operation times out. |
| `max_control_line` | `16 KiB` | Maximum inbound protocol control line length. |
| `max_inbound_payload` | `64 MiB` | Maximum inbound message payload allocation. |
| `max_header_bytes` | `64 KiB` | Maximum inbound header block size. |
| `max_stale_pong_waiters` | `1024` | Maximum timed-out flush waiters retained to preserve PING/PONG ordering. |
| `sub_pending_msgs_limit` | `1024` | Default per-subscription queued message limit. |
| `sub_pending_bytes_limit` | `128 MiB` | Default per-subscription queued byte limit, including NATS header blocks. |
| `close_callback_timeout` | `5.0` | Seconds `close(client)` waits for subscription callback tasks before reporting a cleanup timeout. User callbacks are not interrupted. |
| `error_cb` | warning callback | Receives asynchronous callback, cleanup, and background task errors. |

Durations and production limits are validated when `ConnectOptions` is built. Timeouts, keepalive intervals, pending limits, parser limits, stale waiter limits, and subscription pending limits must be positive. `reconnect_jitter`, `write_buffer_latency`, `write_buffer_size`, and `close_callback_timeout` can be zero, and `max_reconnect_attempts` must be `-1` or non-negative.

Authentication must use one scheme: token, complete `user`/`password`, NKEY, or user JWT credentials. Provide credentials either in options or URL userinfo, not both.

For NKEY auth, pass `nkey_seed` or `nkey_seed_path` to derive the public key, or pass `nkey` with `signature_cb` when signing is managed externally. For user JWT auth, pass a `.creds` file with `credentials_path`, inline `.creds` content with `credentials`, or pair `jwt`/`jwt_path` with a seed or signing callback. NKEY/JWT auth requires a server nonce, which nats-server 2.x provides.

For short connection security snippets, see [Connection Auth And TLS](examples/connection-auth-tls.md).

`ConnectOptions` is immutable. Connection settings are frozen when options are built and the client reads them concurrently from background tasks; create a new client to change connection behavior.

Use `status(client)`, `stats(client)`, and `connected_url(client)` for runtime inspection.

## Julia Task Concurrency

Julia does not require `async`/`await` syntax for normal task-based application code. A web handler or service task can call Natter directly:

```julia
function lookup_user(client, body)
    response = request(client, "users.lookup", body; timeout=0.2)
    publish(client, "audit.users.lookup", body)
    response
end
```

Use `@sync` and `@async` when independent NATS work should run concurrently:

```julia
user = Ref{Msg}()
permissions = Ref{Msg}()

@sync begin
    @async user[] = request(client, "users.lookup", user_id; timeout=0.2)
    @async permissions[] = request(client, "permissions.lookup", user_id; timeout=0.2)
end

build_response(user[], permissions[])
```

The `_async` APIs return `NatterTask` handles for explicit handle-oriented code. They are not needed just because code runs in a task. If the operation fails, `fetch(handle)` throws the same operation error that the synchronous API would throw.

## Publish

```julia
publish(client, "events.created", "payload")
publish(client, "events.created", UInt8[0x01, 0x02])
publish(client, "events.created"; headers=Dict("trace-id" => "abc-123"))

frame = prepare_publish("events.created", "payload")
publish(client, frame)
```

Payloads are converted to bytes. `String`, `Vector{UInt8}`, `AbstractVector{UInt8}`, and `nothing` are supported by the public API.

`publish` validates subjects and server `max_payload`. Use `prepare_publish` for repeated hot-path publishes with the same subject, reply, headers, and payload; the returned `PublishFrame` is a validated immutable snapshot, and publishing it still checks the active server capability and payload limit. Connected clients use a buffered write flusher for throughput; small writes are coalesced up to `write_buffer_latency` unless the buffer threshold is reached first. Transport writes and flushes are bounded by `write_timeout`; a timed-out write closes the active transport so reconnect or close can proceed. Call `flush(client)` when the application needs a server round trip confirming earlier commands were processed. Publish data retained for reconnect replay, whether queued during reconnect or still buffered on a connected transport, is bounded by `pending_size` and replayed after reconnect on a best-effort basis.

Core publish replay should be treated as at-least-once for retained frames, not exactly-once. Ambiguous transport failures can duplicate delivery, and frames at or above `write_buffer_size` are not retained after a successful direct write. Use JetStream publish message IDs or idempotent consumers when duplicate effects are unacceptable.

## Subscribe

```julia
sub = subscribe(client, "events.*";
    pending_msgs_limit=4096,
    pending_bytes_limit=64 * 1024 * 1024,
)

msg = next(sub; timeout=1.0)
```

Per-subscription pending limits must be positive.

Callback subscriptions are callback-only; use `next` with subscriptions created without a callback.

Use queue groups for load-balanced subscribers:

```julia
worker = subscribe(client, "jobs.process"; queue="workers") do msg
    process_job(String(msg.data))
end
```

`max_msgs` automatically closes a subscription after receiving a fixed number of messages:

```julia
one = subscribe(client, "startup.ready"; max_msgs=1)
msg = next(one; timeout=5.0)
```

`unsubscribe(sub; max_msgs=n)` keeps an existing subscription open for `n` additional messages. The client tracks messages already delivered on that subscription and replays only the remaining allowance after reconnect.

## Request Reply

`request` uses a shared inbox subscription, publishes with a unique reply subject, waits for one response, and cleans up the request waiter.

```julia
response = request(client, "time.now", ""; timeout=1.0)
println(String(response.data))
```

When negotiated with a server that supports header status messages, no responder responses are raised as `NoRespondersError`.

## Headers

Received headers are represented as `Headers`, a case-insensitive dictionary of `String` names to `Vector{String}` values. Publish and request calls accept `Headers`, dictionaries with string or vector values, or pair iterators. Header names must be valid NATS/HTTP token field names.
Header publish and request calls require server header support advertised in INFO; older servers that do not advertise it raise `UnsupportedFeatureError` before Natter writes an `HPUB`.

```julia
headers = Dict(
    "Nats-Msg-Id" => "event-1001",
    "trace-id" => "abc-123",
)

publish(client, "events.created", "payload"; headers)
request(client, "events.lookup", "payload"; headers=("trace-id" => "abc-123",))
```

Read headers from received messages with `header(msg, "name")` or copy all headers with `headers(msg)`. Header lookup is case-insensitive, and mixed-case duplicates are merged into one entry using the first inserted field spelling for iteration and serialization.

## Flush, Drain, And Close

`flush(client)` sends a `PING` and waits for the matching `PONG`, which confirms the server has processed commands sent before the flush.

```julia
publish(client, "events.created", "payload")
flush(client; timeout=2.0)
```

`drain(sub)` unsubscribes and waits for queued callback work to finish. Its `timeout` is one overall deadline for the unsubscribe flush and callback work. `drain(client)` shares the same deadline across all subscriptions and the final flush, then closes the client.

```julia
drain(client; timeout=10.0)
```

`close(client)` stops background tasks, closes subscriptions and transports, waits up to `close_callback_timeout` for active subscription callbacks, and invokes the close callback. It does not interrupt user callback code; use `drain` first when shutdown must wait for queued callback work. Use `close(client; throw_errors=true)` if cleanup failures must be surfaced to the caller.

```julia
close(client; throw_errors=true)
```
