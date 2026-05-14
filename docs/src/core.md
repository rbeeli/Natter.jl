# Core Messaging

Core messaging covers the NATS protocol without JetStream persistence.

## Connect Options

`connect(url_or_urls; kwargs...)` builds a client and performs the server handshake. Common options are:

| Option | Default | Purpose |
| :--- | :--- | :--- |
| `name` | `nothing` | Human-readable connection name sent to the server. |
| `token`, `user`, `password` | `nothing` | Authentication credentials. |
| `no_echo` | `false` | Prevent this connection from receiving its own publishes. |
| `connect_timeout` | `2.0` | Socket and handshake timeout in seconds. |
| `ping_interval` | `120.0` | Background keepalive interval in seconds. |
| `max_outstanding_pings` | `2` | Missed keepalive PINGs allowed before reconnecting. |
| `allow_reconnect` | `true` | Enable automatic reconnect after transient failures. |
| `reconnect_wait`, `reconnect_max_wait`, `reconnect_jitter` | `0.5`, `5.0`, `0.1` | Reconnect backoff timing in seconds. |
| `max_reconnect_attempts` | `-1` | Maximum reconnect loop attempts; `-1` means unlimited. |
| `pending_size` | `2 MiB` | Maximum outbound publish data retained for reconnect replay, including unflushed connected publishes. |
| `write_buffer_size` | `32 KiB` | Outbound write buffer size for coalescing small writes. Set to `0` to disable it; publish frames at or above this size bypass the buffer. |
| `write_buffer_latency` | `0.001` | Maximum background flusher delay in seconds for coalescing non-threshold buffered writes. Set to `0` to yield and flush as soon as possible. |
| `max_control_line` | `16 KiB` | Maximum inbound protocol control line length. |
| `max_inbound_payload` | `64 MiB` | Maximum inbound message payload allocation. |
| `max_header_bytes` | `64 KiB` | Maximum inbound header block size. |
| `max_stale_pong_waiters` | `1024` | Maximum timed-out flush waiters retained to preserve PING/PONG ordering. |
| `sub_pending_msgs_limit` | `1024` | Default per-subscription queued message limit. |
| `sub_pending_bytes_limit` | `128 MiB` | Default per-subscription queued byte limit, including NATS header blocks. |
| `error_cb` | warning callback | Receives asynchronous callback, cleanup, and background task errors. |

Durations and production limits are validated when `ConnectOptions` is built. Timeouts, keepalive intervals, pending limits, parser limits, stale waiter limits, and subscription pending limits must be positive. `reconnect_jitter`, `write_buffer_latency`, and `write_buffer_size` can be zero, and `max_reconnect_attempts` must be `-1` or non-negative.

Authentication must use either `token` or a complete `user`/`password` pair. Provide credentials either in options or URL userinfo, not both.

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

The `_async` APIs return `NatterTask` handles for explicit handle-oriented code. They are not needed just because code runs in a task. If the operation fails, `fetch(handle)` throws a `CapturedException` whose `ex` field is the original operation error and whose displayed stack trace points at the failed task.

## Publish

```julia
publish(client, "events.created", "payload")
publish(client, "events.created", UInt8[0x01, 0x02])
publish(client, "events.created"; headers=Dict("trace-id" => "abc-123"))
```

Payloads are converted to bytes. `String`, `Vector{UInt8}`, `AbstractVector{UInt8}`, and `nothing` are supported by the public API.

`publish` validates subjects and server `max_payload`. Connected clients use a buffered write flusher for throughput; small writes are coalesced up to `write_buffer_latency` unless the buffer threshold is reached first. Call `flush(client)` when the application needs a server round trip confirming earlier commands were processed. Publish data retained for reconnect replay, whether queued during reconnect or still unflushed on a connected transport, is bounded by `pending_size` and replayed after reconnect.

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

Received headers are represented as `Dict{String,Vector{String}}` through the `Headers` alias. Publish and request calls accept `Headers`, dictionaries with string or vector values, or pair iterators. Header names must be valid NATS/HTTP token field names.
Header publish and request calls require server header support advertised in INFO; older servers that do not advertise it raise `UnsupportedFeatureError` before Natter writes an `HPUB`.

```julia
headers = Dict(
    "Nats-Msg-Id" => "event-1001",
    "trace-id" => "abc-123",
)

publish(client, "events.created", "payload"; headers)
request(client, "events.lookup", "payload"; headers=("trace-id" => "abc-123",))
```

Read headers from received messages with `header(msg, "name")` or copy all headers with `headers(msg)`. Header lookup is case-insensitive; copied headers preserve the casing received on the wire.

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

`close(client)` stops background tasks, closes subscriptions and transports, and invokes the close callback. Use `close(client; throw_errors=true)` if cleanup failures must be surfaced to the caller.

```julia
close(client; throw_errors=true)
```
