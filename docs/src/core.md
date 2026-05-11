# Core Messaging

Core messaging covers the NATS protocol without JetStream persistence.

## Connect Options

`connect(url_or_urls; kwargs...)` builds a client and performs the server handshake. Common options are:

| Option | Default | Purpose |
| --- | --- | --- |
| `name` | `nothing` | Human-readable connection name sent to the server. |
| `token`, `user`, `password` | `nothing` | Authentication credentials. |
| `no_echo` | `false` | Prevent this connection from receiving its own publishes. |
| `connect_timeout` | `2.0` | Socket and handshake timeout in seconds. |
| `allow_reconnect` | `true` | Enable automatic reconnect after transient failures. |
| `pending_size` | `2 MiB` | Maximum buffered outbound publish data while reconnecting. |
| `sub_pending_msgs_limit` | `1024` | Default per-subscription queued message limit. |
| `sub_pending_bytes_limit` | `128 MiB` | Default per-subscription queued byte limit. |
| `error_cb` | warning callback | Receives asynchronous callback, cleanup, and background task errors. |

Use `status(client)`, `stats(client)`, and `connected_url(client)` for runtime inspection.

## Publish

```julia
publish(client, "events.created", "payload")
publish(client, "events.created", UInt8[0x01, 0x02])
publish(client, "events.created"; headers=Headers("trace-id" => ["abc-123"]))
```

Payloads are converted to bytes. `String`, `Vector{UInt8}`, `AbstractVector{UInt8}`, and `nothing` are supported by the public API.

`publish` validates subjects and server `max_payload`. If the client is reconnecting, publish data is buffered up to `pending_size` and replayed after reconnect.

## Subscribe

```julia
sub = subscribe(client, "events.*";
    pending_msgs_limit=4096,
    pending_bytes_limit=64 * 1024 * 1024,
)

msg = next(sub; timeout=1.0)
```

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

## Request Reply

`request` creates a temporary inbox, publishes with a reply subject, waits for one response, and cleans up the inbox subscription.

```julia
response = request(client, "time.now", ""; timeout=1.0)
println(String(response.data))
```

No responder responses are raised as `NoRespondersError`.

## Headers

Headers are represented as `Dict{String,Vector{String}}` through the `Headers` alias.

```julia
headers = Headers(
    "Nats-Msg-Id" => ["event-1001"],
    "trace-id" => ["abc-123"],
)

publish(client, "events.created", "payload"; headers)
```

Read headers from received messages with `header(msg, "name")` or copy all headers with `headers(msg)`.

## Flush, Drain, And Close

`flush(client)` sends a `PING` and waits for the matching `PONG`, which confirms the server has processed commands sent before the flush.

```julia
publish(client, "events.created", "payload")
flush(client; timeout=2.0)
```

`drain(sub)` unsubscribes and waits for queued callback work to finish. `drain(client)` drains all subscriptions, flushes, and closes the client.

```julia
drain(client; timeout=10.0)
```

`close(client)` stops background tasks, closes subscriptions and transports, and invokes the close callback. Use `close(client; throw_errors=true)` if cleanup failures must be surfaced to the caller.
