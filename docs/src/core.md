# Core Messaging

Core messaging is the lightweight NATS API: publish/subscribe, queue groups, request/reply, headers, flush, drain, and close.

## Connect For A Service

```julia
client = connect([
    "nats://nats-a.internal:4222",
    "nats://nats-b.internal:4222",
];
    name="orders-api",
    no_echo=true,
    pending_size=8 * 1024 * 1024,
    sub_pending_msgs_limit=4096,
    error_cb=err -> @error "NATS client error" exception=err,
    event_cb=event -> begin
        event.kind == ConnectionEventKind.RECONNECTED &&
            @info "NATS reconnected" url=event.url
    end,
)
```

Most applications only need a few connection options. Production reconnect, buffering, TLS, and auth examples are in [Reliability And TLS](reliability.md).

## Publish

```julia
publish(client, "events.created", "payload")
publish(client, "events.created", UInt8[0x01, 0x02])
publish(client, "events.created", """{"id":1001,"status":"created"}""")
```

Payloads may be strings, byte vectors, or `nothing`. Encode structured values explicitly before publishing. `publish` validates subjects and the active server payload limit before handing the command to the writer.

The default `mode=:queued` is the normal hot path on connected clients. It hands publish commands to a bounded background writer, which batches socket writes while preserving command order. Call `flush(client)` when the application needs a server round trip proving earlier commands were processed.

For repeated identical messages, prepare a frame once and publish the frame in queued mode:

```julia
frame = prepare_publish("metrics.tick", """{"service":"api","value":1}""")

for _ in 1:1_000
    publish(client, frame)
end
```

For mutable scratch buffers that you rewrite immediately, `mode=:direct` writes on the caller task and avoids copying the frame into the writer queue:

```julia
payload = Vector{UInt8}(undef, 256)

for metric in metrics
    n = encode_metric!(payload, metric)
    publish(client, "metrics.raw", @view(payload[1:n]);
        mode=:direct,
    )
end
```

For a whole low-latency connection, disable the write buffer:

```julia
client = connect("nats://127.0.0.1:4222";
    write_buffer_size=0,
    read_buffer_size=256 * 1024,
)
```

Then normal publish calls on that client write directly and skip replay buffering by default:

```julia
publish(client, "metrics.raw", @view(payload[1:n]))
```

Core publishes skip reconnect replay buffering by default and fail during reconnect. Use `mode=:replayable` or `buffer_on_reconnect=true` only when best-effort replay is preferable to letting the caller retry, drop, or rebuild the message.

The publish choices are:

- Plain `publish` uses `mode=:queued`, validates, and sends through the background writer when it is active.
- `publish(...; mode=:direct)` writes on the caller task and bypasses the queued writer for that call.
- `publish(...; mode=:replayable)` retains buffered frames for best-effort reconnect replay up to `pending_size`.
- `prepare_publish` copies once into a safe reusable `PublishFrame` for low-allocation queued publishing.

Call `flush(client)` when the application needs a server round trip proving earlier commands were processed.

```julia
publish(client, "events.created", "payload")
flush(client; timeout=2.0)
```

## Subscribe

Use `next` for explicit pull-style reads:

```julia
sub = subscribe(client, "events.*";
    pending_msgs_limit=4096,
    pending_bytes_limit=64 * 1024 * 1024,
)

msg = next(sub; timeout=1.0)
@info "event" subject=msg.subject data=String(msg)
```

Use callbacks for long-running services:

```julia
sub = subscribe(client, "events.created") do msg
    handle_event(String(msg))
end
```

Callback subscriptions are callback-only. Use `next` only with subscriptions created without a callback.

Normal callbacks run on Natter-managed Julia tasks scheduled on the default thread pool and are serialized per subscription. Synchronize shared mutable state the same way you would for any Julia task.

Use `callback_mode=:inline` for callback hot paths that process bytes during the callback and do not retain the message:

```julia
sub = subscribe(client, "ticks.raw"; callback_mode=:inline) do msg
    value = decode_tick(msg.data)
    record_tick!(value)
end
```

Inline callbacks receive `BorrowedMsg`. Its `data` is a view into the reader buffer and is valid only until the callback returns. Copy only at the boundary where the data must outlive the callback:

```julia
jobs = Channel{Vector{UInt8}}(1024)

sub = subscribe(client, "ticks.raw"; callback_mode=:inline) do msg
    put!(jobs, copy(msg.data))
end
```

Inline callbacks run on the reader task. Keep them short and nonblocking; avoid `request`, `flush`, `next`, slow file or network IO, and unbounded `put!` calls from an inline callback. If the work can block, copy the bytes and hand them to another task. Header messages can still allocate header storage; the inline fast path is for payload bytes. `borrowed=true` is accepted as a compatibility alias for `callback_mode=:inline`.

The subscribe choices are:

- `next(sub)` and normal callback subscriptions deliver owned `Msg` values that can be retained safely.
- `subscribe(...; callback_mode=:inline) do msg ... end` delivers callback-only `BorrowedMsg` values and avoids the payload copy and callback task handoff.
- Use a bounded queue plus `copy(msg.data)` when borrowed input needs asynchronous processing.

## Queue Groups

Queue groups load-balance messages across subscribers with the same queue name.

```julia
worker = subscribe(client, "jobs.process"; queue="workers") do msg
    process_job(String(msg))
end
```

Limit a subscription to a fixed number of messages:

```julia
one = subscribe(client, "startup.ready"; max_msgs=1)
msg = next(one; timeout=5.0)
```

`unsubscribe(sub; max_msgs=n)` keeps an existing subscription open for `n` more protocol deliveries. `stats(sub)` reports both `delivered` and user-visible `received` counts, which can differ when slow-consumer limits drop messages.

## Request Reply

`request` publishes a message with a reply inbox and waits for one response.

```julia
response = request(client, "users.lookup", "user-42"; timeout=0.5)
user = String(response)
```

A simple service handler:

```julia
service = subscribe(client, "users.lookup"; callback_mode=:inline) do msg
    isnothing(msg.reply) && return
    respond(client, msg, lookup_user(String(msg)))
end
```

No active responder raises `NoRespondersError` when the server supports NATS no-responder status messages.

```julia
try
    request(client, "missing.service", ""; timeout=0.2)
catch err
    err isa NoRespondersError || rethrow()
    @warn "service unavailable"
end
```

## Headers

Publish and request calls accept `Headers`, dictionaries, named tuples, or pair iterators:

```julia
publish(client, "events.created", "payload";
    headers=Dict(
        "Nats-Msg-Id" => "event-1001",
        "trace-id" => "abc-123",
    ),
)

response = request(client, "events.lookup", "event-1001";
    headers=("trace-id" => "abc-123",),
)
```

Read received headers with case-insensitive lookup:

```julia
trace_id = header(response, "Trace-Id")
all_headers = headers(response)
```

Header publish and request calls require server header support.

## Julia Task Concurrency

Natter calls are task-friendly. Use direct calls inside web handlers, workers, and subscription callbacks. Add `@sync`/`Threads.@spawn` only when work should run concurrently:

```julia
user = Ref{Msg}()
permissions = Ref{Msg}()

@sync begin
    Threads.@spawn user[] = request(client, "users.lookup", user_id; timeout=0.2)
    Threads.@spawn permissions[] = request(client, "permissions.lookup", user_id; timeout=0.2)
end

build_response(user[], permissions[])
```

Use Julia tasks directly when you want an explicit handle:

```julia
handle = Threads.@spawn request(client, "users.lookup", "user-42"; timeout=0.5)
response = fetch(handle)
```

`fetch(handle)` uses normal Julia task semantics. If the task failed, Julia throws
`TaskFailedException`; wrap the task body when you want to return an error value.

Use a cancellation token when an outer request or shutdown path needs to stop waiting before the normal timeout:

```julia
source = CancellationSource()
handle = Threads.@spawn request(client, "users.lookup", user_id;
                        timeout=5.0,
                        cancel_token=cancellation_token(source))

cancel!(source)
```

Cancelled operations throw `CancelledError`.

## Drain And Close

`drain(sub)` unsubscribes and waits for queued callback work. `drain(client)` drains subscriptions, flushes, and closes the client within one timeout.

```julia
drain(client; timeout=10.0)
```

`close(client)` is immediate teardown. In strict shutdown code or tests, pass `throw_errors=true` to surface cleanup failures.

```julia
close(client; throw_errors=true)
```
