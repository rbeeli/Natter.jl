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

Payloads may be strings, byte vectors, or `nothing`. Encode structured values explicitly before publishing. `publish` validates subjects and the active server payload limit before writing.

Use `prepare_publish` when a hot path sends the same frame repeatedly:

```julia
frame = prepare_publish("metrics.tick", """{"service":"api","value":1}""")

for _ in 1:1_000
    publish(client, frame)
end
```

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

`unsubscribe(sub; max_msgs=n)` keeps an existing subscription open for `n` more messages.

## Request Reply

`request` publishes a message with a reply inbox and waits for one response.

```julia
response = request(client, "users.lookup", "user-42"; timeout=0.5)
user = String(response)
```

A simple service handler:

```julia
service = subscribe(client, "users.lookup") do msg
    isnothing(msg.reply) && return
    publish(client, msg.reply, lookup_user(String(msg)))
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

Natter calls are task-friendly. Use direct calls inside web handlers, workers, and subscription callbacks. Add `@sync`/`@async` only when work should run concurrently:

```julia
user = Ref{Msg}()
permissions = Ref{Msg}()

@sync begin
    @async user[] = request(client, "users.lookup", user_id; timeout=0.2)
    @async permissions[] = request(client, "permissions.lookup", user_id; timeout=0.2)
end

build_response(user[], permissions[])
```

The `_async` helpers are for explicit handles:

```julia
handle = request_async(client, "users.lookup", "user-42"; timeout=0.5)
response = fetch(handle)
```

`fetch(handle)` returns the synchronous result or throws the same operation error.

Use a cancellation token when an outer request or shutdown path needs to stop waiting before the normal timeout:

```julia
source = CancellationSource()
handle = request_async(client, "users.lookup", user_id;
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
