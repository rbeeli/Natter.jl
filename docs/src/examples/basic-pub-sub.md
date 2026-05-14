# Basic Publish And Subscribe

This example uses both pull-style `next` and callback-style subscriptions.

```julia
using Natter

client = connect("nats://127.0.0.1:4222"; name="basic-example")

events = subscribe(client, "events.created")

publish(client, "events.created", "event-1")
msg = next(events; timeout=1.0)
@info "received synchronously" subject=msg.subject data=String(msg.data)

close(events)

callback_sub = subscribe(client, "events.updated") do msg
    @info "received in callback" subject=msg.subject data=String(msg.data)
end

publish(client, "events.updated", "event-1")
flush(client)

drain(callback_sub)
close(client)
```

Use `flush` when the example or test needs to know that commands sent before it reached the server. Use Julia `@sync` and `@async` when multiple independent publishes or requests should run concurrently.

With an active client, queue groups distribute matching messages across workers:

```julia
worker = subscribe(client, "jobs.ready"; queue="workers") do msg
    @info "job" id=String(msg.data)
end

publish(client, "jobs.ready", "job-1001")
flush(client)
close(worker)
```

Headers are accepted on publish and request calls when the server supports NATS headers:

```julia
publish(client, "events.created", "payload";
    headers=Dict("trace-id" => "abc-123", "source" => "example"),
)
```
