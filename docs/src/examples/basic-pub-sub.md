# Basic Publish And Subscribe

This example shows the two core subscription styles: explicit `next` calls for scripts/tests and callbacks for services.

```julia
using Natter

client = connect("nats://127.0.0.1:4222"; name="basic-example")

events = subscribe(client, "events.created")

publish(client, "events.created", "event-1")
msg = next(events; timeout=1.0)
@info "received" subject=msg.subject data=String(msg)

close(events)

updates = subscribe(client, "events.updated") do msg
    @info "updated" subject=msg.subject data=String(msg)
end

publish(client, "events.updated", "event-2")
flush(client)

drain(updates)
close(client)
```

Queue groups distribute matching messages across workers:

```julia
worker = subscribe(client, "jobs.ready"; queue="workers") do msg
    process_job(String(msg))
end

publish(client, "jobs.ready", "job-1001")
flush(client)
```

Headers are available on publish and request calls:

```julia
publish(client, "events.created", "payload";
    headers=Dict("trace-id" => "abc-123", "source" => "example"),
)
```

Run independent work concurrently with Julia tasks:

```julia
@sync begin
    @async publish(client, "events.created", "event-3")
    @async publish(client, "events.created", "event-4")
end

flush(client)
```
