# Request Reply Service

Request/reply is useful for lightweight service calls over NATS.

```julia
using Dates
using Natter

client = connect("nats://127.0.0.1:4222"; name="time-service")

service = subscribe(client, "time.now") do msg
    isnothing(msg.reply) && return
    publish(client, msg.reply, string(now(Dates.UTC)))
end

response = request(client, "time.now", ""; timeout=1.0)
println(String(response.data))

drain(service)
close(client)
```

No active responder produces `NoRespondersError` when the server supports no-responder status messages.

```julia
try
    request(client, "missing.service", ""; timeout=0.5)
catch err
    if err isa NoRespondersError
        @warn "service unavailable"
    else
        rethrow()
    end
end
```
