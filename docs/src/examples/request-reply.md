# Request Reply Service

Request/reply is useful for lightweight service calls over NATS.

Numeric timeout values in this recipe are seconds.

```julia
using Dates
using Natter

client = connect("nats://127.0.0.1:4222"; name="time-service")

service = subscribe(client, "time.now"; callback_mode=:inline) do msg
    isnothing(msg.reply) && return
    respond(client, msg, string(now(Dates.UTC)))
end

response = request(client, "time.now", ""; timeout=1.0)
println(String(response))

drain(service)
close(client)
```

Handle missing responders explicitly:

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

Fan out independent requests with Julia tasks:

```julia
time_response = Ref{Msg}()
date_response = Ref{Msg}()

@sync begin
    Threads.@spawn time_response[] = request(client, "time.now", ""; timeout=1.0)
    Threads.@spawn date_response[] = request(client, "date.today", ""; timeout=1.0)
end
```
