# Production Client

This example collects common production options in one place.

```julia
using Natter

function make_client()
    connect([
        "tls://nats-a.internal:4222",
        "tls://nats-b.internal:4222",
        "tls://nats-c.internal:4222",
    ];
        name="billing-worker",
        connect_timeout=2.0,
        ping_interval=30.0,
        max_outstanding_pings=2,
        allow_reconnect=true,
        reconnect_wait=0.25,
        reconnect_max_wait=5.0,
        reconnect_jitter=0.2,
        max_reconnect_attempts=-1,
        pending_size=16 * 1024 * 1024,
        write_timeout=5.0,
        sub_pending_msgs_limit=8192,
        sub_pending_bytes_limit=128 * 1024 * 1024,
        tls_ca_path="/etc/nats/ca.pem",
        tls_cert_path="/etc/nats/client.pem",
        tls_key_path="/etc/nats/client-key.pem",
        event_cb=event -> begin
            if event.kind == ConnectionEventKind.DISCONNECTED
                @warn "NATS disconnected" error=event.error
            elseif event.kind == ConnectionEventKind.RECONNECTED
                @info "NATS reconnected" url=event.url attempt=event.attempt
            elseif event.kind == ConnectionEventKind.CLOSED
                @info "NATS connection closed"
            end
        end,
        error_cb=err -> begin
            @error "NATS client error" exception=err
        end,
    )
end

client = make_client()
js = jetstream(client; timeout=5.0)

stream_create(js, StreamConfig(
    name="BILLING",
    subjects=["billing.events.*"],
    storage=StorageType.FILE,
    max_msgs=-1,
    max_bytes=50 * 1024 * 1024 * 1024,
    duplicate_window=120.0,
    allow_direct=true,
))

worker = pull_subscribe(js, "billing.events.created";
    stream="BILLING",
    durable="billing-created-workers",
    timeout=5.0,
    config=ConsumerConfig(
        ack_policy=AckPolicy.EXPLICIT,
        ack_wait=60.0,
        max_ack_pending=500,
    ),
)

try
    for msg in fetch(worker, 50; timeout=2.0)
        try
            handle_billing_event(String(msg.data))
            ack(msg)
        catch err
            @error "billing event failed" exception=err
            nak(msg; delay=5.0)
        end
    end
finally
    close(worker)
    drain(client; timeout=10.0)
end
```

For exactly-once effects, combine JetStream message IDs with idempotent application storage. Reconnect publish replay protects availability, but duplicate delivery is still possible after ambiguous network failures.

For CPU-light, I/O-heavy handlers, process a fetched batch concurrently with Julia tasks:

```julia
try
    msgs = fetch(worker, 50; timeout=2.0)

    @sync for msg in msgs
        @async begin
            try
                handle_billing_event(String(msg.data))
                ack(msg)
            catch err
                @error "billing event failed" exception=err
                nak(msg; delay=5.0)
            end
        end
    end
finally
    close(worker)
    drain(client; timeout=10.0)
end
```
