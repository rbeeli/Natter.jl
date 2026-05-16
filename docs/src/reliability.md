# Reliability And TLS

Natter.jl is designed for long-running clients. Reconnect is enabled by default, active subscriptions are replayed after reconnect, and publish buffering is bounded by `pending_size`.

## Production Connection

```julia
client = connect([
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
    record_stats=true,
    sub_pending_msgs_limit=8192,
    sub_pending_bytes_limit=128 * 1024 * 1024,
    tls_ca_path="/etc/nats/ca.pem",
    auth=CredentialsAuth(; path="/etc/nats/user.creds"),
    event_cb=event -> begin
        if event.kind == ConnectionEventKind.DISCONNECTED
            @warn "NATS disconnected" error=event.error
        elseif event.kind == ConnectionEventKind.RECONNECTED
            @info "NATS reconnected" url=event.url attempt=event.attempt
        end
    end,
    error_cb=err -> @error "NATS client error" exception=err,
)
```

`max_reconnect_attempts=-1` means unlimited reconnect attempts.

## Reconnect Semantics

After a transient disconnect the client:

- marks the connection as `ConnectionStatus.RECONNECTING`;
- tries configured and server-discovered URLs;
- replays active subscriptions;
- flushes buffered publishes up to `pending_size`;
- emits `ConnectionEvent` values through `event_cb`.

Core publish replay is best-effort and should be treated as at-least-once for retained frames. If duplicate effects matter, use JetStream `js_publish(...; msg_id=...)` and idempotent application storage.

Customize reconnect delay when needed:

```julia
client = connect("nats://nats.internal:4222";
    reconnect_delay_cb=event -> min(30.0, 0.25 * event.attempt),
)
```

## Subscription Backpressure

Each subscription has message and byte pending limits. When a subscription cannot keep up, Natter drops the message and reports `SlowConsumerError` through `error_cb`.

```julia
sub = subscribe(client, "metrics.>";
    pending_msgs_limit=10_000,
    pending_bytes_limit=256 * 1024 * 1024,
)
```

For high-volume durable work, prefer JetStream pull consumers so the worker controls fetch size and acknowledgement.

## TLS

Use `tls://` for TLS-first servers:

```julia
client = connect("tls://nats.example.com:4222";
    tls_ca_path="/etc/nats/ca.pem",
)
```

Client certificates require both cert and key paths:

```julia
client = connect("tls://nats.example.com:4222";
    tls_ca_path="/etc/nats/ca.pem",
    tls_cert_path="/etc/nats/client.pem",
    tls_key_path="/etc/nats/client-key.pem",
)
```

Use `tls_server_name` when connecting to an address but verifying a DNS certificate name:

```julia
client = connect("tls://10.0.0.5:4222";
    tls_ca_path="/etc/nats/ca.pem",
    tls_server_name="nats.example.com",
)
```

Certificate verification is enabled by default. Disable it only in controlled development or test environments:

```julia
client = connect("tls://127.0.0.1:4222"; tls_verify=false)
```

For deployments that read server `INFO` before upgrading to TLS, use `tls_required=true` on a `nats://` URL or `tls_first=false` on a `tls://` URL.

## Authentication

Use one NATS authentication scheme per connection:

```julia
token_client = connect("nats://nats.example.com:4222";
    auth=TokenAuth(ENV["NATS_TOKEN"]),
)

user_client = connect("nats://nats.example.com:4222";
    auth=UserPassAuth(ENV["NATS_USER"], ENV["NATS_PASSWORD"]),
)

creds_client = connect("tls://nats.example.com:4222";
    tls_ca_path="/etc/nats/ca.pem",
    auth=CredentialsAuth(; path="/etc/nats/user.creds"),
)
```

NKEY and JWT auth are also supported:

```julia
nkey_client = connect("nats://nats.example.com:4222";
    auth=NKeyAuth(; seed_path="/etc/nats/user.nk"),
)

jwt_client = connect("nats://nats.example.com:4222";
    auth=JwtAuth(; jwt_path="/etc/nats/user.jwt", seed_path="/etc/nats/user.nk"),
)
```

Use `CallbackAuth` when credentials are selected dynamically after server `INFO` is available.

```julia
client = connect("nats://nats.example.com:4222";
    auth=CallbackAuth(req -> TokenAuth(token_for(req.url))),
)
```

## Cleanup

Use `drain` for graceful service shutdown:

```julia
try
    drain(client; timeout=10.0)
catch err
    @error "NATS drain failed" exception=err
    close(client)
end
```

Use `close(client; throw_errors=true)` in tests or strict shutdown paths where cleanup failures must fail the caller.
