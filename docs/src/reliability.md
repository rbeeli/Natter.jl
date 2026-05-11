# Reliability And TLS

Natter.jl is synchronous at the public API boundary and task-based internally. Connection readers, ping timers, reconnects, and subscription callbacks run in background tasks owned by the client.

## Automatic Reconnect

Reconnect is enabled by default. After a transient disconnect, the client:

- marks the connection as `ConnectionStatus.RECONNECTING`;
- reconnects to known and discovered servers;
- replays active subscriptions;
- flushes buffered publish commands;
- invokes `disconnected_cb` and `reconnected_cb` callbacks.

```julia
client = connect([
    "nats://nats-a.internal:4222",
    "nats://nats-b.internal:4222",
];
    reconnect_wait=0.25,
    reconnect_max_wait=5.0,
    reconnect_jitter=0.2,
    max_reconnect_attempts=-1,
    pending_size=8 * 1024 * 1024,
    disconnected_cb=() -> @warn("NATS disconnected"),
    reconnected_cb=() -> @info("NATS reconnected"),
    error_cb=err -> @error("NATS client error" exception=err),
)
```

`max_reconnect_attempts=-1` means unlimited reconnect attempts.

## Publish Buffering

Core publishes made while reconnecting are buffered up to `pending_size`. If the transport fails after the server has accepted some bytes but before the client observes success, replay can duplicate delivery. Use idempotent consumers, application message IDs, or JetStream publish expectations when duplicate effects are unacceptable.

## Subscription Backpressure

Each subscription has message and byte pending limits. When those limits are exceeded, the message is dropped and a `SlowConsumerError` is reported through `error_cb`.

```julia
sub = subscribe(client, "metrics.>";
    pending_msgs_limit=10_000,
    pending_bytes_limit=256 * 1024 * 1024,
)
```

For high-volume services, prefer bounded work queues in the callback or pull-based JetStream consumers.

## TLS

Use `tls://` URLs for TLS-first servers:

```julia
client = connect("tls://nats.example.com:4222";
    tls_ca_path="/etc/ssl/certs/ca.pem",
    tls_cert_path="/etc/nats/client.pem",
    tls_key_path="/etc/nats/client-key.pem",
)
```

For deployments that require the server `INFO` line before upgrading to TLS, pass `tls_first=false`.

```julia
client = connect("tls://nats.example.com:4222"; tls_first=false)
```

Certificate verification is enabled by default. Disable it only for trusted environments where verification is intentionally handled elsewhere:

```julia
client = connect("tls://127.0.0.1:4222"; tls_verify=false)
```

`tls_required=true` requests TLS even when the URL scheme is `nats://`.

## Cleanup

`close(client)` reports cleanup failures through `error_cb` and returns. Use `close(client; throw_errors=true)` in tests or strict shutdown code.

```julia
try
    close(client; throw_errors=true)
catch err
    @error "client cleanup failed" exception=err
end
```
