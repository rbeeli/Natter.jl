# Reliability And TLS

Natter.jl uses Julia tasks for connection readers, ping timers, reconnects, subscription callbacks, and explicit async handles. Application code can usually call Natter directly inside web handlers, workers, and other Julia tasks; use `@sync` and `@async` when independent work should run concurrently.

## Automatic Reconnect

Reconnect is enabled by default. After a transient disconnect, the client:

- marks the connection as `ConnectionStatus.RECONNECTING`;
- reconnects to known and discovered servers;
- replays active subscriptions;
- flushes buffered publish commands on a best-effort basis;
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
    disconnected_cb=() -> begin
        @warn "NATS disconnected"
    end,
    reconnected_cb=() -> begin
        @info "NATS reconnected"
    end,
    error_cb=err -> begin
        @error "NATS client error" exception=err
    end,
)
```

`max_reconnect_attempts=-1` means unlimited reconnect attempts.

Reconnect coverage currently includes same-server reconnect and multi-URL failover in real-server tests. `INFO` `connect_urls` updates add new discovered routes and prune stale discovered routes from the reconnect pool. Multi-node cluster chaos and auth failover are still tracked as partial hardening work in the feature coverage matrix.

## Publish Buffering

Core publishes retained for reconnect replay are buffered up to `pending_size`. This includes publishes made while reconnecting and buffered connected publishes that have not yet been flushed successfully. Treat reconnect publish replay as best-effort, at-least-once behavior for retained frames, not exactly-once delivery. If the transport fails after the server has accepted some bytes but before the client observes success, replay can duplicate delivery.

Publish frames at or above `write_buffer_size` bypass the buffered replay capture path. If such a direct write fails inside the publish call, Natter can requeue the frame for reconnect; after a successful direct socket write and flush, the frame is no longer retained for later reconnect replay. Use JetStream `js_publish` with `msg_id` and an appropriate stream `duplicate_window`, plus idempotent application storage, when duplicate effects are unacceptable.

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

Client certificate authentication requires both `tls_cert_path` and `tls_key_path`.

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

`close(client)` reports cleanup failures through `error_cb` and returns. It waits up to `close_callback_timeout` for active subscription callbacks and reports a cleanup timeout if they keep running; it does not interrupt user callback code. Use `drain` before `close` when shutdown must wait for queued callback work. Use `close(client; throw_errors=true)` in tests or strict shutdown code.

```julia
try
    close(client; throw_errors=true)
catch err
    @error "client cleanup failed" exception=err
end
```
