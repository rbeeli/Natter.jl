# KeyValue

KeyValue buckets are built on JetStream streams. They are useful for configuration, profiles, feature flags, small state records, and watched state.

```julia
using Natter
using Natter.JetStream
using Natter.KeyValue

client = connect("nats://127.0.0.1:4222")
js = jetstream(client)
```

## Create Or Open A Bucket

```julia
kv = kv_create(js, "settings";
    history=5,
    ttl=24 * 60 * 60,
    max_bytes=128 * 1024 * 1024,
    max_value_size=1 * 1024 * 1024,
    storage=StorageType.FILE,
    replicas=1,
    direct=true,
    metadata=Dict("owner" => "config-service"),
)
```

Open an existing bucket:

```julia
kv = kv_open(js, "settings")
```

Inspect bucket state:

```julia
st = kv_status(kv)
@info "bucket" values=st.values history=st.history bytes=st.bytes
```

Durations are seconds. `history` must be between 1 and 64. Buckets created with `direct=true` use direct reads by default when the server supports them.

## Put, Get, And Update

```julia
revision = kv_put(kv, "checkout.currency", "CHF")
entry = kv_get(kv, "checkout.currency"; timeout=2.0)

@assert entry.key == "checkout.currency"
@assert entry.revision == revision
@assert entry.operation == KeyValueOperation.PUT
@assert String(entry) == "CHF"
```

Create a key only when it is absent or currently deleted:

```julia
created = kv_create_key(kv, "checkout.enabled", "true")
```

Update only when the key is still at a known revision:

```julia
current = kv_get(kv, "checkout.currency")
new_revision = kv_update(kv, "checkout.currency", "EUR", current.revision)
```

`kv_put(kv, key, value; revision=rev)` is the same guarded write pattern. Per-key TTL is available with `ttl=...` when the bucket is configured for message TTL markers.

## Delete And Purge

```julia
latest = kv_get(kv, "checkout.currency")
kv_delete(kv, "checkout.currency"; revision=latest.revision)

kv_purge(kv, "checkout.currency")
```

`kv_delete` leaves a delete marker. `kv_purge` removes prior values for that key and leaves a purge marker.

Common KeyValue errors are typed:

```julia
try
    kv_update(kv, "checkout.currency", "USD", 12)
catch err
    if err isa KeyValueWrongRevisionError
        @warn "settings changed; reload and retry"
    else
        rethrow()
    end
end
```

`kv_get` raises `KeyValueKeyNotFoundError` for absent keys and `KeyValueKeyDeletedError` for deleted or purged keys.

## History And Keys

```julia
for entry in kv_history(kv, "checkout.currency")
    @info "version" revision=entry.revision operation=entry.operation value=String(entry)
end

for key in kv_keys(kv)
    println(key)
end
```

Remove old delete and purge markers:

```julia
kv_purge_deletes(kv; older_than=30 * 60)
```

Use `older_than=-1` to remove all delete and purge markers regardless of age.

## Watch Changes

Channel watchers yield historical/current entries first, then the sentinel `KV_WATCH_INITIAL_DONE`, then live updates.

```julia
watcher = kv_watch(kv; keys=["checkout.*", "system.*"])

try
    while true
        update = take!(watcher)
        update === KV_WATCH_INITIAL_DONE && break
        @info "initial setting" key=update.key revision=update.revision
    end

    Threads.@spawn begin
        for update in watcher.updates
            @info "setting changed" key=update.key operation=update.operation
        end
    end
finally
    close(watcher)
end
```

Use the callback form when a channel is not needed:

```julia
watcher = kv_watch(kv; key="checkout.*", ignore_deletes=true) do entry
    @info "setting changed" key=entry.key value=String(entry)
end

close(watcher)
```

Useful watch options:

| Option | Use |
| :--- | :--- |
| `updates_only=true` | Skip the initial snapshot. |
| `history=true` | Include historical revisions. |
| `ignore_deletes=true` | Skip delete and purge markers. |
| `meta_only=true` | Receive metadata without values. |
| `resume_revision=rev` | Resume after a known stream revision. |

## Concurrent Reads

Use Julia tasks for independent KeyValue operations:

```julia
profile = Ref{KeyValueEntry}()
settings = Ref{KeyValueEntry}()

@sync begin
    Threads.@spawn profile[] = kv_get(kv, "users.42.profile")
    Threads.@spawn settings[] = kv_get(kv, "users.42.settings")
end
```

## Delete A Bucket

```julia
kv_delete_bucket(kv)
```
