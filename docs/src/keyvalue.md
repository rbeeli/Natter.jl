# KeyValue

KeyValue buckets are built on JetStream streams and use the `JetStreamContext` API.

```julia
client = connect("nats://127.0.0.1:4222")
js = jetstream(client)
```

## Create Or Open A Bucket

```julia
kv = kv_create(js, "settings";
    history=5,
    ttl=3600.0,
    max_bytes=128 * 1024 * 1024,
    max_value_size=1 * 1024 * 1024,
    storage=StorageType.FILE,
    replicas=1,
    direct=true,
    compression=true,
    metadata=Dict("owner" => "config-service"),
    limit_marker_ttl=86400.0,
)
```

Durations are expressed in seconds. `history` must be between 1 and 64. `limit_marker_ttl` enables the backing stream support needed for expiring delete markers and per-message TTL markers.

Open an existing bucket:

```julia
kv = kv_open(js, "settings")
```

When a bucket is created or opened with direct access enabled on its backing stream, `kv_get` uses direct reads by default. Override that per call with `direct=false` or `direct=true`.

Inspect bucket state with `kv_status`:

```julia
st = kv_status(kv)
@assert st.history == 5
@assert st.direct == true
```

## Put And Get

```julia
ack = kv_put(kv, "theme", "dark")
entry = kv_get(kv, "theme")

@assert entry.key == "theme"
@assert entry.revision == ack.seq
@assert entry.operation == KeyValueOperation.PUT
@assert String(entry.value) == "dark"
```

Create only if the key does not currently exist. A deleted key can be created again:

```julia
kv_create_key(kv, "first-run", "complete")
```

Update only if the key is still at a known revision:

```julia
ack = kv_put(kv, "theme", "dark")
kv_update(kv, "theme", "light", ack.seq)
```

## Delete And Purge

```julia
latest = kv_get(kv, "theme")
kv_delete(kv, "theme"; revision=latest.revision)
kv_purge(kv, "theme")
```

Absent keys raise `KeyValueKeyNotFoundError` from `kv_get`. Deleted and purged keys raise `KeyValueKeyDeletedError` with the tombstone entry attached.

Optimistic operations raise KV-specific errors. `kv_update`, `kv_put(...; revision=rev)`, `kv_delete(...; revision=rev)`, and `kv_purge(...; revision=rev)` raise `KeyValueWrongRevisionError` when the expected revision does not match. `kv_create_key` raises `KeyValueKeyExistsError` when the key is already active.

## History And Keys

```julia
for entry in kv_history(kv, "theme")
    println(entry.revision, ": ", String(entry.value))
end

for key in kv_keys(kv)
    println(key)
end
```

## Concurrent KeyValue Work

Use Julia tasks for independent KeyValue operations:

```julia
profile = Ref{KeyValueEntry}()
settings = Ref{KeyValueEntry}()

@sync begin
    @async profile[] = kv_get(kv, "users.42.profile")
    @async settings[] = kv_get(kv, "users.42.settings")
end
```

## Watch

`kv_watch` returns a push subscription. Close it when the watcher is no longer needed.

```julia
watcher = kv_watch(kv; key=">", history=false) do entry
    @info "key changed" key=entry.key revision=entry.revision value=String(entry.value)
end

close(watcher)
```

## Delete A Bucket

```julia
kv_delete_bucket(kv)
```
