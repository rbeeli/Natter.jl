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
    storage=StorageType.FILE,
    replicas=1,
    direct=true,
)
```

Open an existing bucket:

```julia
kv = kv_open(js, "settings")
```

When a bucket is created or opened with direct access enabled on its backing stream, `kv_get` uses direct reads by default. Override that per call with `direct=false` or `direct=true`.

## Put And Get

```julia
ack = kv_put(kv, "theme", "dark")
msg = kv_get(kv, "theme")

@assert String(msg.data) == "dark"
```

Create only if the key does not already exist:

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
kv_delete(kv, "theme")
kv_purge(kv, "theme")
```

Deleted and purged keys raise `KeyError` from `kv_get`.

## History And Keys

```julia
for msg in kv_history(kv, "theme")
    println(String(msg.data))
end

for key in kv_keys(kv)
    println(key)
end
```

## Watch

`kv_watch` returns a push subscription. Close it when the watcher is no longer needed.

```julia
watcher = kv_watch(kv; key=">", history=false) do msg
    @info "key changed" subject=msg.subject value=String(msg.data)
end

close(watcher)
```

## Delete A Bucket

```julia
kv_delete_bucket(kv)
```
