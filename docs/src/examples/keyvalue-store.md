# KeyValue Store

This example uses a KeyValue bucket with direct reads enabled.

```julia
using Natter

client = connect("nats://127.0.0.1:4222")
js = jetstream(client)

kv = kv_create(js, "profiles";
    history=10,
    ttl=7 * 24 * 60 * 60,
    max_bytes=256 * 1024 * 1024,
    storage=StorageType.FILE,
    direct=true,
    compression=true,
    metadata=Dict("app" => "profiles"),
)

created = kv_create_key(kv, "users.42.name", "Ada")
kv_update(kv, "users.42.name", "Ada Lovelace", created.seq)

current = kv_get(kv, "users.42.name")
@assert current.key == "users.42.name"
@assert String(current.value) == "Ada Lovelace"

for version in kv_history(kv, "users.42.name")
    @info "profile version" revision=version.revision value=String(version.value)
end

watcher = kv_watch(kv; key="users.*.name") do entry
    @info "profile changed" key=entry.key operation=entry.operation value=String(entry.value)
end

kv_put(kv, "users.7.name", "Grace Hopper")
flush(client)

close(watcher)
close(client)
```

`kv_get(kv, key; direct=false)` can force the management API path for troubleshooting or compatibility.

Independent KeyValue reads can run concurrently with Julia tasks:

```julia
name = Ref{KeyValueEntry}()
email = Ref{KeyValueEntry}()

@sync begin
    @async name[] = kv_get(kv, "users.42.name")
    @async email[] = kv_get(kv, "users.42.email")
end
```
