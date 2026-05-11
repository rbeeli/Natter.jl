# KeyValue Store

This example uses a KeyValue bucket with direct reads enabled.

```julia
using Natter

client = connect("nats://127.0.0.1:4222")
js = jetstream(client)

kv = kv_create(js, "profiles";
    history=10,
    storage=StorageType.FILE,
    direct=true,
)

created = kv_create_key(kv, "users.42.name", "Ada")
kv_update(kv, "users.42.name", "Ada Lovelace", created.seq)

current = kv_get(kv, "users.42.name")
@assert String(current.data) == "Ada Lovelace"

for version in kv_history(kv, "users.42.name")
    @info "profile version" value=String(version.data)
end

watcher = kv_watch(kv; key="users.*.name") do msg
    @info "profile changed" subject=msg.subject value=String(msg.data)
end

kv_put(kv, "users.7.name", "Grace Hopper")
flush(client)

close(watcher)
close(client)
```

`kv_get(kv, key; direct=false)` can force the management API path for troubleshooting or compatibility.
