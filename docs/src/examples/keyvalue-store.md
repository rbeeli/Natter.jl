# KeyValue Store

This recipe uses a KeyValue bucket with direct reads and optimistic writes.

```julia
using Natter
using Natter.JetStream
using Natter.KeyValue

client = connect("nats://127.0.0.1:4222")
js = jetstream(client)

kv = kv_create(js, "profiles";
    history=10,
    ttl=7 * 24 * 60 * 60,
    max_bytes=256 * 1024 * 1024,
    storage=StorageType.FILE,
    direct=true,
    metadata=Dict("app" => "profiles"),
)

created_revision = kv_create_key(kv, "users.42.name", "Ada")
updated_revision = kv_update(kv, "users.42.name", "Ada Lovelace", created_revision)

current = kv_get(kv, "users.42.name"; timeout=2.0)
@assert current.revision == updated_revision
@assert String(current) == "Ada Lovelace"
```

Read history and active keys:

```julia
for version in kv_history(kv, "users.42.name")
    @info "profile version" revision=version.revision value=String(version)
end

for key in kv_keys(kv)
    @info "profile key" key
end
```

Watch a subset of keys:

```julia
watcher = kv_watch(kv; key="users.*.name") do entry
    @info "profile changed" key=entry.key operation=entry.operation value=String(entry)
end

kv_put(kv, "users.7.name", "Grace Hopper")
flush(client)

close(watcher)
close(client)
```

Handle optimistic-write conflicts:

```julia
try
    kv_update(kv, "users.42.name", "Ada", 1)
catch err
    if err isa KeyValueWrongRevisionError
        @warn "profile changed; reload and retry"
    else
        rethrow()
    end
end
```

Run independent reads concurrently with Julia tasks:

```julia
name = Ref{KeyValueEntry}()
email = Ref{KeyValueEntry}()

@sync begin
    Threads.@spawn name[] = kv_get(kv, "users.42.name")
    Threads.@spawn email[] = kv_get(kv, "users.42.email")
end
```
