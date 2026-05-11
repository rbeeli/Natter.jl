struct KeyValue
    js::JetStreamContext
    bucket::String
    stream::String
    prefix::String
    direct::Bool
end
KeyValue(js::JetStreamContext, bucket::String, stream::String, prefix::String) = KeyValue(js, bucket, stream, prefix, false)

_kv_stream(bucket) = "KV_$bucket"
_kv_prefix(bucket) = "\$KV.$bucket."

_validate_kv_bucket(bucket::AbstractString)::String = _validate_api_name("bucket", bucket)
_validate_kv_key(key::AbstractString)::String = _validate_publish_subject(key)
_validate_kv_watch_key(key::AbstractString)::String = _validate_subject(key)

function kv_create(js::JetStreamContext, bucket::AbstractString; history::Int=1,
                   storage::Union{String,StorageType.T}="file", replicas::Int=1, direct::Bool=false)
    bucket = _validate_kv_bucket(bucket)
    cfg = StreamConfig(
        name=_kv_stream(bucket),
        subjects=["\$KV.$bucket.>"],
        max_msgs_per_subject=history,
        max_msgs=-1,
        max_bytes=-1,
        max_consumers=-1,
        storage=_parse_storage_type(storage),
        num_replicas=replicas,
        allow_rollup_hdrs=true,
        allow_direct=direct,
        deny_delete=true,
        discard=DiscardPolicy.NEW,
    )
    info = stream_create(js, cfg)
    KeyValue(js, bucket, _kv_stream(bucket), _kv_prefix(bucket), something(info.config.allow_direct, false))
end

function kv_open(js::JetStreamContext, bucket::AbstractString)
    bucket = _validate_kv_bucket(bucket)
    info = stream_info(js, _kv_stream(bucket))
    KeyValue(js, bucket, info.name, _kv_prefix(bucket), something(info.config.allow_direct, false))
end

kv_delete_bucket(kv::KeyValue) = stream_delete(kv.js, kv.stream)

function kv_get(kv::KeyValue, key::AbstractString; revision::Union{Nothing,Int}=nothing, direct::Union{Bool,Nothing}=nothing)
    key = _validate_kv_key(key)
    subject = "$(kv.prefix)$key"
    use_direct = isnothing(direct) ? kv.direct : direct
    msg = try
        isnothing(revision) ?
        stream_message_get(kv.js, kv.stream; subject, direct=use_direct) :
        stream_message_get(kv.js, kv.stream; seq=revision, direct=use_direct)
    catch err
        err isa JetStreamError && err.code == 404 ? throw(KeyError(String(key))) : rethrow()
    end
    msg.subject == subject || throw(KeyError(String(key)))
    op = header(msg, "KV-Operation")
    op in ("DEL", "PURGE") && throw(KeyError(String(key)))
    msg
end

function kv_put(kv::KeyValue, key::AbstractString, value; revision::Union{Nothing,Int}=nothing)
    key = _validate_kv_key(key)
    hdrs = Headers()
    isnothing(revision) || push!(get!(hdrs, "Nats-Expected-Last-Subject-Sequence", String[]), string(revision))
    js_publish(kv.js, "$(kv.prefix)$key", value; headers=hdrs)
end

function kv_create_key(kv::KeyValue, key::AbstractString, value)
    key = _validate_kv_key(key)
    hdrs = Headers("Nats-Expected-Last-Subject-Sequence" => ["0"])
    js_publish(kv.js, "$(kv.prefix)$key", value; headers=hdrs)
end

kv_update(kv::KeyValue, key::AbstractString, value, revision::Int) = kv_put(kv, key, value; revision)

function kv_delete(kv::KeyValue, key::AbstractString)
    key = _validate_kv_key(key)
    hdrs = Headers("KV-Operation" => ["DEL"])
    js_publish(kv.js, "$(kv.prefix)$key", UInt8[]; headers=hdrs)
end

function kv_purge(kv::KeyValue, key::AbstractString)
    key = _validate_kv_key(key)
    hdrs = Headers("KV-Operation" => ["PURGE"], "Nats-Rollup" => ["sub"])
    js_publish(kv.js, "$(kv.prefix)$key", UInt8[]; headers=hdrs)
end

function kv_history(kv::KeyValue, key::AbstractString; batch::Int=256)
    key = _validate_kv_key(key)
    sub = pull_subscribe(kv.js, "$(kv.prefix)$key"; stream=kv.stream,
                         config=ConsumerConfig(deliver_policy=DeliverPolicy.ALL))
    msgs = Msg[]
    try
        while true
            chunk = fetch(sub, batch; timeout=kv.js.timeout)
            isempty(chunk) && break
            append!(msgs, chunk)
            length(chunk) < batch && break
        end
    catch err
        try
            close(sub)
        catch cleanup_err
            throw(Base.CompositeException([err, CleanupError("close key-value history consumer", cleanup_err)]))
        end
        rethrow()
    end
    close(sub)
    msgs
end

function kv_keys(kv::KeyValue)
    sub = pull_subscribe(kv.js, "$(kv.prefix)>"; stream=kv.stream,
                         config=ConsumerConfig(deliver_policy=DeliverPolicy.LAST_PER_SUBJECT))
    latest = Dict{String,Msg}()
    try
        while true
            chunk = fetch(sub, 256; timeout=kv.js.timeout)
            isempty(chunk) && break
            for msg in chunk
                latest[msg.subject] = msg
            end
            length(chunk) < 256 && break
        end
    catch err
        try
            close(sub)
        catch cleanup_err
            throw(Base.CompositeException([err, CleanupError("close key-value keys consumer", cleanup_err)]))
        end
        rethrow()
    end
    close(sub)
    [String(chop(msg.subject; head=length(kv.prefix), tail=0)) for msg in values(latest) if !(header(msg, "KV-Operation") in ("DEL", "PURGE"))]
end

function kv_watch(callback::Function, kv::KeyValue; key::String=">", history::Bool=false)
    key = _validate_kv_watch_key(key)
    policy = history ? DeliverPolicy.ALL : DeliverPolicy.LAST_PER_SUBJECT
    push_subscribe(kv.js, "$(kv.prefix)$key"; stream=kv.stream, callback=callback, manual_ack=true,
                   config=ConsumerConfig(deliver_policy=policy, ack_policy=AckPolicy.NONE))
end
