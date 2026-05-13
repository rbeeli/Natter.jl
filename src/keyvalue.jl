EnumX.@enumx KeyValueOperation begin
    PUT
    DELETE
    PURGE
end

struct KeyValue
    js::JetStreamContext
    bucket::String
    stream::String
    prefix::String
    direct::Bool
end
KeyValue(js::JetStreamContext, bucket::String, stream::String, prefix::String) = KeyValue(js, bucket, stream, prefix, false)

struct KeyValueEntry
    bucket::String
    key::String
    value::Vector{UInt8}
    revision::Int
    created::DateTime
    delta::Int
    operation::KeyValueOperation.T
    msg::Msg
end

Base.String(entry::KeyValueEntry) = String(entry.value)

struct KeyValueStatus
    bucket::String
    stream::String
    values::Int
    history::Int
    ttl::Union{Float64,Nothing}
    bytes::Int
    storage::Union{StorageType.T,Nothing}
    replicas::Union{Int,Nothing}
    direct::Bool
    stream_info::StreamInfo
end

_kv_stream(bucket) = "KV_$bucket"
_kv_prefix(bucket) = "\$KV.$bucket."

_validate_kv_bucket(bucket::AbstractString)::String = _validate_api_name("bucket", bucket)
_validate_kv_key(key::AbstractString)::String = _validate_publish_subject(key)
_validate_kv_watch_key(key::AbstractString)::String = _validate_subject(key)

const _KV_INACTIVE_OPERATIONS = ("DEL", "PURGE")

_kv_keys_consumer_config() = ConsumerConfig(deliver_policy=DeliverPolicy.LAST_PER_SUBJECT, headers_only=true)
_kv_key_active(msg::Msg)::Bool = !(header(msg, "KV-Operation") in _KV_INACTIVE_OPERATIONS)

function _kv_key_from_subject(kv::KeyValue, subject::AbstractString)::String
    startswith(subject, kv.prefix) ||
        throw(ProtocolError("message subject does not belong to key-value bucket $(kv.bucket): $subject"))
    String(chop(String(subject); head=length(kv.prefix), tail=0))
end

function _kv_operation(msg::Msg)::KeyValueOperation.T
    op = header(msg, "KV-Operation")
    isnothing(op) && return KeyValueOperation.PUT
    op == "DEL" && return KeyValueOperation.DELETE
    op == "PURGE" && return KeyValueOperation.PURGE
    throw(ProtocolError("unknown key-value operation: $op"))
end

_kv_is_delete_marker(operation::KeyValueOperation.T)::Bool =
    operation in (KeyValueOperation.DELETE, KeyValueOperation.PURGE)

function _kv_entry(kv::KeyValue, msg::Msg, revision::Int, created::DateTime, delta::Int)::KeyValueEntry
    KeyValueEntry(kv.bucket, _kv_key_from_subject(kv, msg.subject), msg.data, revision, created, delta, _kv_operation(msg), msg)
end

function _kv_entry_from_stored_msg(kv::KeyValue, msg::Msg, revision::Int, created::Union{DateTime,Nothing})::KeyValueEntry
    isnothing(created) && throw(ProtocolError("key-value message is missing created timestamp"))
    _kv_entry(kv, msg, revision, created, 0)
end

function _kv_created_from_metadata(meta::MsgMetadata)::DateTime
    DateTime(1970, 1, 1) + Millisecond(meta.timestamp_ns ÷ 1_000_000)
end

function _kv_entry_from_consumer_msg(kv::KeyValue, msg::Msg)::KeyValueEntry
    meta = metadata(msg)
    _kv_entry(kv, msg, meta.stream_sequence, _kv_created_from_metadata(meta), meta.pending)
end

function _kv_record_key!(latest::Dict{String,Tuple{Int,Bool}}, prefix::String, msg::Msg)
    startswith(msg.subject, prefix) ||
        throw(ProtocolError("key-value keys consumer received subject outside bucket prefix"))
    key = String(chop(msg.subject; head=length(prefix), tail=0))
    seq = metadata(msg).stream_sequence
    current_seq = get(latest, key, (0, false))[1]
    seq >= current_seq && (latest[key] = (seq, _kv_key_active(msg)))
    latest
end

_kv_active_keys(latest::Dict{String,Tuple{Int,Bool}})::Vector{String} =
    [key for (key, (_seq, active)) in latest if active]

_kv_not_found_error(kv::KeyValue, key::AbstractString, message::AbstractString="") =
    KeyValueKeyNotFoundError(kv.bucket, String(key), String(message))
_kv_deleted_error(kv::KeyValue, entry::KeyValueEntry) =
    KeyValueKeyDeletedError(kv.bucket, entry.key, entry)
_kv_wrong_revision_error(kv::KeyValue, key::AbstractString, expected_revision::Union{Int,Nothing}, cause::JetStreamError) =
    KeyValueWrongRevisionError(kv.bucket, String(key), expected_revision, cause)
_kv_key_exists_error(kv::KeyValue, key::AbstractString, cause::JetStreamError) =
    KeyValueKeyExistsError(kv.bucket, String(key), cause)

function _kv_state_int(info::StreamInfo, field::String)::Int
    Int(get(info.state, field, 0))
end

function _kv_status(kv::KeyValue, info::StreamInfo)::KeyValueStatus
    KeyValueStatus(
        kv.bucket,
        info.name,
        _kv_state_int(info, "messages"),
        something(info.config.max_msgs_per_subject, 0),
        info.config.max_age,
        _kv_state_int(info, "bytes"),
        info.config.storage,
        info.config.num_replicas,
        something(info.config.allow_direct, false),
        info,
    )
end

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

function kv_status(kv::KeyValue; timeout::Real=kv.js.timeout)
    _kv_status(kv, stream_info(kv.js, kv.stream; timeout))
end

status(kv::KeyValue; kwargs...) = kv_status(kv; kwargs...)

function kv_get(kv::KeyValue, key::AbstractString; revision::Union{Nothing,Int}=nothing, direct::Union{Bool,Nothing}=nothing)
    key = _validate_kv_key(key)
    subject = "$(kv.prefix)$key"
    use_direct = isnothing(direct) ? kv.direct : direct
    msg, sequence, created = try
        req = isnothing(revision) ?
              _stream_message_get_request(nothing, subject, false) :
              _stream_message_get_request(revision, nothing, false)
        _stream_message_get_info(kv.js, kv.stream, req; direct=use_direct, timeout=kv.js.timeout)
    catch err
        err isa JetStreamError && err.code == 404 ? throw(_kv_not_found_error(kv, key)) : rethrow()
    end
    msg.subject == subject ||
        throw(_kv_not_found_error(kv, key, "expected subject $subject, got $(msg.subject)"))
    entry = _kv_entry_from_stored_msg(kv, msg, sequence, created)
    _kv_is_delete_marker(entry.operation) && throw(_kv_deleted_error(kv, entry))
    entry
end

function kv_put(kv::KeyValue, key::AbstractString, value; revision::Union{Nothing,Int}=nothing)
    key = _validate_kv_key(key)
    hdrs = Headers()
    isnothing(revision) || push!(get!(hdrs, "Nats-Expected-Last-Subject-Sequence", String[]), string(revision))
    try
        js_publish(kv.js, "$(kv.prefix)$key", value; headers=hdrs)
    catch err
        _kv_wrong_last_sequence(err) && throw(_kv_wrong_revision_error(kv, key, revision, err))
        rethrow()
    end
end

_kv_wrong_last_sequence(err) = err isa JetStreamError && err.err_code == 10071
_kv_delete_marker_revision(msg::Msg, sequence::Int) = header(msg, "KV-Operation") in ("DEL", "PURGE") ? sequence : nothing

function _kv_latest_delete_marker_revision(kv::KeyValue, subject::String)
    req = _stream_message_get_request(nothing, subject, false)
    msg, sequence, _created = try
        _stream_message_get_api(kv.js, kv.stream, req; timeout=kv.js.timeout)
    catch err
        err isa JetStreamError && err.code == 404 && return nothing
        rethrow()
    end
    msg.subject == subject || return nothing
    _kv_delete_marker_revision(msg, sequence)
end

function kv_create_key(kv::KeyValue, key::AbstractString, value)
    key = _validate_kv_key(key)
    subject = "$(kv.prefix)$key"
    hdrs = Headers("Nats-Expected-Last-Subject-Sequence" => ["0"])
    try
        return js_publish(kv.js, subject, value; headers=hdrs)
    catch err
        _kv_wrong_last_sequence(err) || rethrow()
        marker_revision = _kv_latest_delete_marker_revision(kv, subject)
        isnothing(marker_revision) && throw(_kv_key_exists_error(kv, key, err))
        retry_hdrs = Headers("Nats-Expected-Last-Subject-Sequence" => [string(marker_revision)])
        try
            return js_publish(kv.js, subject, value; headers=retry_hdrs)
        catch retry_err
            _kv_wrong_last_sequence(retry_err) &&
                throw(_kv_wrong_revision_error(kv, key, marker_revision, retry_err))
            rethrow()
        end
    end
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
    entries = KeyValueEntry[]
    try
        while true
            chunk = fetch(sub, batch; timeout=kv.js.timeout)
            isempty(chunk) && break
            for msg in chunk
                push!(entries, _kv_entry_from_consumer_msg(kv, msg))
            end
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
    entries
end

function kv_keys(kv::KeyValue)
    sub = pull_subscribe(kv.js, "$(kv.prefix)>"; stream=kv.stream,
                         config=_kv_keys_consumer_config())
    latest = Dict{String,Tuple{Int,Bool}}()
    try
        while true
            chunk = fetch(sub, 256; timeout=kv.js.timeout)
            isempty(chunk) && break
            for msg in chunk
                _kv_record_key!(latest, kv.prefix, msg)
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
    _kv_active_keys(latest)
end

function kv_watch(callback::Function, kv::KeyValue; key::String=">", history::Bool=false)
    key = _validate_kv_watch_key(key)
    policy = history ? DeliverPolicy.ALL : DeliverPolicy.LAST_PER_SUBJECT
    entry_callback = msg -> callback(_kv_entry_from_consumer_msg(kv, msg))
    push_subscribe(kv.js, "$(kv.prefix)$key"; stream=kv.stream, callback=entry_callback, manual_ack=true,
                   config=ConsumerConfig(deliver_policy=policy, ack_policy=AckPolicy.NONE))
end
