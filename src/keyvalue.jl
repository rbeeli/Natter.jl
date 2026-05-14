EnumX.@enumx KeyValueOperation begin
    PUT
    DELETE
    PURGE
end

struct KeyValue{J<:JetStreamContext}
    js::J
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
end

Base.String(entry::KeyValueEntry) = _bytes_to_string(entry.value)

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

struct KeyValueWatchInitialDone end

const KV_WATCH_INITIAL_DONE = KeyValueWatchInitialDone()
const _KeyValueWatchUpdate = Union{KeyValueEntry,KeyValueWatchInitialDone}

mutable struct _KeyValueWatcherState
    updates::Union{Channel{_KeyValueWatchUpdate},Nothing}
    callback::Union{Function,Nothing}
    notify_initial_done::Bool
    lock::ReentrantLock
    closed::Bool
    initial_done::Bool
    initial_pending::Int
    initial_received::Int
end

mutable struct KeyValueWatcher{S<:PushSubscription}
    subscription::S
    updates::Union{Channel{_KeyValueWatchUpdate},Nothing}
    state::_KeyValueWatcherState
end

_kv_stream(bucket) = "KV_$bucket"
_kv_prefix(bucket) = "\$KV.$bucket."

_validate_kv_bucket(bucket::AbstractString)::String = _validate_api_name("bucket", bucket)
_validate_kv_key(key::AbstractString)::String = _validate_publish_subject(key)
_validate_kv_watch_key(key::AbstractString)::String = _validate_subject(key)

const _KV_MAX_HISTORY = 64
const _KV_EXPECTED_LAST_SUBJECT_SEQUENCE = "Nats-Expected-Last-Subject-Sequence"
const _KV_MARKER_REASON = "Nats-Marker-Reason"

_kv_keys_consumer_config() = ConsumerConfig(deliver_policy=DeliverPolicy.LAST_PER_SUBJECT, headers_only=true)

function _validate_kv_history(history::Integer)::Int
    1 <= history <= _KV_MAX_HISTORY ||
        throw(ArgumentError("key-value history must be between 1 and $_KV_MAX_HISTORY"))
    Int(history)
end

function _validate_kv_limit(name::AbstractString, value::Integer)::Int
    value >= -1 || throw(ArgumentError("$name must be -1 or non-negative"))
    Int(value)
end

function _kv_optional_seconds(name::AbstractString, value::Union{Real,Nothing}; allow_zero::Bool=true)
    isnothing(value) && return nothing
    seconds = Float64(value)
    isfinite(seconds) || throw(ArgumentError("$name must be finite"))
    if allow_zero
        seconds >= 0 || throw(ArgumentError("$name must be non-negative"))
    else
        seconds > 0 || throw(ArgumentError("$name must be positive"))
    end
    seconds
end

function _parse_kv_compression(value)
    isnothing(value) && return nothing
    value isa Bool && return value ? StoreCompression.S2 : nothing
    value isa StoreCompression.T && return value
    _parse_store_compression(value)
end

_kv_metadata(::Nothing) = nothing
_kv_metadata(value) = _string_dict(value, "metadata")

_kv_expected_revision(::Nothing) = nothing

function _kv_expected_revision(revision::Integer)::Int
    revision >= 0 || throw(ArgumentError("revision must be non-negative"))
    Int(revision)
end

function _kv_add_expected_revision!(hdrs::Headers, revision::Union{Integer,Nothing})::Union{Int,Nothing}
    expected = _kv_expected_revision(revision)
    isnothing(expected) || push!(get!(hdrs, _KV_EXPECTED_LAST_SUBJECT_SEQUENCE, String[]), string(expected))
    expected
end

function _kv_key_from_subject(kv::KeyValue, subject::AbstractString)::String
    startswith(subject, kv.prefix) ||
        throw(ProtocolError("message subject does not belong to key-value bucket $(kv.bucket): $subject"))
    String(chop(String(subject); head=length(kv.prefix), tail=0))
end

function _kv_marker_operation(msg::AbstractMsg)
    reason = header(msg, _KV_MARKER_REASON)
    isnothing(reason) && return nothing
    reason in ("MaxAge", "Purge") && return KeyValueOperation.PURGE
    reason == "Remove" && return KeyValueOperation.DELETE
    nothing
end

function _kv_operation(msg::AbstractMsg)::KeyValueOperation.T
    op = header(msg, "KV-Operation")
    if isnothing(op)
        marker_op = _kv_marker_operation(msg)
        isnothing(marker_op) && return KeyValueOperation.PUT
        return marker_op
    end
    op == "DEL" && return KeyValueOperation.DELETE
    op == "PURGE" && return KeyValueOperation.PURGE
    throw(ProtocolError("unknown key-value operation: $op"))
end

_kv_is_delete_marker(operation::KeyValueOperation.T)::Bool =
    operation in (KeyValueOperation.DELETE, KeyValueOperation.PURGE)

_kv_key_active(msg::AbstractMsg)::Bool = !_kv_is_delete_marker(_kv_operation(msg))

function _kv_entry(kv::KeyValue, msg::AbstractMsg, revision::Int, created::DateTime, delta::Int)::KeyValueEntry
    KeyValueEntry(kv.bucket, _kv_key_from_subject(kv, msg.subject), msg.data, revision, created, delta, _kv_operation(msg))
end

function _kv_entry_from_stored_msg(kv::KeyValue, msg::AbstractMsg, revision::Int, created::Union{DateTime,Nothing})::KeyValueEntry
    isnothing(created) && throw(ProtocolError("key-value message is missing created timestamp"))
    _kv_entry(kv, msg, revision, created, 0)
end

_kv_created_from_timestamp_ns(timestamp_ns::Int)::DateTime =
    DateTime(1970, 1, 1) + Millisecond(timestamp_ns ÷ 1_000_000)

_kv_created_from_metadata(meta::MsgMetadata)::DateTime =
    _kv_created_from_timestamp_ns(meta.timestamp_ns)

function _kv_entry_from_consumer_msg(kv::KeyValue, msg::AbstractMsg)::KeyValueEntry
    meta = _parse_msg_metadata(msg)
    _kv_entry(kv, msg, meta.stream_sequence, _kv_created_from_timestamp_ns(meta.timestamp_ns), meta.pending)
end

function _kv_watcher_state(callback::Union{Function,Nothing}, channel_size::Integer,
                           notify_initial_done::Bool)
    updates = isnothing(callback) ? Channel{_KeyValueWatchUpdate}(Int(channel_size)) : nothing
    _KeyValueWatcherState(updates, callback, notify_initial_done, ReentrantLock(), false, false, -1, 0)
end

function _kv_watcher_closed(state::_KeyValueWatcherState)::Bool
    @lock state.lock state.closed
end

function _kv_watcher_emit!(state::_KeyValueWatcherState, update::_KeyValueWatchUpdate)
    _kv_watcher_closed(state) && return nothing
    try
        if !isnothing(state.updates)
            put!(state.updates, update)
        end
        if !isnothing(state.callback)
            if update isa KeyValueEntry || state.notify_initial_done
                state.callback(update)
            end
        end
    catch err
        err isa InvalidStateException && _kv_watcher_closed(state) && return nothing
        rethrow()
    end
    nothing
end

function _kv_watcher_record_initial!(state::_KeyValueWatcherState, entry::KeyValueEntry)::Bool
    @lock state.lock begin
        state.initial_done && return false
        state.initial_received += 1
        if entry.delta == 0 ||
           (state.initial_pending > 0 && state.initial_received >= state.initial_pending)
            state.initial_done = true
            return true
        end
        false
    end
end

function _kv_watcher_set_initial_pending!(state::_KeyValueWatcherState, pending::Int, updates_only::Bool)
    emit_done = @lock state.lock begin
        updates_only && (state.initial_done = true; return false)
        state.initial_pending = pending
        if !state.initial_done && (pending == 0 || state.initial_received >= pending)
            state.initial_done = true
            true
        else
            false
        end
    end
    emit_done && _kv_watcher_emit!(state, KV_WATCH_INITIAL_DONE)
    nothing
end

function _kv_watcher_close_state!(state::_KeyValueWatcherState)
    updates = state.updates
    already_closed = @lock state.lock begin
        was_closed = state.closed
        state.closed = true
        was_closed
    end
    if !already_closed && !isnothing(updates) && isopen(updates)
        close(updates)
    end
    nothing
end

function Base.close(watcher::KeyValueWatcher)
    errors = Any[]
    _kv_watcher_close_state!(watcher.state)
    try
        close(watcher.subscription)
    catch err
        push!(errors, err)
    end
    _throw_errors(errors)
    nothing
end

function Base.take!(watcher::KeyValueWatcher)
    isnothing(watcher.updates) &&
        throw(ArgumentError("callback key-value watchers do not buffer updates"))
    take!(watcher.updates)
end

function _kv_record_key!(latest::Dict{String,Tuple{Int,Bool}}, prefix::String, msg::AbstractMsg)
    startswith(msg.subject, prefix) ||
        throw(ProtocolError("key-value keys consumer received subject outside bucket prefix"))
    key = String(chop(msg.subject; head=length(prefix), tail=0))
    seq = _parse_msg_metadata(msg).stream_sequence
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

function _kv_stream_config(bucket::AbstractString; history::Integer=1,
                           ttl::Union{Real,Nothing}=nothing, max_bytes::Integer=-1,
                           max_value_size::Integer=-1, storage::Union{AbstractString,StorageType.T}="file",
                           replicas::Integer=1, direct::Bool=false, compression=nothing,
                           metadata=nothing, limit_marker_ttl::Union{Real,Nothing}=nothing)
    bucket = _validate_kv_bucket(bucket)
    marker_ttl = _kv_optional_seconds("limit_marker_ttl", limit_marker_ttl; allow_zero=false)
    StreamConfig(
        name=_kv_stream(bucket),
        subjects=["\$KV.$bucket.>"],
        max_msgs_per_subject=_validate_kv_history(history),
        max_msgs=-1,
        max_bytes=_validate_kv_limit("max_bytes", max_bytes),
        max_consumers=-1,
        max_age=_kv_optional_seconds("ttl", ttl),
        max_msg_size=_validate_kv_limit("max_value_size", max_value_size),
        storage=_parse_storage_type(storage),
        num_replicas=Int(replicas),
        allow_rollup_hdrs=true,
        allow_direct=direct,
        deny_delete=true,
        discard=DiscardPolicy.NEW,
        compression=_parse_kv_compression(compression),
        allow_msg_ttl=isnothing(marker_ttl) ? nothing : true,
        metadata=_kv_metadata(metadata),
        subject_delete_marker_ttl=marker_ttl,
    )
end

function kv_create(js::JetStreamContext, bucket::AbstractString; history::Integer=1,
                   ttl::Union{Real,Nothing}=nothing, max_bytes::Integer=-1,
                   max_value_size::Integer=-1, storage::Union{AbstractString,StorageType.T}="file",
                   replicas::Integer=1, direct::Bool=false, compression=nothing,
                   metadata=nothing, limit_marker_ttl::Union{Real,Nothing}=nothing)
    bucket = _validate_kv_bucket(bucket)
    cfg = _kv_stream_config(bucket; history, ttl, max_bytes, max_value_size, storage,
                            replicas, direct, compression, metadata, limit_marker_ttl)
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

function kv_put(kv::KeyValue, key::AbstractString, value; revision::Union{Nothing,Int}=nothing, ttl=nothing)
    key = _validate_kv_key(key)
    hdrs = Headers()
    expected_revision = _kv_add_expected_revision!(hdrs, revision)
    try
        js_publish(kv.js, "$(kv.prefix)$key", value; headers=hdrs, ttl)
    catch err
        _kv_wrong_last_sequence(err) && throw(_kv_wrong_revision_error(kv, key, expected_revision, err))
        rethrow()
    end
end

_kv_wrong_last_sequence(err) = err isa JetStreamError && err.err_code == 10071
_kv_delete_marker_revision(msg::AbstractMsg, sequence::Int) =
    _kv_is_delete_marker(_kv_operation(msg)) ? sequence : nothing

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

function kv_create_key(kv::KeyValue, key::AbstractString, value; ttl=nothing)
    key = _validate_kv_key(key)
    subject = "$(kv.prefix)$key"
    hdrs = Headers(_KV_EXPECTED_LAST_SUBJECT_SEQUENCE => ["0"])
    try
        return js_publish(kv.js, subject, value; headers=hdrs, ttl)
    catch err
        _kv_wrong_last_sequence(err) || rethrow()
        marker_revision = _kv_latest_delete_marker_revision(kv, subject)
        isnothing(marker_revision) && throw(_kv_key_exists_error(kv, key, err))
        retry_hdrs = Headers(_KV_EXPECTED_LAST_SUBJECT_SEQUENCE => [string(marker_revision)])
        try
            return js_publish(kv.js, subject, value; headers=retry_hdrs, ttl)
        catch retry_err
            _kv_wrong_last_sequence(retry_err) &&
                throw(_kv_wrong_revision_error(kv, key, marker_revision, retry_err))
            rethrow()
        end
    end
end

kv_update(kv::KeyValue, key::AbstractString, value, revision::Int; ttl=nothing) =
    kv_put(kv, key, value; revision, ttl)

function kv_delete(kv::KeyValue, key::AbstractString; revision::Union{Nothing,Int}=nothing)
    key = _validate_kv_key(key)
    hdrs = Headers("KV-Operation" => ["DEL"])
    expected_revision = _kv_add_expected_revision!(hdrs, revision)
    try
        js_publish(kv.js, "$(kv.prefix)$key", UInt8[]; headers=hdrs)
    catch err
        _kv_wrong_last_sequence(err) && throw(_kv_wrong_revision_error(kv, key, expected_revision, err))
        rethrow()
    end
end

function kv_purge(kv::KeyValue, key::AbstractString; revision::Union{Nothing,Int}=nothing, ttl=nothing)
    key = _validate_kv_key(key)
    hdrs = Headers("KV-Operation" => ["PURGE"], "Nats-Rollup" => ["sub"])
    expected_revision = _kv_add_expected_revision!(hdrs, revision)
    try
        js_publish(kv.js, "$(kv.prefix)$key", UInt8[]; headers=hdrs, ttl)
    catch err
        _kv_wrong_last_sequence(err) && throw(_kv_wrong_revision_error(kv, key, expected_revision, err))
        rethrow()
    end
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

function _kv_watch_channel_size(value::Integer)::Int
    value isa Bool && throw(ArgumentError("watch channel_size must be positive"))
    value > 0 || throw(ArgumentError("watch channel_size must be positive"))
    Int(value)
end

function _kv_watch_resume_revision(revision::Union{Integer,Nothing})
    isnothing(revision) && return nothing
    revision isa Bool && throw(ArgumentError("resume_revision must be positive"))
    revision > 0 || throw(ArgumentError("resume_revision must be positive"))
    Int(revision)
end

function _kv_watch_filters(key::AbstractString, keys)
    if isnothing(keys)
        return String[_validate_kv_watch_key(key)]
    end
    String(key) == ">" || throw(ArgumentError("provide either key or keys, not both"))
    keys isa AbstractVector || throw(ArgumentError("keys must be a vector of key filters"))
    isempty(keys) && return String[">"]
    filters = String[]
    sizehint!(filters, length(keys))
    for filter in keys
        filter isa AbstractString || throw(ArgumentError("keys entries must be strings"))
        push!(filters, _validate_kv_watch_key(filter))
    end
    filters
end

function _kv_watch_consumer_config(kv::KeyValue, filters::Vector{String}; history::Bool=false,
                                   updates_only::Bool=false, meta_only::Bool=false,
                                   resume_revision::Union{Integer,Nothing}=nothing)
    history && updates_only && throw(ArgumentError("history and updates_only cannot both be true"))
    revision = _kv_watch_resume_revision(resume_revision)
    !isnothing(revision) && updates_only &&
        throw(ArgumentError("resume_revision and updates_only cannot both be used"))
    subjects = ["$(kv.prefix)$filter" for filter in filters]
    cfg = Dict{String,Any}("ack_policy" => AckPolicy.NONE)
    if !isnothing(revision)
        cfg["deliver_policy"] = DeliverPolicy.BY_START_SEQUENCE
        cfg["opt_start_seq"] = revision
    elseif updates_only
        cfg["deliver_policy"] = DeliverPolicy.NEW
    elseif history
        cfg["deliver_policy"] = DeliverPolicy.ALL
    else
        cfg["deliver_policy"] = DeliverPolicy.LAST_PER_SUBJECT
    end
    meta_only && (cfg["headers_only"] = true)
    if length(subjects) == 1
        cfg["filter_subject"] = first(subjects)
    else
        cfg["filter_subjects"] = subjects
    end
    _js_config_payload(cfg)
end

function _consumer_num_pending(info::Union{ConsumerInfo,Nothing})::Int
    isnothing(info) && return 0
    value = get(info.raw, "num_pending", 0)
    value isa Real && !(value isa Bool) || return 0
    max(0, Int(value))
end

function _kv_watch_callback(kv::KeyValue, state::_KeyValueWatcherState, ignore_deletes::Bool)
    msg -> begin
        entry = _kv_entry_from_consumer_msg(kv, msg)
        if !ignore_deletes || !_kv_is_delete_marker(entry.operation)
            _kv_watcher_emit!(state, entry)
        end
        if _kv_watcher_record_initial!(state, entry)
            _kv_watcher_emit!(state, KV_WATCH_INITIAL_DONE)
        end
        nothing
    end
end

function _kv_watch(callback::Union{Function,Nothing}, kv::KeyValue; key::AbstractString=">",
                   keys=nothing, history::Bool=false, updates_only::Bool=false,
                   ignore_deletes::Bool=false, meta_only::Bool=false,
                   resume_revision::Union{Integer,Nothing}=nothing,
                   channel_size::Integer=256, notify_initial_done::Bool=false)
    filters = _kv_watch_filters(key, keys)
    cfg = _kv_watch_consumer_config(kv, filters; history, updates_only, meta_only, resume_revision)
    state = _kv_watcher_state(callback, _kv_watch_channel_size(channel_size), notify_initial_done)
    entry_callback = _kv_watch_callback(kv, state, ignore_deletes)
    sub = push_subscribe(kv.js, "$(kv.prefix)$(first(filters))"; stream=kv.stream,
                         callback=entry_callback, manual_ack=true, config=cfg)
    watcher = KeyValueWatcher(sub, state.updates, state)
    _kv_watcher_set_initial_pending!(state, _consumer_num_pending(sub.info), updates_only)
    watcher
end

kv_watch(kv::KeyValue; kwargs...) = _kv_watch(nothing, kv; kwargs...)

function kv_watch(callback::Function, kv::KeyValue; kwargs...)
    _kv_watch(callback, kv; kwargs...)
end

const _KV_DEFAULT_PURGE_DELETES_OLDER_THAN = 30 * 60.0

function _kv_purge_deletes_threshold(older_than::Real)::Float64
    older_than isa Bool && throw(ArgumentError("older_than must be a number of seconds"))
    seconds = Float64(older_than)
    isfinite(seconds) || throw(ArgumentError("older_than must be finite"))
    seconds == 0 ? _KV_DEFAULT_PURGE_DELETES_OLDER_THAN : seconds
end

function kv_purge_deletes(kv::KeyValue; older_than::Real=_KV_DEFAULT_PURGE_DELETES_OLDER_THAN)
    threshold = _kv_purge_deletes_threshold(older_than)
    watcher = kv_watch(kv; key=">", meta_only=true)
    markers = KeyValueEntry[]
    try
        while true
            update = take!(watcher)
            update isa KeyValueWatchInitialDone && break
            entry = update::KeyValueEntry
            _kv_is_delete_marker(entry.operation) && push!(markers, entry)
        end
    finally
        close(watcher)
    end

    current = DateTime(1970, 1, 1) + Millisecond(round(Int, time() * 1000))
    limit = threshold > 0 ? current - Millisecond(round(Int, threshold * 1000)) : nothing
    for entry in markers
        keep = !isnothing(limit) && entry.created > limit ? 1 : nothing
        stream_purge(kv.js, kv.stream; filter_subject="$(kv.prefix)$(entry.key)", keep)
    end
    nothing
end
