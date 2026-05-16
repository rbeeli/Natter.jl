EnumX.@enumx RetentionPolicy begin
    LIMITS
    INTEREST
    WORK_QUEUE
end

EnumX.@enumx StorageType begin
    FILE
    MEMORY
end

EnumX.@enumx DiscardPolicy begin
    OLD
    NEW
end

EnumX.@enumx StoreCompression begin
    NONE
    S2
end

EnumX.@enumx PersistMode begin
    DEFAULT
    ASYNC
end

EnumX.@enumx AckPolicy begin
    NONE
    ALL
    EXPLICIT
end

EnumX.@enumx DeliverPolicy begin
    ALL
    LAST
    NEW
    BY_START_SEQUENCE
    BY_START_TIME
    LAST_PER_SUBJECT
end

EnumX.@enumx ReplayPolicy begin
    INSTANT
    ORIGINAL
end

EnumX.@enumx PriorityPolicy begin
    NONE
    OVERFLOW
    PINNED_CLIENT
    PRIORITIZED
end

const _JS_MAX_REPLICAS = 5
const _JS_MAX_NAME_LEN = 255
const _JS_MAX_DESCRIPTION_LEN = 4 * 1024
const _JS_MAX_METADATA_LEN = 128 * 1024
const _JS_MIN_DUPLICATE_WINDOW_NS = 100_000_000
const _JS_MIN_MAX_AGE_NS = 100_000_000
const _JS_MIN_HEARTBEAT_NS = 100_000_000
const _JS_MIN_MAX_EXPIRES_NS = 1_000_000
const _JS_MIN_SUBJECT_DELETE_MARKER_TTL_NS = 1_000_000_000
const _JS_MAX_INT32 = Int(typemax(Int32))
const _JS_PRIORITY_GROUP_RE = r"^[A-Za-z0-9/_=-]{1,16}$"

Base.@kwdef struct Placement
    cluster::Union{String,Nothing} = nothing
    preferred::Union{String,Nothing} = nothing
    tags::Union{Vector{String},Nothing} = nothing
end

Base.@kwdef struct ExternalStreamSource
    api::String
    deliver::Union{String,Nothing} = nothing
end

Base.@kwdef struct SubjectTransform
    src::String
    dest::String
end

Base.@kwdef struct StreamSource
    name::String
    opt_start_seq::Union{Int,Nothing} = nothing
    opt_start_time::Union{DateTime,String,Nothing} = nothing
    filter_subject::Union{String,Nothing} = nothing
    external::Union{ExternalStreamSource,Nothing} = nothing
    subject_transforms::Union{Vector{SubjectTransform},Nothing} = nothing
end

Base.@kwdef struct StreamConsumerLimits
    inactive_threshold::Union{Float64,Nothing} = nothing
    max_ack_pending::Union{Int,Nothing} = nothing
end

Base.@kwdef struct RePublish
    src::String
    dest::String
    headers_only::Union{Bool,Nothing} = nothing
end

Base.@kwdef struct StreamConfig
    name::Union{String,Nothing} = nothing
    description::Union{String,Nothing} = nothing
    subjects::Union{Vector{String},Nothing} = nothing
    retention::Union{RetentionPolicy.T,Nothing} = nothing
    max_consumers::Union{Int,Nothing} = nothing
    max_msgs::Union{Int,Nothing} = nothing
    max_bytes::Union{Int,Nothing} = nothing
    discard::Union{DiscardPolicy.T,Nothing} = nothing
    discard_new_per_subject::Union{Bool,Nothing} = nothing
    max_age::Union{Float64,Nothing} = nothing
    max_msgs_per_subject::Union{Int,Nothing} = nothing
    max_msg_size::Union{Int,Nothing} = nothing
    storage::Union{StorageType.T,Nothing} = nothing
    num_replicas::Union{Int,Nothing} = nothing
    no_ack::Union{Bool,Nothing} = nothing
    template_owner::Union{String,Nothing} = nothing
    duplicate_window::Union{Float64,Nothing} = nothing
    placement::Union{Placement,Nothing} = nothing
    mirror::Union{StreamSource,Nothing} = nothing
    sources::Union{Vector{StreamSource},Nothing} = nothing
    sealed::Union{Bool,Nothing} = nothing
    deny_delete::Union{Bool,Nothing} = nothing
    deny_purge::Union{Bool,Nothing} = nothing
    allow_rollup_hdrs::Union{Bool,Nothing} = nothing
    republish::Union{RePublish,Nothing} = nothing
    subject_transform::Union{SubjectTransform,Nothing} = nothing
    allow_direct::Union{Bool,Nothing} = nothing
    mirror_direct::Union{Bool,Nothing} = nothing
    compression::Union{StoreCompression.T,Nothing} = nothing
    allow_msg_ttl::Union{Bool,Nothing} = nothing
    allow_msg_schedules::Union{Bool,Nothing} = nothing
    allow_atomic::Union{Bool,Nothing} = nothing
    allow_batched::Union{Bool,Nothing} = nothing
    persist_mode::Union{PersistMode.T,Nothing} = nothing
    metadata::Union{Dict{String,String},Nothing} = nothing
    first_seq::Union{Int,Nothing} = nothing
    consumer_limits::Union{StreamConsumerLimits,Nothing} = nothing
    subject_delete_marker_ttl::Union{Float64,Nothing} = nothing
end

Base.@kwdef struct ConsumerConfig
    name::Union{String,Nothing} = nothing
    durable_name::Union{String,Nothing} = nothing
    description::Union{String,Nothing} = nothing
    deliver_policy::Union{DeliverPolicy.T,Nothing} = nothing
    opt_start_seq::Union{Int,Nothing} = nothing
    opt_start_time::Union{DateTime,String,Nothing} = nothing
    ack_policy::Union{AckPolicy.T,Nothing} = nothing
    ack_wait::Union{Float64,Nothing} = nothing
    max_deliver::Union{Int,Nothing} = nothing
    backoff::Union{Vector{Float64},Nothing} = nothing
    filter_subject::Union{String,Nothing} = nothing
    filter_subjects::Union{Vector{String},Nothing} = nothing
    replay_policy::Union{ReplayPolicy.T,Nothing} = nothing
    rate_limit_bps::Union{Int,Nothing} = nothing
    sample_freq::Union{String,Nothing} = nothing
    max_waiting::Union{Int,Nothing} = nothing
    max_ack_pending::Union{Int,Nothing} = nothing
    flow_control::Union{Bool,Nothing} = nothing
    idle_heartbeat::Union{Float64,Nothing} = nothing
    headers_only::Union{Bool,Nothing} = nothing
    deliver_subject::Union{String,Nothing} = nothing
    deliver_group::Union{String,Nothing} = nothing
    inactive_threshold::Union{Float64,Nothing} = nothing
    num_replicas::Union{Int,Nothing} = nothing
    mem_storage::Union{Bool,Nothing} = nothing
    metadata::Union{Dict{String,String},Nothing} = nothing
    pause_until::Union{DateTime,String,Nothing} = nothing
    direct::Union{Bool,Nothing} = nothing
    max_batch::Union{Int,Nothing} = nothing
    max_expires::Union{Float64,Nothing} = nothing
    max_bytes::Union{Int,Nothing} = nothing
    priority_groups::Union{Vector{String},Nothing} = nothing
    priority_policy::Union{PriorityPolicy.T,Nothing} = nothing
    priority_timeout::Union{Float64,Nothing} = nothing
end

const _JSConfigObject = Union{
    Placement,
    ExternalStreamSource,
    SubjectTransform,
    StreamSource,
    StreamConsumerLimits,
    RePublish,
    StreamConfig,
    ConsumerConfig,
}

const _JS_DURATION_FIELDS = Set{Symbol}((
    :ack_wait,
    :duplicate_window,
    :idle_heartbeat,
    :inactive_threshold,
    :max_age,
    :max_expires,
    :priority_timeout,
    :subject_delete_marker_ttl,
))

const _JS_TIMESTAMP_FIELDS = Set{Symbol}((:opt_start_time, :pause_until))

function _seconds_to_nanoseconds(value::Real)::Int
    seconds = Float64(value)
    isfinite(seconds) || throw(ArgumentError("duration values must be finite"))
    nanoseconds = seconds * 1_000_000_000
    typemin(Int) <= nanoseconds <= typemax(Int) ||
        throw(ArgumentError("duration value is outside Int range after nanosecond conversion"))
    round(Int, nanoseconds)
end
_nanoseconds_to_seconds(value)::Float64 = Float64(value) / 1_000_000_000

function _timestamp_to_rfc3339(value::DateTime)::String
    "$(Dates.format(value, dateformat"yyyy-mm-ddTHH:MM:SS.sss"))Z"
end
_timestamp_to_rfc3339(value::AbstractString)::String = String(value)

function _string_key_dict(obj)::Dict{String,Any}
    obj isa Dict{String,Any} && return obj
    Dict{String,Any}(String(k) => v for (k, v) in pairs(obj))
end

function _string_dict(value, field::AbstractString)::Dict{String,String}
    result = Dict{String,String}()
    for (k, v) in pairs(value)
        v isa AbstractString || throw(ArgumentError("$field values must be strings"))
        result[String(k)] = String(v)
    end
    result
end

function _validate_api_name(kind::AbstractString, name)
    n = String(name)
    isempty(n) && throw(ArgumentError("$kind name cannot be empty"))
    for c in n
        if isspace(c) || c in ('.', '*', '>', '/', '\\') || !isprint(c)
            throw(ArgumentError("$kind name contains an invalid character: $n"))
        end
    end
    n
end

function _validate_js_name_field!(payload::Dict{String,Any}, field::AbstractString, kind::AbstractString)
    haskey(payload, field) || return payload
    value = payload[field]
    isnothing(value) && return payload
    value isa AbstractString || throw(ArgumentError("$field must be a string"))
    name = _validate_api_name(kind, value)
    ncodeunits(name) <= _JS_MAX_NAME_LEN ||
        throw(ArgumentError("$kind name is too long; maximum is $_JS_MAX_NAME_LEN bytes"))
    payload[field] = name
    payload
end

function _validate_js_description!(payload::Dict{String,Any})
    haskey(payload, "description") || return payload
    value = payload["description"]
    isnothing(value) && return payload
    value isa AbstractString || throw(ArgumentError("description must be a string"))
    ncodeunits(value) <= _JS_MAX_DESCRIPTION_LEN ||
        throw(ArgumentError("description is too long; maximum is $_JS_MAX_DESCRIPTION_LEN bytes"))
    payload["description"] = String(value)
    payload
end

function _validate_js_metadata!(payload::Dict{String,Any})
    haskey(payload, "metadata") || return payload
    value = payload["metadata"]
    isnothing(value) && return payload
    value isa AbstractDict || throw(ArgumentError("metadata must be a dictionary"))
    total = 0
    for (k, v) in pairs(value)
        k isa AbstractString || throw(ArgumentError("metadata keys must be strings"))
        v isa AbstractString || throw(ArgumentError("metadata values must be strings"))
        total += ncodeunits(k) + ncodeunits(v)
    end
    total <= _JS_MAX_METADATA_LEN ||
        throw(ArgumentError("metadata exceeds maximum size of $_JS_MAX_METADATA_LEN bytes"))
    payload
end

_js_enum_value(value::RetentionPolicy.T)::String =
    value == RetentionPolicy.LIMITS ? "limits" :
    value == RetentionPolicy.INTEREST ? "interest" :
    value == RetentionPolicy.WORK_QUEUE ? "workqueue" :
    throw(ArgumentError("invalid retention policy: $value"))

_js_enum_value(value::StorageType.T)::String =
    value == StorageType.FILE ? "file" :
    value == StorageType.MEMORY ? "memory" :
    throw(ArgumentError("invalid storage type: $value"))

_js_enum_value(value::DiscardPolicy.T)::String =
    value == DiscardPolicy.OLD ? "old" :
    value == DiscardPolicy.NEW ? "new" :
    throw(ArgumentError("invalid discard policy: $value"))

_js_enum_value(value::StoreCompression.T)::String =
    value == StoreCompression.NONE ? "none" :
    value == StoreCompression.S2 ? "s2" :
    throw(ArgumentError("invalid store compression: $value"))

_js_enum_value(value::PersistMode.T)::String =
    value == PersistMode.DEFAULT ? "default" :
    value == PersistMode.ASYNC ? "async" :
    throw(ArgumentError("invalid persist mode: $value"))

_js_enum_value(value::AckPolicy.T)::String =
    value == AckPolicy.NONE ? "none" :
    value == AckPolicy.ALL ? "all" :
    value == AckPolicy.EXPLICIT ? "explicit" :
    throw(ArgumentError("invalid ack policy: $value"))

_js_enum_value(value::DeliverPolicy.T)::String =
    value == DeliverPolicy.ALL ? "all" :
    value == DeliverPolicy.LAST ? "last" :
    value == DeliverPolicy.NEW ? "new" :
    value == DeliverPolicy.BY_START_SEQUENCE ? "by_start_sequence" :
    value == DeliverPolicy.BY_START_TIME ? "by_start_time" :
    value == DeliverPolicy.LAST_PER_SUBJECT ? "last_per_subject" :
    throw(ArgumentError("invalid deliver policy: $value"))

_js_enum_value(value::ReplayPolicy.T)::String =
    value == ReplayPolicy.INSTANT ? "instant" :
    value == ReplayPolicy.ORIGINAL ? "original" :
    throw(ArgumentError("invalid replay policy: $value"))

_js_enum_value(value::PriorityPolicy.T)::String =
    value == PriorityPolicy.NONE ? "none" :
    value == PriorityPolicy.OVERFLOW ? "overflow" :
    value == PriorityPolicy.PINNED_CLIENT ? "pinned_client" :
    value == PriorityPolicy.PRIORITIZED ? "prioritized" :
    throw(ArgumentError("invalid priority policy: $value"))

function _js_field_value(field::Symbol, value)
    if isnothing(value)
        return nothing
    elseif field in _JS_DURATION_FIELDS
        return _seconds_to_nanoseconds(value)
    elseif field in _JS_TIMESTAMP_FIELDS
        return _timestamp_to_rfc3339(value)
    elseif value isa _JSConfigObject
        return _js_config_payload(value)
    elseif value isa AbstractDict
        return field == :metadata ? _string_dict(value, "metadata") :
               Dict{String,Any}(String(k) => _js_field_value(Symbol(String(k)), v) for (k, v) in pairs(value))
    else
        return value
    end
end

_js_field_value(_field::Symbol, value::RetentionPolicy.T) = _js_enum_value(value)
_js_field_value(_field::Symbol, value::StorageType.T) = _js_enum_value(value)
_js_field_value(_field::Symbol, value::DiscardPolicy.T) = _js_enum_value(value)
_js_field_value(_field::Symbol, value::StoreCompression.T) = _js_enum_value(value)
_js_field_value(_field::Symbol, value::PersistMode.T) = _js_enum_value(value)
_js_field_value(_field::Symbol, value::AckPolicy.T) = _js_enum_value(value)
_js_field_value(_field::Symbol, value::DeliverPolicy.T) = _js_enum_value(value)
_js_field_value(_field::Symbol, value::ReplayPolicy.T) = _js_enum_value(value)
_js_field_value(_field::Symbol, value::PriorityPolicy.T) = _js_enum_value(value)

function _js_field_value(field::Symbol, value::AbstractVector)
    field == :backoff && return [_seconds_to_nanoseconds(v) for v in value]
    [_js_field_value(field, v) for v in value]
end

function _js_config_payload(config::_JSConfigObject)::Dict{String,Any}
    payload = Dict{String,Any}()
    for field in fieldnames(typeof(config))
        value = getfield(config, field)
        isnothing(value) && continue
        payload[String(field)] = _js_field_value(field, value)
    end
    _validate_js_config_payload!(payload, config)
    payload
end

function _js_config_payload(config::AbstractDict)::Dict{String,Any}
    Dict{String,Any}(String(k) => _js_field_value(Symbol(String(k)), v) for (k, v) in pairs(config))
end

_validate_js_config_payload!(payload::Dict{String,Any}, ::_JSConfigObject) = payload
_validate_js_config_payload!(payload::Dict{String,Any}, ::ExternalStreamSource) =
    _validate_js_external_stream_source_payload!(payload)
_validate_js_config_payload!(payload::Dict{String,Any}, ::SubjectTransform) =
    _validate_js_subject_transform_payload!(payload)
_validate_js_config_payload!(payload::Dict{String,Any}, ::StreamSource) =
    _validate_js_stream_source_payload!(payload)
_validate_js_config_payload!(payload::Dict{String,Any}, ::StreamConsumerLimits) =
    _validate_js_stream_consumer_limits_payload!(payload)
_validate_js_config_payload!(payload::Dict{String,Any}, ::RePublish) =
    _validate_js_republish_payload!(payload)
_validate_js_config_payload!(payload::Dict{String,Any}, ::StreamConfig) =
    _validate_stream_config_payload!(payload)
_validate_js_config_payload!(payload::Dict{String,Any}, ::ConsumerConfig) =
    _validate_consumer_config_payload!(payload)

function _validate_js_integer!(payload::Dict{String,Any}, field::AbstractString; min::Union{Integer,Nothing}=nothing,
                               max::Union{Integer,Nothing}=nothing)
    haskey(payload, field) || return payload
    value = payload[field]
    isnothing(value) && return payload
    value isa Integer && !(value isa Bool) || throw(ArgumentError("$field must be an integer"))
    if !isnothing(min) && value < min
        throw(ArgumentError("$field must be at least $min"))
    elseif !isnothing(max) && value > max
        throw(ArgumentError("$field must be at most $max"))
    end
    payload[field] = Int(value)
    payload
end

function _validate_js_number!(payload::Dict{String,Any}, field::AbstractString; min::Union{Real,Nothing}=nothing,
                              max::Union{Real,Nothing}=nothing, min_positive::Union{Real,Nothing}=nothing)
    haskey(payload, field) || return payload
    value = payload[field]
    isnothing(value) && return payload
    value isa Real && !(value isa Bool) || throw(ArgumentError("$field must be numeric"))
    isfinite(Float64(value)) || throw(ArgumentError("$field must be finite"))
    if !isnothing(min) && value < min
        throw(ArgumentError("$field must be at least $min"))
    elseif !isnothing(max) && value > max
        throw(ArgumentError("$field must be at most $max"))
    elseif !isnothing(min_positive) && value > 0 && value < min_positive
        throw(ArgumentError("$field must be 0 or at least $min_positive"))
    end
    payload
end

function _validate_subject_field!(payload::Dict{String,Any}, field::AbstractString; allow_wildcards::Bool=true)
    haskey(payload, field) || return payload
    value = payload[field]
    isnothing(value) && return payload
    value isa AbstractString || throw(ArgumentError("$field must be a string"))
    payload[field] = _validate_subject(value; allow_wildcards)
    payload
end

function _validate_subject_vector_field!(payload::Dict{String,Any}, field::AbstractString; allow_wildcards::Bool=true,
                                         allow_empty::Bool=true)
    haskey(payload, field) || return String[]
    value = payload[field]
    isnothing(value) && return String[]
    value isa AbstractVector || throw(ArgumentError("$field must be a vector of subjects"))
    subjects = String[]
    sizehint!(subjects, length(value))
    for subject in value
        subject isa AbstractString || throw(ArgumentError("$field entries must be strings"))
        push!(subjects, _validate_subject(subject; allow_wildcards))
    end
    if isempty(subjects)
        allow_empty || throw(ArgumentError("$field cannot be empty"))
        return subjects
    end
    payload[field] = subjects
    subjects
end

function _validate_non_overlapping_subjects(subjects::Vector{String}, field::AbstractString)
    for i in eachindex(subjects)
        for j in (i + 1):lastindex(subjects)
            _subjects_overlap(subjects[i], subjects[j]) &&
                throw(ArgumentError("$field entries cannot overlap"))
        end
    end
    subjects
end

function _validate_js_object_field!(payload::Dict{String,Any}, field::AbstractString, validate!)
    haskey(payload, field) || return payload
    value = payload[field]
    isnothing(value) && return payload
    value isa Dict{String,Any} || throw(ArgumentError("$field must be an object"))
    validate!(value)
    payload
end

function _validate_js_object_vector_field!(payload::Dict{String,Any}, field::AbstractString, validate!)
    haskey(payload, field) || return payload
    value = payload[field]
    isnothing(value) && return payload
    value isa AbstractVector || throw(ArgumentError("$field must be a vector of objects"))
    for item in value
        item isa Dict{String,Any} || throw(ArgumentError("$field entries must be objects"))
        validate!(item)
    end
    payload
end

function _validate_js_subject_transform_payload!(payload::Dict{String,Any})::Dict{String,Any}
    _validate_subject_field!(payload, "src")
    _validate_subject_field!(payload, "dest")
    payload
end

function _validate_js_external_stream_source_payload!(payload::Dict{String,Any})::Dict{String,Any}
    _validate_subject_field!(payload, "api"; allow_wildcards=false)
    _validate_subject_field!(payload, "deliver"; allow_wildcards=false)
    payload
end

function _validate_js_stream_source_payload!(payload::Dict{String,Any})::Dict{String,Any}
    _validate_js_name_field!(payload, "name", "stream")
    _validate_js_integer!(payload, "opt_start_seq"; min=0)
    _validate_subject_field!(payload, "filter_subject")
    _validate_js_object_field!(payload, "external", _validate_js_external_stream_source_payload!)
    _validate_js_object_vector_field!(payload, "subject_transforms", _validate_js_subject_transform_payload!)
    if haskey(payload, "filter_subject") && !isnothing(payload["filter_subject"]) &&
       haskey(payload, "subject_transforms") && !isnothing(payload["subject_transforms"]) &&
       !isempty(payload["subject_transforms"])
        throw(ArgumentError("stream source cannot have both filter_subject and subject_transforms specified"))
    end
    payload
end

function _validate_js_stream_consumer_limits_payload!(payload::Dict{String,Any})::Dict{String,Any}
    _validate_js_number!(payload, "inactive_threshold"; min=0)
    _validate_js_integer!(payload, "max_ack_pending"; min=-1)
    payload
end

function _validate_js_republish_payload!(payload::Dict{String,Any})::Dict{String,Any}
    _validate_subject_field!(payload, "src")
    _validate_subject_field!(payload, "dest")
    payload
end

function _validate_stream_config_payload!(payload::Dict{String,Any})::Dict{String,Any}
    _validate_js_name_field!(payload, "name", "stream")
    _validate_js_description!(payload)
    _validate_js_metadata!(payload)

    subjects = _validate_subject_vector_field!(payload, "subjects"; allow_wildcards=true)
    _validate_non_overlapping_subjects(subjects, "stream subjects")

    _validate_js_integer!(payload, "max_consumers"; min=-1)
    _validate_js_integer!(payload, "max_msgs"; min=-1)
    _validate_js_integer!(payload, "max_bytes"; min=-1)
    _validate_js_integer!(payload, "max_msgs_per_subject"; min=-1)
    _validate_js_integer!(payload, "max_msg_size"; min=-1, max=_JS_MAX_INT32)
    _validate_js_integer!(payload, "num_replicas"; min=0, max=_JS_MAX_REPLICAS)
    _validate_js_integer!(payload, "first_seq"; min=0)

    _validate_js_number!(payload, "max_age"; min=0, min_positive=_JS_MIN_MAX_AGE_NS)
    _validate_js_number!(payload, "duplicate_window"; min=0, min_positive=_JS_MIN_DUPLICATE_WINDOW_NS)
    if haskey(payload, "max_age") && !isnothing(payload["max_age"]) && payload["max_age"] > 0 &&
       haskey(payload, "duplicate_window") && !isnothing(payload["duplicate_window"]) &&
       payload["duplicate_window"] > payload["max_age"]
        throw(ArgumentError("duplicate_window cannot be larger than max_age"))
    end
    _validate_js_number!(payload, "subject_delete_marker_ttl"; min=0,
                         min_positive=_JS_MIN_SUBJECT_DELETE_MARKER_TTL_NS)
    if get(payload, "discard_new_per_subject", false) == true &&
       get(payload, "max_msgs_per_subject", 0) <= 0
        throw(ArgumentError("discard_new_per_subject requires max_msgs_per_subject > 0"))
    end
    _validate_js_object_field!(payload, "mirror", _validate_js_stream_source_payload!)
    _validate_js_object_vector_field!(payload, "sources", _validate_js_stream_source_payload!)
    _validate_js_object_field!(payload, "republish", _validate_js_republish_payload!)
    _validate_js_object_field!(payload, "subject_transform", _validate_js_subject_transform_payload!)
    _validate_js_object_field!(payload, "consumer_limits", _validate_js_stream_consumer_limits_payload!)
    payload
end

function _validate_filter_subjects_value(value)::Vector{String}
    value isa AbstractVector || throw(ArgumentError("filter_subjects must be a vector of subjects"))
    subjects = String[]
    sizehint!(subjects, length(value))
    for subject in value
        subject isa AbstractString || throw(ArgumentError("filter_subjects entries must be strings"))
        push!(subjects, _validate_subject(subject))
    end
    subjects
end

function _subject_tokens_overlap(a::Vector{SubString{String}}, ai::Int,
                                 b::Vector{SubString{String}}, bi::Int)::Bool
    ai > length(a) && return bi > length(b)
    bi > length(b) && return false

    atok = a[ai]
    btok = b[bi]
    atok == ">" && return true
    btok == ">" && return true
    (atok == "*" || btok == "*" || atok == btok) ||
        return false
    _subject_tokens_overlap(a, ai + 1, b, bi + 1)
end

function _subjects_overlap(a::String, b::String)::Bool
    _subject_tokens_overlap(split(a, "."), 1, split(b, "."), 1)
end

function _validate_filter_subjects_do_not_overlap(subjects::Vector{String})
    for i in eachindex(subjects)
        for j in (i + 1):lastindex(subjects)
            _subjects_overlap(subjects[i], subjects[j]) &&
                throw(ArgumentError("consumer subject filters cannot overlap"))
        end
    end
    subjects
end

function _validate_consumer_priority_policy!(payload::Dict{String,Any})::Union{String,Nothing}
    haskey(payload, "priority_policy") || return nothing
    value = payload["priority_policy"]
    isnothing(value) && return nothing
    value isa AbstractString || throw(ArgumentError("priority_policy must be a string"))
    policy = String(value)
    policy in ("none", "overflow", "pinned_client", "prioritized") ||
        throw(ArgumentError("priority_policy must be one of none, overflow, pinned_client, or prioritized"))
    payload["priority_policy"] = policy
    policy
end

function _validate_consumer_priority_config!(payload::Dict{String,Any},
                                             policy::Union{String,Nothing},
                                             priority_groups::Vector{String})
    has_priority_groups = !isempty(priority_groups)
    has_priority_policy = !isnothing(policy) && policy != "none"
    has_priority_timeout = haskey(payload, "priority_timeout") && !isnothing(payload["priority_timeout"])
    has_push_delivery =
        (haskey(payload, "deliver_subject") && !isnothing(payload["deliver_subject"])) ||
        (haskey(payload, "deliver_group") && !isnothing(payload["deliver_group"]))

    if has_push_delivery && (has_priority_groups || has_priority_policy || has_priority_timeout)
        throw(ArgumentError("push consumers do not support priority policy, priority groups, or priority_timeout"))
    end
    if has_priority_policy && !has_priority_groups
        throw(ArgumentError("priority_policy requires at least one priority_groups entry"))
    end
    if has_priority_groups && !has_priority_policy
        throw(ArgumentError("priority_groups require priority_policy"))
    end
    if has_priority_timeout && policy != "pinned_client"
        throw(ArgumentError("priority_timeout requires priority_policy=pinned_client"))
    end
    payload
end

function _validate_consumer_config_payload!(payload::Dict{String,Any})::Dict{String,Any}
    _validate_js_name_field!(payload, "name", "consumer")
    _validate_js_name_field!(payload, "durable_name", "consumer")
    _validate_js_description!(payload)
    _validate_js_metadata!(payload)
    if haskey(payload, "name") && !isnothing(payload["name"]) &&
       haskey(payload, "durable_name") && !isnothing(payload["durable_name"]) &&
       payload["name"] != payload["durable_name"]
        throw(ArgumentError("consumer name and durable_name must match when both are specified"))
    end

    _validate_subject_field!(payload, "filter_subject")

    has_filter_subjects = false
    if haskey(payload, "filter_subjects") && !isnothing(payload["filter_subjects"])
        subjects = _validate_filter_subjects_value(payload["filter_subjects"])
        if isempty(subjects)
            delete!(payload, "filter_subjects")
        else
            payload["filter_subjects"] = subjects
            has_filter_subjects = true
        end
    end

    if haskey(payload, "filter_subject") && !isnothing(payload["filter_subject"]) && has_filter_subjects
        throw(ArgumentError("consumer cannot have both filter_subject and filter_subjects specified"))
    end
    has_filter_subjects && _validate_filter_subjects_do_not_overlap(payload["filter_subjects"])

    _validate_subject_field!(payload, "deliver_subject"; allow_wildcards=false)
    if haskey(payload, "deliver_group") && !isnothing(payload["deliver_group"])
        payload["deliver_group"] isa AbstractString || throw(ArgumentError("deliver_group must be a string"))
        payload["deliver_group"] = _validate_queue(String(payload["deliver_group"]))
    end

    _validate_js_integer!(payload, "opt_start_seq"; min=0)
    _validate_js_integer!(payload, "max_deliver"; min=-1)
    _validate_js_integer!(payload, "rate_limit_bps"; min=0)
    _validate_js_integer!(payload, "max_waiting"; min=0)
    _validate_js_integer!(payload, "max_ack_pending"; min=-1)
    _validate_js_integer!(payload, "num_replicas"; min=0, max=_JS_MAX_REPLICAS)
    _validate_js_integer!(payload, "max_batch"; min=0)
    _validate_js_integer!(payload, "max_bytes"; min=0)

    _validate_js_number!(payload, "ack_wait"; min=0)
    _validate_js_number!(payload, "idle_heartbeat"; min=0, min_positive=_JS_MIN_HEARTBEAT_NS)
    _validate_js_number!(payload, "inactive_threshold"; min=0)
    _validate_js_number!(payload, "max_expires"; min=0, min_positive=_JS_MIN_MAX_EXPIRES_NS)
    _validate_js_number!(payload, "priority_timeout"; min=0)

    if haskey(payload, "backoff") && !isnothing(payload["backoff"])
        backoff = payload["backoff"]
        backoff isa AbstractVector || throw(ArgumentError("backoff must be a vector of durations"))
        for value in backoff
            value isa Real && !(value isa Bool) || throw(ArgumentError("backoff entries must be numeric"))
            isfinite(Float64(value)) || throw(ArgumentError("backoff entries must be finite"))
            value >= 0 || throw(ArgumentError("backoff entries must be non-negative"))
        end
    end

    if haskey(payload, "sample_freq") && !isnothing(payload["sample_freq"])
        payload["sample_freq"] isa AbstractString || throw(ArgumentError("sample_freq must be a string"))
        raw = String(payload["sample_freq"])
        freq = endswith(raw, "%") ? chop(raw; tail=1) : raw
        !isempty(freq) && all(isdigit, freq) || throw(ArgumentError("sample_freq must be a non-negative integer percentage"))
        payload["sample_freq"] = raw
    end

    priority_groups = String[]
    if haskey(payload, "priority_groups") && !isnothing(payload["priority_groups"])
        groups = payload["priority_groups"]
        groups isa AbstractVector || throw(ArgumentError("priority_groups must be a vector of strings"))
        validated = String[]
        sizehint!(validated, length(groups))
        for group in groups
            group isa AbstractString || throw(ArgumentError("priority_groups entries must be strings"))
            s = String(group)
            occursin(_JS_PRIORITY_GROUP_RE, s) ||
                throw(ArgumentError("priority_groups entries must match [A-Za-z0-9/_=-]{1,16}"))
            push!(validated, s)
        end
        payload["priority_groups"] = validated
        priority_groups = validated
    end
    policy = _validate_consumer_priority_policy!(payload)
    _validate_consumer_priority_config!(payload, policy, priority_groups)
    payload
end

_present(d::Dict{String,Any}, field::Symbol)::Bool = haskey(d, String(field)) && !isnothing(d[String(field)])
_get(d::Dict{String,Any}, field::Symbol) = haskey(d, String(field)) ? d[String(field)] : nothing

_maybe_string(value) = isnothing(value) ? nothing : String(value)
_maybe_bool(value) = isnothing(value) ? nothing : Bool(value)
_maybe_int(value) = isnothing(value) ? nothing : Int(value)
_maybe_seconds(value) = isnothing(value) ? nothing : _nanoseconds_to_seconds(value)
_maybe_timestamp(value) = isnothing(value) ? nothing : String(value)

function _maybe_string_vector(value)
    isnothing(value) && return nothing
    [String(v) for v in value]
end

function _maybe_seconds_vector(value)
    isnothing(value) && return nothing
    [_nanoseconds_to_seconds(v) for v in value]
end

function _maybe_metadata(value)
    isnothing(value) && return nothing
    _string_dict(value, "metadata")
end

function _parse_retention_policy(value)
    s = String(value)
    s == "limits" && return RetentionPolicy.LIMITS
    s == "interest" && return RetentionPolicy.INTEREST
    s == "workqueue" && return RetentionPolicy.WORK_QUEUE
    throw(ArgumentError("unknown retention policy: $s"))
end

function _parse_storage_type(value)
    value isa StorageType.T && return value
    s = String(value)
    s == "file" && return StorageType.FILE
    s == "memory" && return StorageType.MEMORY
    throw(ArgumentError("unknown storage type: $s"))
end

function _parse_discard_policy(value)
    s = String(value)
    s == "old" && return DiscardPolicy.OLD
    s == "new" && return DiscardPolicy.NEW
    throw(ArgumentError("unknown discard policy: $s"))
end

function _parse_store_compression(value)
    s = String(value)
    s == "none" && return StoreCompression.NONE
    s == "s2" && return StoreCompression.S2
    throw(ArgumentError("unknown store compression: $s"))
end

function _parse_persist_mode(value)
    s = String(value)
    s == "default" && return PersistMode.DEFAULT
    s == "async" && return PersistMode.ASYNC
    throw(ArgumentError("unknown persist mode: $s"))
end

function _parse_ack_policy(value)
    s = String(value)
    s == "none" && return AckPolicy.NONE
    s == "all" && return AckPolicy.ALL
    s == "explicit" && return AckPolicy.EXPLICIT
    throw(ArgumentError("unknown ack policy: $s"))
end

function _parse_deliver_policy(value)
    s = String(value)
    s == "all" && return DeliverPolicy.ALL
    s == "last" && return DeliverPolicy.LAST
    s == "new" && return DeliverPolicy.NEW
    s == "by_start_sequence" && return DeliverPolicy.BY_START_SEQUENCE
    s == "by_start_time" && return DeliverPolicy.BY_START_TIME
    s == "last_per_subject" && return DeliverPolicy.LAST_PER_SUBJECT
    throw(ArgumentError("unknown deliver policy: $s"))
end

function _parse_replay_policy(value)
    s = String(value)
    s == "instant" && return ReplayPolicy.INSTANT
    s == "original" && return ReplayPolicy.ORIGINAL
    throw(ArgumentError("unknown replay policy: $s"))
end

function _parse_priority_policy(value)
    s = String(value)
    s == "none" && return PriorityPolicy.NONE
    s == "overflow" && return PriorityPolicy.OVERFLOW
    s == "pinned_client" && return PriorityPolicy.PINNED_CLIENT
    s == "prioritized" && return PriorityPolicy.PRIORITIZED
    throw(ArgumentError("unknown priority policy: $s"))
end

_maybe_enum(parser::Function, value) = isnothing(value) ? nothing : parser(value)

function _placement_from_payload(value)::Placement
    d = _string_key_dict(value)
    Placement(; cluster=_maybe_string(_get(d, :cluster)),
              preferred=_maybe_string(_get(d, :preferred)),
              tags=_maybe_string_vector(_get(d, :tags)))
end

function _external_stream_source_from_payload(value)::ExternalStreamSource
    d = _string_key_dict(value)
    _present(d, :api) || throw(ArgumentError("external stream source api is required"))
    ExternalStreamSource(; api=String(_get(d, :api)),
                         deliver=_maybe_string(_get(d, :deliver)))
end

function _subject_transform_from_payload(value)::SubjectTransform
    d = _string_key_dict(value)
    _present(d, :src) || throw(ArgumentError("subject transform src is required"))
    _present(d, :dest) || throw(ArgumentError("subject transform dest is required"))
    SubjectTransform(; src=String(_get(d, :src)), dest=String(_get(d, :dest)))
end

function _stream_source_from_payload(value)::StreamSource
    d = _string_key_dict(value)
    _present(d, :name) || throw(ArgumentError("stream source name is required"))
    external = _present(d, :external) ? _external_stream_source_from_payload(_get(d, :external)) : nothing
    transforms = _present(d, :subject_transforms) ?
                 [_subject_transform_from_payload(v) for v in _get(d, :subject_transforms)] :
                 nothing
    StreamSource(; name=String(_get(d, :name)),
                 opt_start_seq=_maybe_int(_get(d, :opt_start_seq)),
                 opt_start_time=_maybe_timestamp(_get(d, :opt_start_time)),
                 filter_subject=_maybe_string(_get(d, :filter_subject)),
                 external,
                 subject_transforms=transforms)
end

function _stream_consumer_limits_from_payload(value)::StreamConsumerLimits
    d = _string_key_dict(value)
    StreamConsumerLimits(; inactive_threshold=_maybe_seconds(_get(d, :inactive_threshold)),
                         max_ack_pending=_maybe_int(_get(d, :max_ack_pending)))
end

function _republish_from_payload(value)::RePublish
    d = _string_key_dict(value)
    _present(d, :src) || throw(ArgumentError("republish src is required"))
    _present(d, :dest) || throw(ArgumentError("republish dest is required"))
    RePublish(; src=String(_get(d, :src)),
              dest=String(_get(d, :dest)),
              headers_only=_maybe_bool(_get(d, :headers_only)))
end

function _stream_config_from_payload(value)::StreamConfig
    d = _string_key_dict(value)
    StreamConfig(;
        name=_maybe_string(_get(d, :name)),
        description=_maybe_string(_get(d, :description)),
        subjects=_maybe_string_vector(_get(d, :subjects)),
        retention=_maybe_enum(_parse_retention_policy, _get(d, :retention)),
        max_consumers=_maybe_int(_get(d, :max_consumers)),
        max_msgs=_maybe_int(_get(d, :max_msgs)),
        max_bytes=_maybe_int(_get(d, :max_bytes)),
        discard=_maybe_enum(_parse_discard_policy, _get(d, :discard)),
        discard_new_per_subject=_maybe_bool(_get(d, :discard_new_per_subject)),
        max_age=_maybe_seconds(_get(d, :max_age)),
        max_msgs_per_subject=_maybe_int(_get(d, :max_msgs_per_subject)),
        max_msg_size=_maybe_int(_get(d, :max_msg_size)),
        storage=_maybe_enum(_parse_storage_type, _get(d, :storage)),
        num_replicas=_maybe_int(_get(d, :num_replicas)),
        no_ack=_maybe_bool(_get(d, :no_ack)),
        template_owner=_maybe_string(_get(d, :template_owner)),
        duplicate_window=_maybe_seconds(_get(d, :duplicate_window)),
        placement=_present(d, :placement) ? _placement_from_payload(_get(d, :placement)) : nothing,
        mirror=_present(d, :mirror) ? _stream_source_from_payload(_get(d, :mirror)) : nothing,
        sources=_present(d, :sources) ? [_stream_source_from_payload(v) for v in _get(d, :sources)] : nothing,
        sealed=_maybe_bool(_get(d, :sealed)),
        deny_delete=_maybe_bool(_get(d, :deny_delete)),
        deny_purge=_maybe_bool(_get(d, :deny_purge)),
        allow_rollup_hdrs=_maybe_bool(_get(d, :allow_rollup_hdrs)),
        republish=_present(d, :republish) ? _republish_from_payload(_get(d, :republish)) : nothing,
        subject_transform=_present(d, :subject_transform) ? _subject_transform_from_payload(_get(d, :subject_transform)) : nothing,
        allow_direct=_maybe_bool(_get(d, :allow_direct)),
        mirror_direct=_maybe_bool(_get(d, :mirror_direct)),
        compression=_maybe_enum(_parse_store_compression, _get(d, :compression)),
        allow_msg_ttl=_maybe_bool(_get(d, :allow_msg_ttl)),
        allow_msg_schedules=_maybe_bool(_get(d, :allow_msg_schedules)),
        allow_atomic=_maybe_bool(_get(d, :allow_atomic)),
        allow_batched=_maybe_bool(_get(d, :allow_batched)),
        persist_mode=_maybe_enum(_parse_persist_mode, _get(d, :persist_mode)),
        metadata=_maybe_metadata(_get(d, :metadata)),
        first_seq=_maybe_int(_get(d, :first_seq)),
        consumer_limits=_present(d, :consumer_limits) ? _stream_consumer_limits_from_payload(_get(d, :consumer_limits)) : nothing,
        subject_delete_marker_ttl=_maybe_seconds(_get(d, :subject_delete_marker_ttl)))
end

function _consumer_config_from_payload(value)::ConsumerConfig
    d = _string_key_dict(value)
    ConsumerConfig(;
        name=_maybe_string(_get(d, :name)),
        durable_name=_maybe_string(_get(d, :durable_name)),
        description=_maybe_string(_get(d, :description)),
        deliver_policy=_maybe_enum(_parse_deliver_policy, _get(d, :deliver_policy)),
        opt_start_seq=_maybe_int(_get(d, :opt_start_seq)),
        opt_start_time=_maybe_timestamp(_get(d, :opt_start_time)),
        ack_policy=_maybe_enum(_parse_ack_policy, _get(d, :ack_policy)),
        ack_wait=_maybe_seconds(_get(d, :ack_wait)),
        max_deliver=_maybe_int(_get(d, :max_deliver)),
        backoff=_maybe_seconds_vector(_get(d, :backoff)),
        filter_subject=_maybe_string(_get(d, :filter_subject)),
        filter_subjects=_maybe_string_vector(_get(d, :filter_subjects)),
        replay_policy=_maybe_enum(_parse_replay_policy, _get(d, :replay_policy)),
        rate_limit_bps=_maybe_int(_get(d, :rate_limit_bps)),
        sample_freq=_maybe_string(_get(d, :sample_freq)),
        max_waiting=_maybe_int(_get(d, :max_waiting)),
        max_ack_pending=_maybe_int(_get(d, :max_ack_pending)),
        flow_control=_maybe_bool(_get(d, :flow_control)),
        idle_heartbeat=_maybe_seconds(_get(d, :idle_heartbeat)),
        headers_only=_maybe_bool(_get(d, :headers_only)),
        deliver_subject=_maybe_string(_get(d, :deliver_subject)),
        deliver_group=_maybe_string(_get(d, :deliver_group)),
        inactive_threshold=_maybe_seconds(_get(d, :inactive_threshold)),
        num_replicas=_maybe_int(_get(d, :num_replicas)),
        mem_storage=_maybe_bool(_get(d, :mem_storage)),
        metadata=_maybe_metadata(_get(d, :metadata)),
        pause_until=_maybe_timestamp(_get(d, :pause_until)),
        direct=_maybe_bool(_get(d, :direct)),
        max_batch=_maybe_int(_get(d, :max_batch)),
        max_expires=_maybe_seconds(_get(d, :max_expires)),
        max_bytes=_maybe_int(_get(d, :max_bytes)),
        priority_groups=_maybe_string_vector(_get(d, :priority_groups)),
        priority_policy=_maybe_enum(_parse_priority_policy, _get(d, :priority_policy)),
        priority_timeout=_maybe_seconds(_get(d, :priority_timeout)))
end
