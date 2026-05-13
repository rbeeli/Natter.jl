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
end

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

_seconds_to_nanoseconds(value::Real)::Int = round(Int, Float64(value) * 1_000_000_000)
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
    payload
end

function _js_config_payload(config::AbstractDict)::Dict{String,Any}
    Dict{String,Any}(String(k) => _js_field_value(Symbol(String(k)), v) for (k, v) in pairs(config))
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
