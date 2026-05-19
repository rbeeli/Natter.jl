function _parse_server_urls(urls; drop_empty::Bool=false)::Vector{String}
    servers = String[]
    for raw in urls
        url = strip(String(raw))
        if isempty(url)
            drop_empty && continue
            throw(ArgumentError("server URL cannot be empty"))
        end
        push!(servers, url)
    end
    isempty(servers) && throw(ArgumentError("at least one server URL is required"))
    servers
end

function _parse_options(url_or_urls; kwargs...)
    servers =
        if isnothing(url_or_urls)
            [DEFAULT_URL]
        elseif url_or_urls isa AbstractString
            _parse_server_urls(split(String(url_or_urls), ","; keepempty=false); drop_empty=true)
        elseif url_or_urls isa Union{AbstractVector,Tuple}
            _parse_server_urls(url_or_urls)
        else
            throw(ArgumentError("servers must be a string, vector, or tuple of strings"))
        end
    ConnectOptions(; servers, kwargs...)
end

_write_buffer_prealloc_size(opts::ConnectOptions)::Int =
    min(max(0, opts.write_buffer_size), 16 * 1024 * 1024)

_write_transport_for_options(io, opts::ConnectOptions) =
    max(0, opts.write_buffer_size) == 0 ? io : BufferedWriteIO(io, _write_buffer_prealloc_size(opts))

function _connect_with_options(opts::ConnectOptions; cancel_token::MaybeCancellationToken=nothing)
    _throw_if_cancelled(cancel_token)
    client = Client(
        opts,
        [Server(s) for s in opts.servers],
        nothing,
        nothing,
        ConnectionStatus.DISCONNECTED,
        ServerInfo(),
        nothing,
        nothing,
        nothing,
        ReentrantLock(),
        ReentrantLock(),
        Base.Threads.Condition(ReentrantLock()),
        FlushSignal(),
        nothing,
        0,
        Dict{Int,Subscription}(),
        ReentrantLock(),
        nothing,
        ReentrantLock(),
        IOBuffer(),
        0,
        PongWaiterQueue(),
        nothing,
        nothing,
        nothing,
        0,
        Stats(),
        MersenneTwister(rand(UInt)),
        0,
    )
    _connect_initial!(client; cancel_token)
    client
end

function connect(options::ConnectOptions; cancel_token::MaybeCancellationToken=nothing)
    _connect_with_options(options; cancel_token)
end

function connect(url_or_urls=nothing; cancel_token::MaybeCancellationToken=nothing, kwargs...)
    _connect_with_options(_parse_options(url_or_urls; kwargs...); cancel_token)
end

function _server_attempt_order!(client::Client)::Vector{Server}
    @lock client.lock begin
        servers = copy(client.servers)
        client.options.randomize_servers && shuffle!(client.rng, servers)
        servers
    end
end

function _normalize_url(url::String)
    contains(url, "://") ? url : "nats://$url"
end

function _normalize_discovered_url(raw_url::AbstractString, base_url::Union{String,Nothing})::String
    url = String(raw_url)
    contains(url, "://") && return url
    isnothing(base_url) && return _normalize_url(url)

    base = URI(_normalize_url(base_url))
    scheme = isempty(base.scheme) ? "nats" : String(base.scheme)
    userinfo = isempty(base.userinfo) ? "" : "$(base.userinfo)@"
    "$scheme://$userinfo$url"
end

function _host_is_ip(host::AbstractString)::Bool
    value = String(host)
    if startswith(value, "[") && endswith(value, "]")
        value = value[2:end-1]
    end

    if occursin(':', value)
        try
            Sockets.IPv6(value)
            return true
        catch
            return false
        end
    end

    if occursin('.', value)
        parts = split(value, '.')
        length(parts) == 4 || return false
        for part in parts
            isempty(part) && return false
            all(isdigit, part) || return false
            parsed = tryparse(Int, part)
            isnothing(parsed) && return false
            0 <= parsed <= 255 || return false
        end
        return true
    end
    false
end

function _server_parts(url::String)
    uri = URI(_normalize_url(url))
    scheme = isempty(uri.scheme) ? "nats" : uri.scheme
    scheme in ("nats", "tls") || throw(UnsupportedFeatureError("transport scheme $scheme"))
    host = String(uri.host)
    isempty(host) && throw(ArgumentError("server host is missing in $url"))
    port = isempty(uri.port) ? 4222 : parse(Int, uri.port)
    user = nothing
    password = nothing
    if !isempty(uri.userinfo)
        pieces = split(uri.userinfo, ":"; limit=2)
        user = unescapeuri(String(first(pieces)))
        password = length(pieces) == 2 ? unescapeuri(String(last(pieces))) : nothing
    end
    scheme, host, port, user, password
end

_tls_hostname(server::Server, host::String)::String = something(server.tls_name, host)
_tls_server_name(opts::ConnectOptions, server::Server, host::String)::String =
    something(opts.tls_server_name, _tls_hostname(server, host))

function _current_tls_name_for_discovery(current_server::Union{Server,Nothing},
                                         base_url::Union{String,Nothing})::Union{String,Nothing}
    if !isnothing(current_server)
        !isnothing(current_server.tls_name) && return current_server.tls_name
        _, host, _, _, _ = _server_parts(current_server.url)
        return _host_is_ip(host) ? nothing : host
    end
    isnothing(base_url) && return nothing

    _, host, _, _, _ = _server_parts(base_url)
    _host_is_ip(host) ? nothing : host
end

function _discovery_uses_tls(opts::ConnectOptions, info::ServerInfo,
                             current_server::Union{Server,Nothing},
                             base_url::Union{String,Nothing})::Bool
    !isnothing(current_server) && !isnothing(current_server.tls_name) && return true
    source_url = !isnothing(current_server) ? current_server.url : base_url
    scheme = isnothing(source_url) ? "nats" : first(_server_parts(source_url))
    scheme == "tls" || opts.tls_required || opts.tls_first === true || info.tls_required === true
end

function _discovered_tls_name(url::String, current_server::Union{Server,Nothing},
                              base_url::Union{String,Nothing}, tls_active::Bool)::Union{String,Nothing}
    tls_active || return nothing
    _, host, _, _, _ = _server_parts(url)
    _host_is_ip(host) ? _current_tls_name_for_discovery(current_server, base_url) : nothing
end

function _tls_ip_text(host::AbstractString)::String
    value = String(host)
    startswith(value, "[") && endswith(value, "]") ? value[2:end-1] : value
end

function _tls_integer_bytes(value::Unsigned, len::Int)::Vector{UInt8}
    bytes = Vector{UInt8}(undef, len)
    mask = typeof(value)(0xff)
    for i in 1:len
        shift = 8 * (len - i)
        bytes[i] = UInt8((value >> shift) & mask)
    end
    bytes
end

function _tls_ip_address_byte_candidates(host::AbstractString)::Union{Vector{Vector{UInt8}},Nothing}
    value = _tls_ip_text(host)
    if occursin(':', value)
        ip = try
            Sockets.IPv6(value)
        catch
            return nothing
        end
        raw = UInt128(ip)
        candidates = [_tls_integer_bytes(raw, 16)]
        if raw >> 32 == UInt128(0xffff)
            push!(candidates, _tls_integer_bytes(UInt32(raw & UInt128(0xffffffff)), 4))
        end
        return candidates
    end

    _host_is_ip(value) || return nothing
    ip = try
        Sockets.IPv4(value)
    catch
        return nothing
    end
    [_tls_integer_bytes(UInt32(ip), 4)]
end

struct _MbedTLSAsn1Buf
    tag::Cint
    len::Csize_t
    p::Ptr{UInt8}
end

struct _MbedTLSX509CrtRawPrefix
    own_buffer::Cint
    raw::_MbedTLSAsn1Buf
end

struct _Asn1TLV
    tag::UInt8
    content_start::Int
    content_stop::Int
    next::Int
end

const _TLS_SUBJECT_ALT_NAME_OID = UInt8[0x55, 0x1d, 0x11]

function _asn1_tlv(bytes::AbstractVector{UInt8}, offset::Int, stop::Int)::_Asn1TLV
    firstindex(bytes) == 1 || throw(ArgumentError("ASN.1 buffers must be one-indexed"))
    offset <= stop || throw(ArgumentError("truncated ASN.1 value"))
    tag = bytes[offset]
    offset += 1
    offset <= stop || throw(ArgumentError("truncated ASN.1 length"))
    len_byte = bytes[offset]
    offset += 1
    len::Int = 0
    if len_byte & 0x80 == 0
        len = Int(len_byte)
    else
        len_len = Int(len_byte & 0x7f)
        0 < len_len <= sizeof(Int) || throw(ArgumentError("invalid ASN.1 length"))
        offset + len_len - 1 <= stop || throw(ArgumentError("truncated ASN.1 length"))
        for _ in 1:len_len
            len = (len << 8) | Int(bytes[offset])
            offset += 1
        end
    end
    len <= stop - offset + 1 || throw(ArgumentError("truncated ASN.1 content"))
    _Asn1TLV(tag, offset, offset + len - 1, offset + len)
end

function _asn1_expect(bytes::AbstractVector{UInt8}, offset::Int, stop::Int, tag::UInt8)::_Asn1TLV
    tlv = _asn1_tlv(bytes, offset, stop)
    tlv.tag == tag || throw(ArgumentError("unexpected ASN.1 tag"))
    tlv
end

function _asn1_content_equals(bytes::AbstractVector{UInt8}, tlv::_Asn1TLV,
                              expected::AbstractVector{UInt8})::Bool
    length(expected) == tlv.content_stop - tlv.content_start + 1 || return false
    for (i, byte) in pairs(expected)
        bytes[tlv.content_start + i - 1] == byte || return false
    end
    true
end

function _tls_find_subject_alt_name_extn_value(bytes::AbstractVector{UInt8},
                                               extensions::_Asn1TLV)::Union{Vector{UInt8},Nothing}
    pos = extensions.content_start
    while pos <= extensions.content_stop
        ext = _asn1_expect(bytes, pos, extensions.content_stop, 0x30)
        field = ext.content_start
        oid = _asn1_expect(bytes, field, ext.content_stop, 0x06)
        field = oid.next
        if field <= ext.content_stop && bytes[field] == 0x01
            field = _asn1_tlv(bytes, field, ext.content_stop).next
        end
        value = _asn1_expect(bytes, field, ext.content_stop, 0x04)
        if _asn1_content_equals(bytes, oid, _TLS_SUBJECT_ALT_NAME_OID)
            return bytes[value.content_start:value.content_stop]
        end
        pos = ext.next
    end
    nothing
end

function _tls_certificate_subject_alt_name(cert_der::AbstractVector{UInt8})::Union{Vector{UInt8},Nothing}
    cert = _asn1_expect(cert_der, firstindex(cert_der), lastindex(cert_der), 0x30)
    tbs = _asn1_expect(cert_der, cert.content_start, cert.content_stop, 0x30)
    field = tbs.content_start
    if field <= tbs.content_stop && cert_der[field] == 0xa0
        field = _asn1_tlv(cert_der, field, tbs.content_stop).next
    end
    for _ in 1:6
        field = _asn1_tlv(cert_der, field, tbs.content_stop).next
    end
    while field <= tbs.content_stop
        value = _asn1_tlv(cert_der, field, tbs.content_stop)
        if value.tag == 0xa3
            extensions = _asn1_expect(cert_der, value.content_start, value.content_stop, 0x30)
            return _tls_find_subject_alt_name_extn_value(cert_der, extensions)
        end
        field = value.next
    end
    nothing
end

function _tls_general_names_have_ip_san(general_names::AbstractVector{UInt8},
                                        candidates::Vector{Vector{UInt8}})::Bool
    names = _asn1_expect(general_names, firstindex(general_names), lastindex(general_names), 0x30)
    pos = names.content_start
    while pos <= names.content_stop
        name = _asn1_tlv(general_names, pos, names.content_stop)
        if name.tag == 0x87
            for candidate in candidates
                _asn1_content_equals(general_names, name, candidate) && return true
            end
        end
        pos = name.next
    end
    false
end

function _tls_certificate_has_ip_san(cert_der::AbstractVector{UInt8}, host::AbstractString)::Bool
    candidates = _tls_ip_address_byte_candidates(host)
    isnothing(candidates) && return false
    try
        general_names = _tls_certificate_subject_alt_name(cert_der)
        isnothing(general_names) && return false
        return _tls_general_names_have_ip_san(general_names, candidates)
    catch err
        err isa ArgumentError || rethrow()
        return false
    end
end

function _tls_peer_certificate_der(ctx::MbedTLS.SSLContext)::Vector{UInt8}
    data = ccall((:mbedtls_ssl_get_peer_cert, MbedTLS.libmbedtls), Ptr{Cvoid},
                 (Ptr{Cvoid},), ctx.data)
    data == C_NULL && return UInt8[]
    GC.@preserve ctx begin
        prefix = unsafe_load(Ptr{_MbedTLSX509CrtRawPrefix}(data))
        prefix.raw.p == C_NULL && return UInt8[]
        len = Int(prefix.raw.len)
        len <= 0 && return UInt8[]
        return copy(unsafe_wrap(Vector{UInt8}, prefix.raw.p, len; own=false))
    end
end

function _tls_peer_verify_error(hostname::AbstractString)
    message = "TLS certificate does not contain an IP subjectAltName matching $hostname"
    Base.IOError(message, MbedTLS.MBEDTLS_ERR_SSL_PEER_VERIFY_FAILED)
end

function _tls_verify_info(flags::UInt32)::String
    buf = Base.StringVector(1000)
    ret = ccall((:mbedtls_x509_crt_verify_info, MbedTLS.libmbedx509), Cint,
                (Ptr{Cvoid}, Csize_t, Cstring, UInt32),
                buf, length(buf), "", flags)
    ret < 0 && return "certificate verification failed"
    resize!(buf, something(findfirst(iszero, buf), ret + 1) - 1)
    strip(String(buf))
end

function _tls_verify_peer_chain!(ctx::MbedTLS.SSLContext)
    flags = ccall((:mbedtls_ssl_get_verify_result, MbedTLS.libmbedtls), UInt32,
                  (Ptr{Cvoid},), ctx.data)
    flags == 0 && return nothing
    info = _tls_verify_info(flags)
    message = isempty(info) ? "TLS certificate verification failed" :
              "TLS certificate verification failed: $info"
    throw(Base.IOError(message, MbedTLS.MBEDTLS_ERR_SSL_PEER_VERIFY_FAILED))
end

function _tls_verify_ip_san!(ctx::MbedTLS.SSLContext, hostname::AbstractString)
    _tls_certificate_has_ip_san(_tls_peer_certificate_der(ctx), hostname) ||
        throw(_tls_peer_verify_error(hostname))
    nothing
end

function _tls_first_for_connection(opts::ConnectOptions, scheme::AbstractString)::Bool
    isnothing(opts.tls_first) ? scheme == "tls" : opts.tls_first
end

function _tls_authmode(opts::ConnectOptions)::Int
    opts.tls_verify ? MbedTLS.MBEDTLS_SSL_VERIFY_REQUIRED : MbedTLS.MBEDTLS_SSL_VERIFY_NONE
end

function _tls_config(opts::ConnectOptions, authmode::Int=_tls_authmode(opts))
    entropy = MbedTLS.Entropy()
    rng = MbedTLS.CtrDrbg()
    MbedTLS.seed!(rng, entropy)
    conf = MbedTLS.SSLConfig()
    MbedTLS.config_defaults!(conf)
    MbedTLS.rng!(conf, rng)
    MbedTLS.authmode!(conf, authmode)
    if isnothing(opts.tls_ca_path)
        MbedTLS.ca_chain!(conf)
    else
        MbedTLS.ca_chain!(conf, MbedTLS.crt_parse_file(opts.tls_ca_path))
    end
    if !isnothing(opts.tls_cert_path) && !isnothing(opts.tls_key_path)
        MbedTLS.own_cert!(conf, MbedTLS.crt_parse_file(opts.tls_cert_path), MbedTLS.parse_keyfile(opts.tls_key_path))
    end
    conf
end
