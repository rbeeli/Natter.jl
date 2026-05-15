const _NKEY_PREFIX_SEED = UInt8(18 << 3)
const _NKEY_PREFIX_OPERATOR = UInt8(14 << 3)
const _NKEY_PREFIX_SERVER = UInt8(13 << 3)
const _NKEY_PREFIX_CLUSTER = UInt8(2 << 3)
const _NKEY_PREFIX_ACCOUNT = UInt8(0)
const _NKEY_PREFIX_USER = UInt8(20 << 3)
const _NKEY_PUBLIC_PREFIXES = (
    _NKEY_PREFIX_OPERATOR,
    _NKEY_PREFIX_SERVER,
    _NKEY_PREFIX_CLUSTER,
    _NKEY_PREFIX_ACCOUNT,
    _NKEY_PREFIX_USER,
)
const _NKEY_BASE32_ALPHABET = codeunits("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567")

_nkey_valid_public_prefix(prefix::UInt8)::Bool = prefix in _NKEY_PUBLIC_PREFIXES
_nkey_is_user_public_prefix(prefix::UInt8)::Bool = prefix == _NKEY_PREFIX_USER

_ascii_space(byte::UInt8)::Bool =
    byte == UInt8(' ') || byte == UInt8('\t') ||
    byte == UInt8('\r') || byte == UInt8('\n')

function _stripped_bytes(bytes::AbstractVector{UInt8})
    lo = firstindex(bytes)
    hi = lastindex(bytes)
    while lo <= hi && _ascii_space(bytes[lo])
        lo += 1
    end
    while hi >= lo && _ascii_space(bytes[hi])
        hi -= 1
    end
    @view bytes[lo:hi]
end

_stripped_bytes(value::AbstractString) = _stripped_bytes(codeunits(value))

function _nkey_base32_value(byte::UInt8)::UInt8
    if UInt8('A') <= byte <= UInt8('Z')
        return byte - UInt8('A')
    elseif UInt8('2') <= byte <= UInt8('7')
        return byte - UInt8('2') + 26
    end
    throw(ArgumentError("invalid nkey base32 character"))
end

function _nkey_base32_decode!(out::Vector{UInt8}, encoded)::Vector{UInt8}
    input = _stripped_bytes(encoded)
    _wipe_bytes!(out)
    empty!(out)
    isempty(input) && throw(ArgumentError("nkey value cannot be empty"))
    sizehint!(out, (length(input) * 5) ÷ 8)
    acc::UInt32 = 0
    bits::Int = 0
    try
        @inbounds for byte in input
            byte == UInt8('=') && throw(ArgumentError("nkey base32 padding is not allowed"))
            value = UInt32(_nkey_base32_value(byte))
            acc = (acc << 5) | value
            bits += 5
            while bits >= 8
                push!(out, UInt8((acc >> (bits - 8)) & 0xff))
                bits -= 8
                acc &= bits == 0 ? UInt32(0) : (UInt32(1) << bits) - UInt32(1)
            end
        end
        bits > 0 && acc != 0 && throw(ArgumentError("invalid nkey base32 trailing bits"))
        return out
    catch
        _wipe_bytes!(out)
        rethrow()
    end
end

_nkey_base32_decode(encoded)::Vector{UInt8} = _nkey_base32_decode!(UInt8[], encoded)

function _nkey_base32_encode(bytes::AbstractVector{UInt8})::String
    out = Vector{UInt8}()
    sizehint!(out, cld(length(bytes) * 8, 5))
    acc::UInt32 = 0
    bits::Int = 0
    @inbounds for byte in bytes
        acc = (acc << 8) | UInt32(byte)
        bits += 8
        while bits >= 5
            index = Int((acc >> (bits - 5)) & 0x1f) + 1
            push!(out, _NKEY_BASE32_ALPHABET[index])
            bits -= 5
            acc &= bits == 0 ? UInt32(0) : (UInt32(1) << bits) - UInt32(1)
        end
    end
    if bits > 0
        index = Int((acc << (5 - bits)) & 0x1f) + 1
        push!(out, _NKEY_BASE32_ALPHABET[index])
    end
    String(out)
end

function _nkey_crc16(bytes::AbstractVector{UInt8})::UInt16
    crc = UInt16(0)
    @inbounds for byte in bytes
        crc ⊻= UInt16(byte) << 8
        for _ in 1:8
            if (crc & UInt16(0x8000)) != 0
                crc = (crc << 1) ⊻ UInt16(0x1021)
            else
                crc <<= 1
            end
        end
    end
    crc
end

function _nkey_decode_checked(encoded, label::String)::Vector{UInt8}
    raw = _nkey_base32_decode(encoded)
    try
        length(raw) >= 4 || throw(ArgumentError("invalid $label encoding"))
        data = @view raw[1:(end - 2)]
        expected = UInt16(raw[end - 1]) | (UInt16(raw[end]) << 8)
        actual = _nkey_crc16(data)
        actual == expected || throw(ArgumentError("invalid $label checksum"))
        copy(data)
    finally
        _wipe_bytes!(raw)
    end
end

function _nkey_decode_seed(encoded)
    raw = _nkey_decode_checked(encoded, "nkey seed")
    try
        length(raw) == 34 || throw(ArgumentError("invalid nkey seed length"))
        prefix = raw[1] & UInt8(0xf8)
        public_prefix = ((raw[1] & UInt8(0x07)) << 5) | ((raw[2] & UInt8(0xf8)) >> 3)
        prefix == _NKEY_PREFIX_SEED || throw(ArgumentError("invalid nkey seed prefix"))
        _nkey_valid_public_prefix(public_prefix) ||
            throw(ArgumentError("invalid nkey public prefix in seed"))
        (public_prefix=public_prefix, seed=raw[3:end])
    finally
        _wipe_bytes!(raw)
    end
end

function _validate_nkey_seed(encoded)
    decoded = _nkey_decode_seed(encoded)
    _wipe_bytes!(decoded.seed)
    nothing
end

function _nkey_decode_user_seed(encoded)
    decoded = _nkey_decode_seed(encoded)
    if !_nkey_is_user_public_prefix(decoded.public_prefix)
        _wipe_bytes!(decoded.seed)
        throw(ArgumentError("nkey seed must be a user NKEY seed"))
    end
    decoded
end

function _validate_user_nkey_seed(encoded)
    decoded = _nkey_decode_user_seed(encoded)
    _wipe_bytes!(decoded.seed)
    nothing
end

function _nkey_decode_public(encoded::AbstractString)
    raw = _nkey_decode_checked(encoded, "nkey public key")
    try
        length(raw) == 33 || throw(ArgumentError("invalid nkey public key length"))
        prefix = raw[1] & UInt8(0xf8)
        _nkey_valid_public_prefix(prefix) || throw(ArgumentError("invalid nkey public prefix"))
        (public_prefix=prefix, key=raw[2:end])
    finally
        _wipe_bytes!(raw)
    end
end

function _nkey_decode_user_public(encoded::AbstractString)
    decoded = _nkey_decode_public(encoded)
    _nkey_is_user_public_prefix(decoded.public_prefix) ||
        throw(ArgumentError("nkey public key must be a user NKEY"))
    decoded
end

function _nkey_encode_public(public_prefix::UInt8, key::AbstractVector{UInt8})::String
    _nkey_valid_public_prefix(public_prefix) || throw(ArgumentError("invalid nkey public prefix"))
    length(key) == 32 || throw(ArgumentError("nkey public key must be 32 bytes"))
    raw = Vector{UInt8}(undef, 1 + length(key) + 2)
    raw[1] = public_prefix
    copyto!(raw, 2, key, firstindex(key), length(key))
    crc = _nkey_crc16(@view raw[1:(end - 2)])
    raw[end - 1] = UInt8(crc & UInt16(0x00ff))
    raw[end] = UInt8(crc >> 8)
    _nkey_base32_encode(raw)
end

function _sodium_init()
    rc = ccall((:sodium_init, libsodium_jll.libsodium), Cint, ())
    rc >= 0 || error("libsodium initialization failed")
    nothing
end

function _ed25519_keypair_from_seed(seed::Vector{UInt8})
    length(seed) == 32 || throw(ArgumentError("nkey seed payload must be 32 bytes"))
    _sodium_init()
    public_key = Vector{UInt8}(undef, 32)
    secret_key = Vector{UInt8}(undef, 64)
    rc = ccall((:crypto_sign_seed_keypair, libsodium_jll.libsodium), Cint,
               (Ptr{UInt8}, Ptr{UInt8}, Ptr{UInt8}),
               public_key, secret_key, seed)
    rc == 0 || error("ed25519 key derivation failed")
    public_key, secret_key
end

function _nkey_public_from_seed(encoded_seed)::String
    decoded = _nkey_decode_user_seed(encoded_seed)
    seed = decoded.seed
    public_key = UInt8[]
    secret_key = UInt8[]
    try
        public_key, secret_key = _ed25519_keypair_from_seed(seed)
        _nkey_encode_public(decoded.public_prefix, public_key)
    finally
        _wipe_bytes!(seed)
        _wipe_bytes!(secret_key)
    end
end

function _nkey_sign(encoded_seed, message::AbstractVector{UInt8})::Vector{UInt8}
    decoded = _nkey_decode_user_seed(encoded_seed)
    seed = decoded.seed
    secret_key = UInt8[]
    try
        _, secret_key = _ed25519_keypair_from_seed(seed)
        signature = Vector{UInt8}(undef, 64)
        signature_len = Ref{Culonglong}(0)
        rc = ccall((:crypto_sign_detached, libsodium_jll.libsodium), Cint,
                   (Ptr{UInt8}, Ref{Culonglong}, Ptr{UInt8}, Culonglong, Ptr{UInt8}),
                   signature, signature_len, message, Culonglong(length(message)), secret_key)
        rc == 0 || error("ed25519 signing failed")
        signature_len[] == 64 || error("ed25519 signing returned an unexpected signature length")
        signature
    finally
        _wipe_bytes!(seed)
        _wipe_bytes!(secret_key)
    end
end

_nkey_sign(encoded_seed, message::AbstractString) =
    _nkey_sign(encoded_seed, Vector{UInt8}(codeunits(message)))

function _base64url_encode_raw(bytes::AbstractVector{UInt8})::String
    encoded = base64encode(bytes)
    encoded = replace(encoded, '+' => '-', '/' => '_')
    replace(encoded, r"=+$" => "")
end

function _secret_data(value::SecretBytes)
    value
end

function _secret_data(value::AbstractVector{UInt8})
    value
end

function _secret_data(value::AbstractString)
    codeunits(value)
end

function _secret_to_string(secret::SecretBytes)::String
    String(Vector{UInt8}(secret))
end

function _secret_to_string(value::AbstractString)::String
    String(value)
end

function _read_auth_secret(path::AbstractString)::SecretBytes
    SecretBytes(read(String(path)); take=true)
end

function _range_contains(bytes::AbstractVector{UInt8}, lo::Int, hi::Int,
                         needle::AbstractString)::Bool
    needle_bytes = codeunits(needle)
    n = length(needle_bytes)
    n == 0 && return true
    n <= hi - lo + 1 || return false
    @inbounds for start in lo:(hi - n + 1)
        matched = true
        for offset in 1:n
            if bytes[start + offset - 1] != needle_bytes[offset]
                matched = false
                break
            end
        end
        matched && return true
    end
    false
end

function _decorated_line(bytes::AbstractVector{UInt8}, lo::Int, hi::Int)::Bool
    hi - lo + 1 >= 6 || return false
    bytes[lo] == UInt8('-') && bytes[lo + 1] == UInt8('-') &&
        bytes[lo + 2] == UInt8('-') &&
        bytes[hi] == UInt8('-') && bytes[hi - 1] == UInt8('-') &&
        bytes[hi - 2] == UInt8('-')
end

function _line_bounds(bytes::AbstractVector{UInt8}, start::Int)
    i = start
    hi = lastindex(bytes)
    while i <= hi && bytes[i] != UInt8('\n')
        i += 1
    end
    line_hi = i - 1
    if line_hi >= start && bytes[line_hi] == UInt8('\r')
        line_hi -= 1
    end
    (start, line_hi, i + 1)
end

function _decorated_blocks(text)::Vector{SecretBytes}
    bytes = _secret_data(text)
    blocks = SecretBytes[]
    in_block = false
    content = nothing
    i = firstindex(bytes)
    hi = lastindex(bytes)
    while i <= hi
        line_lo, line_hi, next_i = _line_bounds(bytes, i)
        stripped = _stripped_bytes(@view bytes[line_lo:line_hi])
        stripped_lo = firstindex(stripped)
        stripped_hi = lastindex(stripped)
        if !isempty(stripped) && _decorated_line(stripped, stripped_lo, stripped_hi)
            if _range_contains(stripped, stripped_lo, stripped_hi, "BEGIN")
                _wipe_secret!(content)
                in_block = true
                content = nothing
            elseif in_block && _range_contains(stripped, stripped_lo, stripped_hi, "END")
                isnothing(content) || push!(blocks, content)
                content = nothing
                in_block = false
            end
        elseif in_block && isnothing(content) && !isempty(stripped)
            content = SecretBytes(stripped)
        end
        i = next_i
    end
    _wipe_secret!(content)
    blocks
end

function _wipe_except!(blocks::Vector{SecretBytes}, keep::Int)
    for i in eachindex(blocks)
        i == keep || _wipe_secret!(blocks[i])
    end
    nothing
end

function _extract_jwt(text)::SecretBytes
    blocks = _decorated_blocks(text)
    jwt =
        if isempty(blocks)
            SecretBytes(_stripped_bytes(text))
        else
            _wipe_except!(blocks, firstindex(blocks))
            first(blocks)
        end
    if isempty(jwt)
        _wipe_secret!(jwt)
        throw(ArgumentError("JWT credentials do not contain a user JWT"))
    end
    jwt
end

function _find_nkey_seed_line(text)
    bytes = _secret_data(text)
    i = firstindex(bytes)
    hi = lastindex(bytes)
    while i <= hi
        line_lo, line_hi, next_i = _line_bounds(bytes, i)
        candidate = _stripped_bytes(@view bytes[line_lo:line_hi])
        candidate_lo = firstindex(candidate)
        candidate_hi = lastindex(candidate)
        if length(candidate) >= 2 &&
           candidate[candidate_lo] == UInt8('S') &&
           candidate[candidate_lo + 1] == UInt8('U')
            return SecretBytes(candidate)
        end
        i = next_i
    end
    nothing
end

function _extract_nkey_seed(text)::SecretBytes
    blocks = _decorated_blocks(text)
    seed =
        if length(blocks) > 1
            _wipe_except!(blocks, 2)
            blocks[2]
        else
            foreach(_wipe_secret!, blocks)
            _find_nkey_seed_line(text)
        end
    isnothing(seed) && throw(ArgumentError("credentials do not contain an nkey seed"))
    try
        _validate_user_nkey_seed(seed)
        return seed
    catch
        _wipe_secret!(seed)
        rethrow()
    end
end

struct ResolvedAuth
    token::Union{SecretBytes,Nothing}
    user::Union{String,Nothing}
    password::Union{SecretBytes,Nothing}
    jwt::Union{String,Nothing}
    nkey::Union{String,Nothing}
    sig::Union{String,Nothing}
end

const NO_RESOLVED_AUTH = ResolvedAuth(nothing, nothing, nothing, nothing, nothing, nothing)

_url_has_auth(url_user, url_pass)::Bool = !isnothing(url_user) || !isnothing(url_pass)

function _resolved_url_auth(url_user, url_pass)::ResolvedAuth
    _url_has_auth(url_user, url_pass) || return NO_RESOLVED_AUTH
    if isnothing(url_pass)
        return ResolvedAuth(TokenAuth(url_user).token, nothing, nothing, nothing, nothing, nothing)
    end
    auth = UserPassAuth(url_user, url_pass)
    ResolvedAuth(nothing, auth.user, auth.password, nothing, nothing, nothing)
end

function _reject_url_auth(auth::AbstractAuth, url_user, url_pass)
    _url_has_auth(url_user, url_pass) || return nothing
    throw(ArgumentError("URL userinfo cannot be combined with $(nameof(typeof(auth)))"))
end

function _with_resolved_credentials(f, auth::CredentialsAuth)
    text = nothing
    jwt = nothing
    seed = nothing
    try
        if !isnothing(auth.credentials)
            text = auth.credentials
        elseif !isnothing(auth.path)
            text = _read_auth_secret(auth.path)
        else
            throw(ArgumentError("CredentialsAuth requires credentials or path"))
        end
        jwt = _extract_jwt(text)
        seed = _extract_nkey_seed(text)
        return f(jwt, seed)
    finally
        _wipe_secret!(jwt)
        _wipe_secret!(seed)
        auth.credentials === text || _wipe_secret!(text)
    end
end

function _with_resolved_jwt(f, auth::JwtAuth)
    jwt = nothing
    text = nothing
    try
        if !isnothing(auth.jwt)
            jwt = SecretBytes(_stripped_bytes(auth.jwt))
        elseif !isnothing(auth.jwt_path)
            text = _read_auth_secret(auth.jwt_path)
            jwt = _extract_jwt(text)
        else
            throw(ArgumentError("JwtAuth requires jwt or jwt_path"))
        end
        isempty(jwt) && throw(ArgumentError("JWT cannot be empty"))
        return f(jwt)
    finally
        _wipe_secret!(jwt)
        _wipe_secret!(text)
    end
end

function _with_signature_seed(f, seed::Union{SecretBytes,Nothing},
                              seed_path::Union{String,Nothing}, credential_seed=nothing)
    text = nothing
    resolved_seed = nothing
    try
        if !isnothing(credential_seed)
            resolved_seed = credential_seed
        elseif !isnothing(seed)
            resolved_seed = seed
        elseif !isnothing(seed_path)
            text = _read_auth_secret(seed_path)
            resolved_seed = _extract_nkey_seed(text)
        else
            throw(ArgumentError("NKey/JWT authentication requires a signature source"))
        end
        isempty(resolved_seed) && throw(ArgumentError("nkey seed cannot be empty"))
        return f(resolved_seed)
    finally
        resolved_seed === credential_seed || resolved_seed === seed || _wipe_secret!(resolved_seed)
        _wipe_secret!(text)
    end
end

function _signature_bytes(signature)::Vector{UInt8}
    signature isa AbstractVector{UInt8} ||
        throw(ArgumentError("signature callback must return a 64-byte Vector{UInt8} signature"))
    bytes = Vector{UInt8}(signature)
    length(bytes) == 64 ||
        throw(ArgumentError("signature callback must return a 64-byte Ed25519 signature"))
    bytes
end

function _resolve_signature(signature_cb, seed::Union{SecretBytes,Nothing},
                            seed_path::Union{String,Nothing}, nonce::String;
                            credential_seed=nothing)::String
    nonce_bytes = Vector{UInt8}(codeunits(nonce))
    raw = UInt8[]
    if !isnothing(signature_cb)
        raw = _signature_bytes(signature_cb(nonce_bytes))
    else
        raw = _with_signature_seed(seed, seed_path, credential_seed) do resolved_seed
            _nkey_sign(resolved_seed, nonce_bytes)
        end
    end
    try
        _base64url_encode_raw(raw)
    finally
        _wipe_bytes!(nonce_bytes)
        _wipe_bytes!(raw)
    end
end

function _normalize_user_nkey(nkey)::String
    normalized = strip(String(nkey))
    isempty(normalized) && throw(ArgumentError("nkey cannot be empty"))
    _nkey_decode_user_public(normalized)
    normalized
end

function _resolve_seed_nkey(nkey::Union{String,Nothing}, seed::SecretBytes)::String
    derived = _nkey_public_from_seed(seed)
    if !isnothing(nkey)
        nkey = _normalize_user_nkey(nkey)
        derived == nkey || throw(ArgumentError("nkey does not match nkey seed"))
        return nkey
    end
    derived
end

function _connect_nonce(info::ServerInfo)::String
    nonce = info.nonce
    if isnothing(nonce) || isempty(nonce)
        throw(UnsupportedFeatureError("nkey/JWT authentication requires server nonce support"))
    end
    nonce
end

function _resolve_auth(auth::NoAuth, _request::AuthRequest, url_user, url_pass)::ResolvedAuth
    _resolved_url_auth(url_user, url_pass)
end

function _resolve_auth(auth::TokenAuth, _request::AuthRequest, url_user, url_pass)::ResolvedAuth
    _reject_url_auth(auth, url_user, url_pass)
    ResolvedAuth(auth.token, nothing, nothing, nothing, nothing, nothing)
end

function _resolve_auth(auth::UserPassAuth, _request::AuthRequest, url_user, url_pass)::ResolvedAuth
    _reject_url_auth(auth, url_user, url_pass)
    ResolvedAuth(nothing, auth.user, auth.password, nothing, nothing, nothing)
end

function _resolve_auth(auth::NKeyAuth, request::AuthRequest, url_user, url_pass)::ResolvedAuth
    _reject_url_auth(auth, url_user, url_pass)
    nonce = _connect_nonce(request.info)
    if !isnothing(auth.signature_cb)
        nkey = _normalize_user_nkey(auth.nkey)
        sig = _resolve_signature(auth.signature_cb, nothing, nothing, nonce)
        return ResolvedAuth(nothing, nothing, nothing, nothing, nkey, sig)
    end
    _with_signature_seed(auth.seed, auth.seed_path) do seed
        nkey = _resolve_seed_nkey(auth.nkey, seed)
        sig = _resolve_signature(nothing, seed, nothing, nonce)
        ResolvedAuth(nothing, nothing, nothing, nothing, nkey, sig)
    end
end

function _resolve_auth(auth::JwtAuth, request::AuthRequest, url_user, url_pass)::ResolvedAuth
    _reject_url_auth(auth, url_user, url_pass)
    nonce = _connect_nonce(request.info)
    if !isnothing(auth.signature_cb)
        isnothing(auth.nkey) || _normalize_user_nkey(auth.nkey)
        sig = _resolve_signature(auth.signature_cb, nothing, nothing, nonce)
        return _with_resolved_jwt(auth) do jwt
            ResolvedAuth(nothing, nothing, nothing, _secret_to_string(jwt), nothing, sig)
        end
    end
    _with_signature_seed(auth.seed, auth.seed_path) do seed
        isnothing(auth.nkey) || _resolve_seed_nkey(auth.nkey, seed)
        sig = _resolve_signature(nothing, seed, nothing, nonce)
        _with_resolved_jwt(auth) do jwt
            ResolvedAuth(nothing, nothing, nothing, _secret_to_string(jwt), nothing, sig)
        end
    end
end

function _resolve_auth(auth::CredentialsAuth, request::AuthRequest, url_user, url_pass)::ResolvedAuth
    _reject_url_auth(auth, url_user, url_pass)
    nonce = _connect_nonce(request.info)
    _with_resolved_credentials(auth) do jwt, seed
        sig = _resolve_signature(nothing, nothing, nothing, nonce; credential_seed=seed)
        ResolvedAuth(nothing, nothing, nothing, _secret_to_string(jwt), nothing, sig)
    end
end

function _resolve_auth(auth::CallbackAuth, request::AuthRequest, url_user, url_pass)::ResolvedAuth
    _reject_url_auth(auth, url_user, url_pass)
    resolved = auth.callback(request)
    resolved isa CallbackAuth &&
        throw(ArgumentError("CallbackAuth must return a concrete static auth value, not CallbackAuth"))
    resolved isa AbstractAuth ||
        throw(ArgumentError("CallbackAuth must return an AbstractAuth value"))
    _resolve_auth(resolved, request, nothing, nothing)
end

function _resolve_connect_auth(opts::ConnectOptions, request::AuthRequest, url_user, url_pass)::ResolvedAuth
    _resolve_auth(opts.auth, request, url_user, url_pass)
end
