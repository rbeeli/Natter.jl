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
const _NKEY_DECORATED_BLOCK_RE = r"-{3,}[^\r\n]*-{3,}\r?\n([\w\-.=]+)\r?\n-{3,}[^\r\n]*-{3,}"

_nkey_valid_public_prefix(prefix::UInt8)::Bool = prefix in _NKEY_PUBLIC_PREFIXES

function _nkey_base32_value(byte::UInt8)::UInt8
    if UInt8('A') <= byte <= UInt8('Z')
        return byte - UInt8('A')
    elseif UInt8('2') <= byte <= UInt8('7')
        return byte - UInt8('2') + 26
    end
    throw(ArgumentError("invalid nkey base32 character"))
end

function _nkey_base32_decode(encoded::AbstractString)::Vector{UInt8}
    input = strip(String(encoded))
    isempty(input) && throw(ArgumentError("nkey value cannot be empty"))
    out = UInt8[]
    sizehint!(out, (ncodeunits(input) * 5) ÷ 8)
    acc::UInt32 = 0
    bits::Int = 0
    @inbounds for byte in codeunits(input)
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
    out
end

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

function _nkey_decode_checked(encoded::AbstractString, label::String)::Vector{UInt8}
    raw = _nkey_base32_decode(encoded)
    try
        length(raw) >= 4 || throw(ArgumentError("invalid $label encoding"))
        data = raw[1:(end - 2)]
        expected = UInt16(raw[end - 1]) | (UInt16(raw[end]) << 8)
        actual = _nkey_crc16(data)
        actual == expected || throw(ArgumentError("invalid $label checksum"))
        data
    finally
        fill!(raw, 0)
    end
end

function _nkey_decode_seed(encoded::AbstractString)
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
        fill!(raw, 0)
    end
end

function _validate_nkey_seed(encoded::AbstractString)
    decoded = _nkey_decode_seed(encoded)
    fill!(decoded.seed, 0)
    nothing
end

function _nkey_decode_public(encoded::AbstractString)
    raw = _nkey_decode_checked(encoded, "nkey public key")
    length(raw) == 33 || throw(ArgumentError("invalid nkey public key length"))
    prefix = raw[1] & UInt8(0xf8)
    _nkey_valid_public_prefix(prefix) || throw(ArgumentError("invalid nkey public prefix"))
    (public_prefix=prefix, key=raw[2:end])
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

function _nkey_public_from_seed(encoded_seed::AbstractString)::String
    decoded = _nkey_decode_seed(encoded_seed)
    seed = decoded.seed
    public_key = UInt8[]
    secret_key = UInt8[]
    try
        public_key, secret_key = _ed25519_keypair_from_seed(seed)
        _nkey_encode_public(decoded.public_prefix, public_key)
    finally
        fill!(seed, 0)
        fill!(secret_key, 0)
    end
end

function _nkey_sign(encoded_seed::AbstractString, message::AbstractVector{UInt8})::Vector{UInt8}
    decoded = _nkey_decode_seed(encoded_seed)
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
        fill!(seed, 0)
        fill!(secret_key, 0)
    end
end

_nkey_sign(encoded_seed::AbstractString, message::AbstractString) =
    _nkey_sign(encoded_seed, Vector{UInt8}(codeunits(message)))

function _base64url_encode_raw(bytes::AbstractVector{UInt8})::String
    encoded = base64encode(bytes)
    encoded = replace(encoded, '+' => '-', '/' => '_')
    replace(encoded, r"=+$" => "")
end

function _read_auth_text(path::AbstractString)::String
    bytes = read(String(path))
    try
        return String(bytes)
    finally
        fill!(bytes, 0)
    end
end

function _decorated_blocks(text::AbstractString)::Vector{String}
    blocks = String[]
    for m in eachmatch(_NKEY_DECORATED_BLOCK_RE, text)
        push!(blocks, strip(only(m.captures)))
    end
    blocks
end

function _extract_jwt(text::AbstractString)::String
    blocks = _decorated_blocks(text)
    jwt = isempty(blocks) ? strip(String(text)) : first(blocks)
    isempty(jwt) && throw(ArgumentError("JWT credentials do not contain a user JWT"))
    jwt
end

function _extract_nkey_seed(text::AbstractString)::String
    blocks = _decorated_blocks(text)
    if length(blocks) > 1
        seed = blocks[2]
    else
        seed = nothing
        for line in eachline(IOBuffer(String(text)))
            candidate = strip(line)
            if startswith(candidate, "SO") || startswith(candidate, "SA") ||
               startswith(candidate, "SU")
                seed = candidate
                break
            end
        end
    end
    isnothing(seed) && throw(ArgumentError("credentials do not contain an nkey seed"))
    _validate_nkey_seed(seed)
    seed
end

function _resolve_credentials(opts::ConnectOptions)
    if !isnothing(opts.credentials)
        text = String(opts.credentials)
        return (jwt=_extract_jwt(text), seed=_extract_nkey_seed(text))
    elseif !isnothing(opts.credentials_path)
        text = _read_auth_text(opts.credentials_path)
        return (jwt=_extract_jwt(text), seed=_extract_nkey_seed(text))
    end
    nothing
end

function _resolve_jwt(opts::ConnectOptions)::String
    credentials = _resolve_credentials(opts)
    isnothing(credentials) || return credentials.jwt
    if !isnothing(opts.jwt)
        jwt = strip(String(opts.jwt))
    elseif !isnothing(opts.jwt_path)
        jwt = _extract_jwt(_read_auth_text(opts.jwt_path))
    else
        throw(ArgumentError("JWT authentication requires jwt, jwt_path, credentials, or credentials_path"))
    end
    isempty(jwt) && throw(ArgumentError("JWT cannot be empty"))
    jwt
end

function _resolve_signature_seed(opts::ConnectOptions, credential_seed::Union{String,Nothing}=nothing)::String
    if !isnothing(credential_seed)
        return credential_seed
    elseif !isnothing(opts.nkey_seed)
        seed = strip(String(opts.nkey_seed))
    elseif !isnothing(opts.nkey_seed_path)
        seed = _extract_nkey_seed(_read_auth_text(opts.nkey_seed_path))
    else
        throw(ArgumentError("nkey/JWT authentication requires a signature source"))
    end
    isempty(seed) && throw(ArgumentError("nkey seed cannot be empty"))
    seed
end

function _signature_bytes(signature)::Vector{UInt8}
    signature isa AbstractVector{UInt8} ||
        throw(ArgumentError("signature_cb must return a 64-byte Vector{UInt8} signature"))
    bytes = Vector{UInt8}(signature)
    length(bytes) == 64 ||
        throw(ArgumentError("signature_cb must return a 64-byte Ed25519 signature"))
    bytes
end

function _resolve_signature(opts::ConnectOptions, nonce::String;
                            credential_seed::Union{String,Nothing}=nothing)::String
    nonce_bytes = Vector{UInt8}(codeunits(nonce))
    if !isnothing(opts.signature_cb)
        raw = _signature_bytes(opts.signature_cb(nonce_bytes))
    else
        raw = _nkey_sign(_resolve_signature_seed(opts, credential_seed), nonce_bytes)
    end
    _base64url_encode_raw(raw)
end

function _resolve_nkey(opts::ConnectOptions)::String
    if !isnothing(opts.nkey)
        nkey = strip(String(opts.nkey))
        _nkey_decode_public(nkey)
        if isnothing(opts.signature_cb) &&
           (!isnothing(opts.nkey_seed) || !isnothing(opts.nkey_seed_path))
            derived = _nkey_public_from_seed(_resolve_signature_seed(opts))
            derived == nkey || throw(ArgumentError("nkey does not match nkey seed"))
        end
        return nkey
    end
    _nkey_public_from_seed(_resolve_signature_seed(opts))
end

function _connect_option_has_jwt(opts::ConnectOptions)::Bool
    !isnothing(opts.jwt) || !isnothing(opts.jwt_path) ||
        !isnothing(opts.credentials) || !isnothing(opts.credentials_path)
end

function _connect_option_has_nkey_jwt(opts::ConnectOptions)::Bool
    _connect_option_has_jwt(opts) || !isnothing(opts.nkey) ||
        !isnothing(opts.nkey_seed) || !isnothing(opts.nkey_seed_path) ||
        !isnothing(opts.signature_cb)
end

function _connect_nonce(info::ServerInfo)::String
    nonce = info.nonce
    if isnothing(nonce) || isempty(nonce)
        throw(UnsupportedFeatureError("nkey/JWT authentication requires server nonce support"))
    end
    nonce
end

function _connect_nkey_jwt_fields(opts::ConnectOptions, info::ServerInfo)
    _connect_option_has_nkey_jwt(opts) || return (jwt=nothing, nkey=nothing, sig=nothing)
    nonce = _connect_nonce(info)
    if _connect_option_has_jwt(opts)
        credentials = _resolve_credentials(opts)
        jwt = isnothing(credentials) ? _resolve_jwt(opts) : credentials.jwt
        seed = isnothing(credentials) ? nothing : credentials.seed
        return (jwt=jwt, nkey=nothing, sig=_resolve_signature(opts, nonce; credential_seed=seed))
    end
    (jwt=nothing, nkey=_resolve_nkey(opts), sig=_resolve_signature(opts, nonce))
end
