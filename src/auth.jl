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

function _with_resolved_credentials(f, opts::ConnectOptions)
    text = nothing
    jwt = nothing
    seed = nothing
    try
        if !isnothing(opts.credentials)
            text = opts.credentials
        elseif !isnothing(opts.credentials_path)
            text = _read_auth_secret(opts.credentials_path)
        else
            return f(nothing)
        end
        jwt = _extract_jwt(text)
        seed = _extract_nkey_seed(text)
        return f((jwt=jwt, seed=seed))
    finally
        _wipe_secret!(jwt)
        _wipe_secret!(seed)
        opts.credentials === text || _wipe_secret!(text)
    end
end

function _with_resolved_jwt(f, opts::ConnectOptions)
    jwt = nothing
    text = nothing
    try
        if !isnothing(opts.jwt)
            jwt = SecretBytes(_stripped_bytes(opts.jwt))
        elseif !isnothing(opts.jwt_path)
            text = _read_auth_secret(opts.jwt_path)
            jwt = _extract_jwt(text)
        else
            throw(ArgumentError("JWT authentication requires jwt, jwt_path, credentials, or credentials_path"))
        end
        isempty(jwt) && throw(ArgumentError("JWT cannot be empty"))
        return f(jwt)
    finally
        _wipe_secret!(jwt)
        _wipe_secret!(text)
    end
end

function _resolve_jwt(opts::ConnectOptions)::String
    _with_resolved_credentials(opts) do credentials
        if !isnothing(credentials)
            return _secret_to_string(credentials.jwt)
        end
        _with_resolved_jwt(opts) do jwt
            _secret_to_string(jwt)
        end
    end
end

function _with_signature_seed(f, opts::ConnectOptions, credential_seed=nothing)
    text = nothing
    seed = nothing
    try
        if !isnothing(credential_seed)
            seed = credential_seed
        elseif !isnothing(opts.nkey_seed)
            seed = opts.nkey_seed
        elseif !isnothing(opts.nkey_seed_path)
            text = _read_auth_secret(opts.nkey_seed_path)
            seed = _extract_nkey_seed(text)
        else
            throw(ArgumentError("nkey/JWT authentication requires a signature source"))
        end
        isempty(seed) && throw(ArgumentError("nkey seed cannot be empty"))
        return f(seed)
    finally
        seed === credential_seed || seed === opts.nkey_seed || _wipe_secret!(seed)
        _wipe_secret!(text)
    end
end

function _signature_bytes(signature)::Vector{UInt8}
    signature isa AbstractVector{UInt8} ||
        throw(ArgumentError("signature_cb must return a 64-byte Vector{UInt8} signature"))
    bytes = Vector{UInt8}(signature)
    length(bytes) == 64 ||
        throw(ArgumentError("signature_cb must return a 64-byte Ed25519 signature"))
    bytes
end

function _resolve_signature(opts::ConnectOptions, nonce::String; credential_seed=nothing)::String
    nonce_bytes = Vector{UInt8}(codeunits(nonce))
    raw = UInt8[]
    if !isnothing(opts.signature_cb)
        raw = _signature_bytes(opts.signature_cb(nonce_bytes))
    else
        raw = _with_signature_seed(opts, credential_seed) do seed
            _nkey_sign(seed, nonce_bytes)
        end
    end
    try
        _base64url_encode_raw(raw)
    finally
        _wipe_bytes!(nonce_bytes)
        _wipe_bytes!(raw)
    end
end

function _resolve_nkey(opts::ConnectOptions)::String
    if !isnothing(opts.nkey)
        nkey = strip(String(opts.nkey))
        _nkey_decode_user_public(nkey)
        if isnothing(opts.signature_cb) &&
           (!isnothing(opts.nkey_seed) || !isnothing(opts.nkey_seed_path))
            derived = _with_signature_seed(opts) do seed
                _nkey_public_from_seed(seed)
            end
            derived == nkey || throw(ArgumentError("nkey does not match nkey seed"))
        end
        return nkey
    end
    _with_signature_seed(opts) do seed
        _nkey_public_from_seed(seed)
    end
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

function _with_connect_nkey_jwt_fields(f, opts::ConnectOptions, info::ServerInfo)
    _connect_option_has_nkey_jwt(opts) || return f((jwt=nothing, nkey=nothing, sig=nothing))
    nonce = _connect_nonce(info)
    if _connect_option_has_jwt(opts)
        return _with_resolved_credentials(opts) do credentials
            if isnothing(credentials)
                return _with_resolved_jwt(opts) do jwt
                    f((jwt=_secret_to_string(jwt),
                       nkey=nothing,
                       sig=_resolve_signature(opts, nonce)))
                end
            end
            f((jwt=_secret_to_string(credentials.jwt),
               nkey=nothing,
               sig=_resolve_signature(opts, nonce; credential_seed=credentials.seed)))
        end
    end
    f((jwt=nothing, nkey=_resolve_nkey(opts), sig=_resolve_signature(opts, nonce)))
end

function _connect_nkey_jwt_fields(opts::ConnectOptions, info::ServerInfo)
    _with_connect_nkey_jwt_fields(identity, opts, info)
end
