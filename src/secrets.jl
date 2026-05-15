mutable struct SecretBytes <: AbstractVector{UInt8}
    bytes::Vector{UInt8}

    function SecretBytes(bytes::AbstractVector{UInt8}; take::Bool=false)
        storage = take && bytes isa Vector{UInt8} ? bytes : Vector{UInt8}(bytes)
        secret = new(storage)
        finalizer(_wipe_secret_finalizer!, secret)
        secret
    end
end

SecretBytes(value::AbstractString) = SecretBytes(codeunits(value))

function _wipe_bytes!(bytes::Vector{UInt8})
    if !isempty(bytes)
        ccall((:sodium_memzero, libsodium_jll.libsodium), Cvoid,
              (Ptr{Cvoid}, Csize_t), bytes, Csize_t(length(bytes)))
    end
    nothing
end

function _wipe_secret_finalizer!(secret::SecretBytes)
    _wipe_bytes!(secret.bytes)
    nothing
end

_secret_bytes(value::Nothing) = nothing
_secret_bytes(value::SecretBytes) = SecretBytes(value.bytes)
_secret_bytes(value::AbstractVector{UInt8}) = SecretBytes(value)
_secret_bytes(value::AbstractString) = SecretBytes(value)

function _wipe_secret!(secret::SecretBytes)
    _wipe_bytes!(secret.bytes)
    nothing
end

_wipe_secret!(::Nothing) = nothing

Base.IndexStyle(::Type{SecretBytes}) = IndexLinear()
Base.eltype(::Type{SecretBytes}) = UInt8
Base.size(secret::SecretBytes) = size(secret.bytes)
Base.getindex(secret::SecretBytes, i::Int) = secret.bytes[i]
Base.setindex!(secret::SecretBytes, value, i::Int) = setindex!(secret.bytes, value, i)
Base.copy(secret::SecretBytes) = SecretBytes(secret.bytes)

function Base.show(io::IO, secret::SecretBytes)
    print(io, "SecretBytes(<redacted>, ", length(secret), " bytes)")
end

function Base.show(io::IO, ::MIME"text/plain", secret::SecretBytes)
    show(io, secret)
end
