mutable struct SecretBytes <: AbstractVector{UInt8}
    _ptr::Ptr{UInt8}
    _len::Int
    _wiped::Bool

    function SecretBytes(bytes::AbstractVector{UInt8}; take::Bool=false)
        len = length(bytes)
        ptr = len == 0 ? Ptr{UInt8}(C_NULL) : Ptr{UInt8}(Libc.malloc(len))
        len == 0 || ptr != C_NULL || throw(OutOfMemoryError())
        secret = new(ptr, len, false)
        try
            offset = 1
            @inbounds for byte in bytes
                unsafe_store!(ptr, byte, offset)
                offset += 1
            end
        catch
            _wipe_secret_finalizer!(secret)
            rethrow()
        finally
            take && bytes isa Vector{UInt8} && _wipe_bytes!(bytes)
        end
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
    if !secret._wiped && secret._ptr != C_NULL
        ccall((:sodium_memzero, libsodium_jll.libsodium), Cvoid,
              (Ptr{Cvoid}, Csize_t), secret._ptr, Csize_t(secret._len))
        Libc.free(secret._ptr)
    end
    secret._ptr = C_NULL
    secret._len = 0
    secret._wiped = true
    nothing
end

_secret_bytes(value::Nothing) = nothing
_secret_bytes(value::SecretBytes) = SecretBytes(value)
_secret_bytes(value::AbstractVector{UInt8}) = SecretBytes(value)
_secret_bytes(value::AbstractString) = SecretBytes(value)

function _wipe_secret!(secret::SecretBytes)
    _wipe_secret_finalizer!(secret)
    nothing
end

_wipe_secret!(::Nothing) = nothing

Base.IndexStyle(::Type{SecretBytes}) = IndexLinear()
Base.eltype(::Type{SecretBytes}) = UInt8
Base.size(secret::SecretBytes) = (secret._len,)

function Base.getindex(secret::SecretBytes, i::Int)
    @boundscheck checkbounds(secret, i)
    unsafe_load(secret._ptr, i)
end

Base.copy(secret::SecretBytes) = SecretBytes(secret)

function Base.show(io::IO, secret::SecretBytes)
    print(io, "SecretBytes(<redacted>, ", length(secret), " bytes)")
end

function Base.show(io::IO, ::MIME"text/plain", secret::SecretBytes)
    show(io, secret)
end
