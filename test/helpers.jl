using TestItems

@testmodule TestHelpers begin
    using Natter
    using Random

    const N = Natter

    bytes(s) = Vector{UInt8}(codeunits(s))

    mutable struct WriteCapture <: IO
        bytes::Vector{UInt8}
        closed::Bool
    end
    WriteCapture() = WriteCapture(UInt8[], false)

    Base.write(t::WriteCapture, byte::UInt8) = (push!(t.bytes, byte); 1)
    Base.write(t::WriteCapture, data::Vector{UInt8}) = (append!(t.bytes, data); length(data))
    Base.write(t::WriteCapture, data::Base.CodeUnits{UInt8}) = (append!(t.bytes, data); length(data))
    Base.write(t::WriteCapture, data::Union{String,SubString{String}}) = (append!(t.bytes, codeunits(data)); ncodeunits(data))
    Base.write(t::WriteCapture, data::AbstractString) = (append!(t.bytes, codeunits(data)); ncodeunits(data))
    Base.write(t::WriteCapture, ch::Char) = write(t, string(ch))
    Base.flush(::WriteCapture) = nothing
    Base.close(t::WriteCapture) = (t.closed = true; nothing)

    capture_text(t::WriteCapture) = String(copy(t.bytes))
    clear_capture!(t::WriteCapture) = (empty!(t.bytes); nothing)

    function fake_client(; opts=N.ConnectOptions(), status=N.ConnectionStatus.DISCONNECTED,
                         read_io=nothing, write_io=nothing)
        N.Client(
            opts,
            N.Server[],
            nothing,
            nothing,
            status,
            N.ServerInfo(),
            nothing,
            read_io,
            write_io,
            ReentrantLock(),
            ReentrantLock(),
            0,
            Dict{Int,N.Subscription}(),
            nothing,
            ReentrantLock(),
            IOBuffer(),
            0,
            N.PongWaiter[],
            nothing,
            nothing,
            nothing,
            0,
            N.Stats(),
            MersenneTwister(1),
            1,
        )
    end
end
