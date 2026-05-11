using TestItems

@testmodule TestHelpers begin
    using Natter
    using Random

    const N = Natter

    bytes(s) = Vector{UInt8}(codeunits(s))

    function fake_client(; opts=N.ConnectOptions(), status=N.ConnectionStatus.DISCONNECTED)
        N.Client(
            opts,
            N.Server[],
            nothing,
            nothing,
            status,
            Dict{String,Any}(),
            nothing,
            nothing,
            nothing,
            ReentrantLock(),
            ReentrantLock(),
            0,
            Dict{Int,N.Subscription}(),
            IOBuffer(),
            0,
            Channel{Bool}[],
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
