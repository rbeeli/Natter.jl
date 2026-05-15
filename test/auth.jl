using TestItems

@testitem "nkey decode failures clear partial buffers" begin
    using Natter

    const N = Natter

    seed = "SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY"
    out = UInt8[]
    decode_err = try
        N._nkey_base32_decode!(out, seed * "!")
        nothing
    catch caught
        caught
    end

    @test decode_err isa ArgumentError
    @test !isempty(out)
    @test all(==(UInt8(0)), out)
end

@testitem "nkey checked decode rejects bad checksums" begin
    using Natter

    const N = Natter

    seed = "SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY"
    bad_seed = seed[begin:(end - 1)] * (seed[end] == 'A' ? "B" : "A")
    @test_throws ArgumentError N._nkey_decode_seed(bad_seed)
end
