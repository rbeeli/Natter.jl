using TestItems

@testitem "KeyValue validates buckets and keys" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client()
    js = jetstream(client)
    kv = KeyValue(js, "bucket", "KV_bucket", "\$KV.bucket.")

    @test kv.direct == false
    @test N._validate_kv_bucket("bucket-1") == "bucket-1"
    @test N._validate_kv_key("path/to.key") == "path/to.key"
    @test N._validate_kv_watch_key("path.>") == "path.>"

    @test_throws ArgumentError N._validate_kv_bucket("bad.bucket")
    @test_throws ArgumentError kv_create(js, "bad.bucket")
    @test_throws ArgumentError kv_get(kv, "bad key")
    @test_throws ArgumentError kv_put(kv, "bad.*", "value")
    @test_throws ArgumentError kv_delete(kv, "bad.>")
    @test_throws ArgumentError kv_watch(_ -> nothing, kv; key="bad.>.tail")
end

@testitem "KeyValue tracks direct get capability" setup=[TestHelpers] begin
    using Natter

    kv = KeyValue(TestHelpers.fake_client() |> jetstream, "bucket", "KV_bucket", "\$KV.bucket.", true)
    @test kv.direct == true
end
