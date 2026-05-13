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

@testitem "KeyValue status summarizes stream state" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    kv = KeyValue(TestHelpers.fake_client() |> jetstream, "bucket", "KV_bucket", "\$KV.bucket.", true)
    info = StreamInfo(
        "KV_bucket",
        StreamConfig(
            name="KV_bucket",
            max_msgs_per_subject=5,
            max_age=30.0,
            storage=StorageType.MEMORY,
            num_replicas=3,
            allow_direct=true,
        ),
        Dict{String,Any}("messages" => 7, "bytes" => 128),
        Dict{String,Any}(),
    )

    status = N._kv_status(kv, info)
    @test status isa KeyValueStatus
    @test status.bucket == "bucket"
    @test status.stream == "KV_bucket"
    @test status.values == 7
    @test status.history == 5
    @test status.ttl == 30.0
    @test status.bytes == 128
    @test status.storage == StorageType.MEMORY
    @test status.replicas == 3
    @test status.direct == true
    @test status.stream_info === info
end

@testitem "KeyValue errors carry bucket and key context" setup=[TestHelpers] begin
    using Natter
    using Dates

    const N = Natter

    kv = KeyValue(TestHelpers.fake_client() |> jetstream, "bucket", "KV_bucket", "\$KV.bucket.")
    missing = N._kv_not_found_error(kv, "alpha")
    @test missing isa KeyValueError
    @test missing.bucket == "bucket"
    @test missing.key == "alpha"
    @test occursin("key not found", sprint(showerror, missing))

    entry = N._kv_entry_from_stored_msg(
        kv,
        Msg("\$KV.bucket.alpha", nothing, UInt8[]; headers=Headers("KV-Operation" => ["DEL"])),
        3,
        DateTime(2026, 1, 1),
    )
    deleted = N._kv_deleted_error(kv, entry)
    @test deleted isa KeyValueError
    @test deleted.entry === entry
    @test occursin("revision=3", sprint(showerror, deleted))

    cause = JetStreamError(400, 10071, "wrong last sequence")
    wrong = N._kv_wrong_revision_error(kv, "alpha", 2, cause)
    @test wrong isa KeyValueError
    @test wrong.expected_revision == 2
    @test wrong.cause === cause
    @test occursin("wrong revision", sprint(showerror, wrong))

    exists = N._kv_key_exists_error(kv, "alpha", cause)
    @test exists isa KeyValueError
    @test exists.cause === cause
    @test occursin("key exists", sprint(showerror, exists))
end

@testitem "KeyValue keys use headers-only metadata snapshots" begin
    using Natter

    const N = Natter
    prefix = "\$KV.bucket."
    latest = Dict{String,Tuple{Int,Bool}}()
    cfg = N._kv_keys_consumer_config()
    payload = N._js_config_payload(cfg)

    @test payload["deliver_policy"] == "last_per_subject"
    @test payload["headers_only"] == true

    large_value = fill(UInt8('x'), 8192)
    N._kv_record_key!(latest, prefix, Msg("$(prefix)alpha", "\$JS.ACK.KV_bucket.C.1.10.1.0.0", large_value))
    @test latest == Dict("alpha" => (10, true))
    @test N._kv_active_keys(latest) == ["alpha"]

    N._kv_record_key!(latest, prefix, Msg("$(prefix)alpha", "\$JS.ACK.KV_bucket.C.2.9.2.0.0", UInt8[]))
    @test latest["alpha"] == (10, true)

    N._kv_record_key!(latest, prefix, Msg("$(prefix)alpha", "\$JS.ACK.KV_bucket.C.3.11.3.0.0", UInt8[];
                                         headers=Headers("KV-Operation" => ["DEL"])))
    @test latest["alpha"] == (11, false)
    @test isempty(N._kv_active_keys(latest))

    N._kv_record_key!(latest, prefix, Msg("$(prefix)beta", "\$JS.ACK.KV_bucket.C.4.12.4.0.0", UInt8[];
                                         headers=Headers("KV-Operation" => ["PURGE"])))
    @test isempty(N._kv_active_keys(latest))

    N._kv_record_key!(latest, prefix, Msg("$(prefix)beta", "\$JS.ACK.KV_bucket.C.5.13.5.0.0", UInt8[]))
    @test N._kv_active_keys(latest) == ["beta"]
    @test_throws ProtocolError N._kv_record_key!(latest, prefix, Msg("other.beta", "\$JS.ACK.KV_bucket.C.6.14.6.0.0", UInt8[]))
end

@testitem "KeyValue create retry is limited to delete markers" begin
    using Natter

    const N = Natter

    wrong_last = JetStreamError(400, 10071, "wrong last sequence")
    other_error = JetStreamError(400, 10014, "stream not found")
    @test N._kv_wrong_last_sequence(wrong_last)
    @test !N._kv_wrong_last_sequence(other_error)

    deleted = Msg("\$KV.bucket.alpha", nothing, UInt8[]; headers=Headers("KV-Operation" => ["DEL"]))
    purged = Msg("\$KV.bucket.alpha", nothing, UInt8[]; headers=Headers("KV-Operation" => ["PURGE"]))
    live = Msg("\$KV.bucket.alpha", nothing, UInt8[])

    @test N._kv_delete_marker_revision(deleted, 2) == 2
    @test N._kv_delete_marker_revision(purged, 3) == 3
    @test isnothing(N._kv_delete_marker_revision(live, 4))
end

@testitem "KeyValue entries expose typed metadata" setup=[TestHelpers] begin
    using Natter
    using Dates

    const N = Natter

    kv = KeyValue(TestHelpers.fake_client() |> jetstream, "bucket", "KV_bucket", "\$KV.bucket.")
    created = DateTime(2026, 1, 1, 2, 3, 4, 567)
    msg = Msg("\$KV.bucket.path.to.key", nothing, TestHelpers.bytes("value"))
    entry = N._kv_entry_from_stored_msg(kv, msg, 12, created)

    @test entry isa KeyValueEntry
    @test entry.bucket == "bucket"
    @test entry.key == "path.to.key"
    @test entry.value == TestHelpers.bytes("value")
    @test String(entry) == "value"
    @test entry.revision == 12
    @test entry.created == created
    @test entry.delta == 0
    @test entry.operation == KeyValueOperation.PUT
    @test entry.msg === msg

    deleted = Msg("\$KV.bucket.path.to.key", nothing, UInt8[]; headers=Headers("KV-Operation" => ["DEL"]))
    deleted_entry = N._kv_entry_from_stored_msg(kv, deleted, 13, created)
    @test deleted_entry.operation == KeyValueOperation.DELETE
    @test N._kv_is_delete_marker(deleted_entry.operation)

    purged = Msg("\$KV.bucket.path.to.key", nothing, UInt8[]; headers=Headers("KV-Operation" => ["PURGE"]))
    @test N._kv_entry_from_stored_msg(kv, purged, 14, created).operation == KeyValueOperation.PURGE

    unknown = Msg("\$KV.bucket.path.to.key", nothing, UInt8[]; headers=Headers("KV-Operation" => ["UNKNOWN"]))
    @test_throws ProtocolError N._kv_entry_from_stored_msg(kv, unknown, 15, created)
    @test_throws ProtocolError N._kv_entry_from_stored_msg(kv, Msg("other.subject", nothing, UInt8[]), 16, created)

    consumer_msg = Msg("\$KV.bucket.path.to.key", "\$JS.ACK.KV_bucket.C1.1.17.2.123456789.4", TestHelpers.bytes("older"))
    consumer_entry = N._kv_entry_from_consumer_msg(kv, consumer_msg)
    @test consumer_entry.revision == 17
    @test consumer_entry.created == DateTime(1970, 1, 1, 0, 0, 0, 123)
    @test consumer_entry.delta == 4
end
