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

@testitem "KeyValue bucket config maps to stream config" begin
    using Natter

    const N = Natter

    cfg = N._kv_stream_config(
        "bucket";
        history=64,
        ttl=30.0,
        max_bytes=4096,
        max_value_size=512,
        storage=StorageType.MEMORY,
        replicas=3,
        direct=true,
        compression=true,
        metadata=Dict(:owner => "kv"),
        limit_marker_ttl=60.0,
    )
    payload = N._js_config_payload(cfg)

    @test payload["name"] == "KV_bucket"
    @test payload["subjects"] == ["\$KV.bucket.>"]
    @test payload["max_msgs_per_subject"] == 64
    @test payload["max_msgs"] == -1
    @test payload["max_consumers"] == -1
    @test payload["max_bytes"] == 4096
    @test payload["max_msg_size"] == 512
    @test payload["max_age"] == 30_000_000_000
    @test payload["storage"] == "memory"
    @test payload["num_replicas"] == 3
    @test payload["allow_direct"] == true
    @test payload["allow_rollup_hdrs"] == true
    @test payload["deny_delete"] == true
    @test payload["discard"] == "new"
    @test payload["compression"] == "s2"
    @test payload["allow_msg_ttl"] == true
    @test payload["subject_delete_marker_ttl"] == 60_000_000_000
    @test payload["metadata"] == Dict("owner" => "kv")

    default_payload = N._js_config_payload(N._kv_stream_config("bucket"))
    @test default_payload["max_msgs_per_subject"] == 1
    @test default_payload["max_bytes"] == -1
    @test default_payload["max_msg_size"] == -1
    @test !haskey(default_payload, "max_age")
    @test !haskey(default_payload, "compression")
    @test !haskey(default_payload, "allow_msg_ttl")
    @test !haskey(default_payload, "subject_delete_marker_ttl")
    @test !haskey(default_payload, "metadata")
end

@testitem "KeyValue bucket config validates local limits" begin
    using Natter

    const N = Natter

    @test N._validate_kv_history(1) == 1
    @test N._validate_kv_history(64) == 64
    @test_throws ArgumentError N._kv_stream_config("bucket"; history=0)
    @test_throws ArgumentError N._kv_stream_config("bucket"; history=65)
    @test_throws ArgumentError N._kv_stream_config("bucket"; ttl=-0.1)
    @test_throws ArgumentError N._kv_stream_config("bucket"; ttl=Inf)
    @test_throws ArgumentError N._kv_stream_config("bucket"; max_bytes=-2)
    @test_throws ArgumentError N._kv_stream_config("bucket"; max_value_size=-2)
    @test_throws ArgumentError N._kv_stream_config("bucket"; limit_marker_ttl=0)
    @test_throws ArgumentError N._kv_stream_config("bucket"; metadata=Dict("owner" => 1))
end

@testitem "KeyValue numeric validators reject Bool" begin
    using Natter

    const N = Natter

    @test_throws ArgumentError N._validate_kv_history(true)
    @test_throws ArgumentError N._validate_kv_limit("max_bytes", true)
    @test_throws ArgumentError N._kv_optional_seconds("ttl", true)
    @test_throws ArgumentError N._kv_expected_revision(true)
    @test_throws ArgumentError N._kv_add_expected_revision!(Headers(), true)
    @test N._kv_expected_revision(big(1)) == 1
    @test_throws ArgumentError N._kv_expected_revision(big(typemax(Int)) + 1)

    @test_throws ArgumentError N._kv_stream_config("bucket"; history=true)
    @test_throws ArgumentError N._kv_stream_config("bucket"; ttl=true)
    @test_throws ArgumentError N._kv_stream_config("bucket"; max_bytes=true)
    @test_throws ArgumentError N._kv_stream_config("bucket"; max_value_size=true)
    @test_throws ArgumentError N._kv_stream_config("bucket"; replicas=true)
    @test_throws ArgumentError N._kv_stream_config("bucket"; limit_marker_ttl=true)
end

@testitem "KeyValue timeout arguments are positive finite before protocol writes" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    for invalid_timeout in (-1.0, 0.0, Inf, NaN, true)
        capture = TestHelpers.WriteCapture()
        client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
        js = jetstream(client)
        kv = KeyValue(js, "bucket", "KV_bucket", "\$KV.bucket.")

        @test_throws ArgumentError kv_create(js, "newbucket"; timeout=invalid_timeout)
        @test_throws ArgumentError kv_open(js, "bucket"; timeout=invalid_timeout)
        @test_throws ArgumentError kv_delete_bucket(kv; timeout=invalid_timeout)
        @test_throws ArgumentError kv_status(kv; timeout=invalid_timeout)
        @test_throws ArgumentError kv_get(kv, "alpha"; timeout=invalid_timeout)
        @test_throws ArgumentError kv_put(kv, "alpha", "value"; timeout=invalid_timeout)
        @test_throws ArgumentError kv_create_key(kv, "alpha", "value"; timeout=invalid_timeout)
        @test_throws ArgumentError kv_update(kv, "alpha", "value", 1; timeout=invalid_timeout)
        @test_throws ArgumentError kv_delete(kv, "alpha"; timeout=invalid_timeout)
        @test_throws ArgumentError kv_purge(kv, "alpha"; timeout=invalid_timeout)
        @test_throws ArgumentError kv_history(kv, "alpha"; timeout=invalid_timeout)
        @test_throws ArgumentError kv_keys(kv; timeout=invalid_timeout)
        @test_throws ArgumentError kv_watch(kv; timeout=invalid_timeout)
        @test_throws ArgumentError kv_watch(_ -> nothing, kv; timeout=invalid_timeout)
        @test_throws ArgumentError kv_purge_deletes(kv; timeout=invalid_timeout)

        @test TestHelpers.capture_text(capture) == ""
        @test isempty(client.subscriptions)
    end

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    kv = KeyValue(jetstream(client), "bucket", "KV_bucket", "\$KV.bucket.")
    @test_throws ArgumentError kv_history(kv, "alpha"; batch=true)
    @test_throws ArgumentError kv_history(kv, "alpha"; batch=big(typemax(Int)) + 1)
    @test TestHelpers.capture_text(capture) == ""
    @test isempty(client.subscriptions)
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
        StreamState(messages=7, bytes=128),
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

@testitem "KeyValue write APIs return revisions without exposing PubAck" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    function reply_next_puback!(client, stream::String, seq::Int)
        deadline = time() + 1.0
        while time() < deadline
            mux = @atomic client.request_mux
            if !isnothing(mux)
                token = begin
                    lock(mux.condition)
                    try
                        isempty(mux.waiters) ? nothing : first(keys(mux.waiters))
                    finally
                        unlock(mux.condition)
                    end
                end
                if !isnothing(token)
                    payload = "{\"stream\":\"$stream\",\"seq\":$seq}"
                    N._dispatch_msg(
                        client,
                        Msg("$(mux.prefix).$token", nothing, TestHelpers.bytes(payload); sid=mux.sub.sid),
                    )
                    return nothing
                end
            end
            sleep(0.001)
        end
        error("request waiter not registered")
    end

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    kv = KeyValue(jetstream(client), "bucket", "KV_bucket", "\$KV.bucket.")

    put_task = @async kv_put(kv, "alpha", "one"; timeout=1.0)
    reply_next_puback!(client, "KV_bucket", 10)
    put_revision = fetch(put_task)
    @test put_revision == 10
    @test put_revision isa Int

    create_task = @async kv_create_key(kv, "beta", "one"; timeout=1.0)
    reply_next_puback!(client, "KV_bucket", 11)
    @test fetch(create_task) == 11

    update_task = @async kv_update(kv, "beta", "two", 11; timeout=1.0)
    reply_next_puback!(client, "KV_bucket", 12)
    @test fetch(update_task) == 12

    delete_task = @async kv_delete(kv, "beta"; revision=12, timeout=1.0)
    reply_next_puback!(client, "KV_bucket", 13)
    @test isnothing(fetch(delete_task))

    purge_task = @async kv_purge(kv, "alpha"; timeout=1.0)
    reply_next_puback!(client, "KV_bucket", 14)
    @test isnothing(fetch(purge_task))
end

@testitem "KeyValue delete and purge include expected revision headers" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    kv = KeyValue(jetstream(client; timeout=0.001), "bucket", "KV_bucket", "\$KV.bucket.")

    @test_throws TimeoutError kv_delete(kv, "alpha"; revision=7)
    written = TestHelpers.capture_text(capture)
    @test occursin("HPUB \$KV.bucket.alpha ", written)
    @test occursin("KV-Operation: DEL\r\n", written)
    @test occursin("Nats-Expected-Last-Subject-Sequence: 7\r\n", written)

    TestHelpers.clear_capture!(capture)
    @test_throws TimeoutError kv_purge(kv, "alpha"; revision=8)
    written = TestHelpers.capture_text(capture)
    @test occursin("HPUB \$KV.bucket.alpha ", written)
    @test occursin("KV-Operation: PURGE\r\n", written)
    @test occursin("Nats-Rollup: sub\r\n", written)
    @test occursin("Nats-Expected-Last-Subject-Sequence: 8\r\n", written)

    @test_throws ArgumentError kv_delete(kv, "alpha"; revision=-1)
    @test_throws ArgumentError kv_purge(kv, "alpha"; revision=-1)
end

@testitem "KeyValue writes expose per-key TTL headers" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    capture = TestHelpers.WriteCapture()
    client = TestHelpers.fake_client(; status=N.ConnectionStatus.CONNECTED, write_io=capture)
    kv = KeyValue(jetstream(client; timeout=0.001), "bucket", "KV_bucket", "\$KV.bucket.")

    @test_throws TimeoutError kv_put(kv, "alpha", "value"; ttl=2)
    written = TestHelpers.capture_text(capture)
    @test occursin("HPUB \$KV.bucket.alpha ", written)
    @test occursin("Nats-TTL: 2s\r\n", written)

    TestHelpers.clear_capture!(capture)
    @test_throws TimeoutError kv_create_key(kv, "beta", "value"; ttl=1.5)
    written = TestHelpers.capture_text(capture)
    @test occursin("HPUB \$KV.bucket.beta ", written)
    @test occursin("Nats-Expected-Last-Subject-Sequence: 0\r\n", written)
    @test occursin("Nats-TTL: 1.5s\r\n", written)

    TestHelpers.clear_capture!(capture)
    @test_throws TimeoutError kv_purge(kv, "gamma"; revision=9, ttl=3)
    written = TestHelpers.capture_text(capture)
    @test occursin("KV-Operation: PURGE\r\n", written)
    @test occursin("Nats-Rollup: sub\r\n", written)
    @test occursin("Nats-Expected-Last-Subject-Sequence: 9\r\n", written)
    @test occursin("Nats-TTL: 3s\r\n", written)

    TestHelpers.clear_capture!(capture)
    @test_throws ArgumentError kv_put(kv, "alpha", "value"; ttl=0)
    @test_throws ArgumentError kv_put(kv, "alpha", "value"; ttl=0.5)
    @test TestHelpers.capture_text(capture) == ""
end

@testitem "KeyValue watcher options map to consumer config" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    kv = KeyValue(TestHelpers.fake_client() |> jetstream, "bucket", "KV_bucket", "\$KV.bucket.")
    filters = N._kv_watch_filters(">", ["alpha", "beta.*"])
    cfg = N._kv_watch_consumer_config(kv, filters; meta_only=true)
    @test cfg["deliver_policy"] == "last_per_subject"
    @test cfg["ack_policy"] == "none"
    @test cfg["headers_only"] == true
    @test cfg["filter_subjects"] == ["\$KV.bucket.alpha", "\$KV.bucket.beta.*"]

    history_cfg = N._kv_watch_consumer_config(kv, [">"]; history=true)
    @test history_cfg["deliver_policy"] == "all"
    @test history_cfg["filter_subject"] == "\$KV.bucket.>"

    updates_cfg = N._kv_watch_consumer_config(kv, [">"]; updates_only=true)
    @test updates_cfg["deliver_policy"] == "new"

    resume_cfg = N._kv_watch_consumer_config(kv, [">"]; resume_revision=7)
    @test resume_cfg["deliver_policy"] == "by_start_sequence"
    @test resume_cfg["opt_start_seq"] == 7

    @test N._kv_watch_filters(">", String[]) == [">"]
    @test_throws ArgumentError N._kv_watch_filters("alpha", ["beta"])
    @test_throws ArgumentError N._kv_watch_consumer_config(kv, [">"]; history=true, updates_only=true)
    @test_throws ArgumentError N._kv_watch_consumer_config(kv, [">"]; updates_only=true, resume_revision=1)
    @test_throws ArgumentError N._kv_watch_consumer_config(kv, [">"]; resume_revision=0)
    @test_throws ArgumentError N._kv_watch_channel_size(0)
end

@testitem "KeyValue watchers accept callable objects" setup=[TestHelpers] begin
    using Dates
    using Natter

    const N = Natter

    mutable struct KeyValueCallable
        updates::Vector{Any}
    end
    function (cb::KeyValueCallable)(update)
        push!(cb.updates, update)
        nothing
    end

    callback = KeyValueCallable(Any[])
    state = N._kv_watcher_state(callback, 1, true)
    entry = KeyValueEntry("BUCKET", "key", TestHelpers.bytes("value"), 1, DateTime(2026), 0, KeyValueOperation.PUT)

    N._kv_watcher_emit!(state, entry)
    N._kv_watcher_emit!(state, KV_WATCH_INITIAL_DONE)

    @test callback.updates == Any[entry, KV_WATCH_INITIAL_DONE]
end

@testitem "KeyValue watcher timed take respects timeout" setup=[TestHelpers] begin
    using Natter

    const N = Natter

    client = TestHelpers.fake_client(; status=N.ConnectionStatus.RECONNECTING)
    js = jetstream(client)
    sub = subscribe(client, "_INBOX.kvwatch")
    psub = N.PushSubscription(js, sub, "KV_bucket", "watcher", ReentrantLock(), false, false)
    state = N._kv_watcher_state(nothing, 1, false)
    watcher = KeyValueWatcher(psub, state.updates, state)

    try
        @test_throws TimeoutError N._kv_take!(watcher, 0.01)
        put!(state.updates, KV_WATCH_INITIAL_DONE)
        @test N._kv_take!(watcher, 0.1) === KV_WATCH_INITIAL_DONE
    finally
        close(watcher)
    end
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
    @test KeyValueEntry isa DataType
    @test !hasfield(KeyValueEntry, :msg)
    @test typeof(KeyValueEntry[]) == Vector{KeyValueEntry}
    @test entry.bucket == "bucket"
    @test entry.key == "path.to.key"
    @test entry.value == TestHelpers.bytes("value")
    @test String(entry) == "value"
    @test entry.revision == 12
    @test entry.created == created
    @test entry.delta == 0
    @test entry.operation == KeyValueOperation.PUT

    deleted = Msg("\$KV.bucket.path.to.key", nothing, UInt8[]; headers=Headers("KV-Operation" => ["DEL"]))
    deleted_entry = N._kv_entry_from_stored_msg(kv, deleted, 13, created)
    @test deleted_entry.operation == KeyValueOperation.DELETE
    @test N._kv_is_delete_marker(deleted_entry.operation)

    purged = Msg("\$KV.bucket.path.to.key", nothing, UInt8[]; headers=Headers("KV-Operation" => ["PURGE"]))
    @test N._kv_entry_from_stored_msg(kv, purged, 14, created).operation == KeyValueOperation.PURGE

    unknown = Msg("\$KV.bucket.path.to.key", nothing, UInt8[]; headers=Headers("KV-Operation" => ["UNKNOWN"]))
    @test_throws ProtocolError N._kv_entry_from_stored_msg(kv, unknown, 15, created)
    @test_throws ProtocolError N._kv_entry_from_stored_msg(kv, Msg("other.subject", nothing, UInt8[]), 16, created)

    max_age = Msg("\$KV.bucket.path.to.key", nothing, UInt8[]; headers=Headers("Nats-Marker-Reason" => ["MaxAge"]))
    max_age_entry = N._kv_entry_from_stored_msg(kv, max_age, 17, created)
    @test max_age_entry.operation == KeyValueOperation.PURGE
    @test !N._kv_key_active(max_age)

    removed = Msg("\$KV.bucket.path.to.key", nothing, UInt8[]; headers=Headers("Nats-Marker-Reason" => ["Remove"]))
    @test N._kv_entry_from_stored_msg(kv, removed, 18, created).operation == KeyValueOperation.DELETE

    consumer_msg = Msg("\$KV.bucket.path.to.key", "\$JS.ACK.KV_bucket.C1.1.17.2.123456789.4", TestHelpers.bytes("older"))
    consumer_entry = N._kv_entry_from_consumer_msg(kv, consumer_msg)
    @test consumer_entry.revision == 17
    @test consumer_entry.created == DateTime(1970, 1, 1, 0, 0, 0, 123)
    @test consumer_entry.delta == 4
end
