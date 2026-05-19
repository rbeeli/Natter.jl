#!/usr/bin/env julia

using Dates
using JSON3
using Natter
using Printf
using Random
import Sockets
using Sockets: @ip_str
using Statistics
using URIs

const DEFAULT_URL = get(ENV, "NATTER_PERF_URL", "nats://127.0.0.1:4222")
const DEFAULT_MESSAGES = parse(Int, get(ENV, "NATTER_PERF_MESSAGES", "10000"))
const DEFAULT_REQUESTS = parse(Int, get(ENV, "NATTER_PERF_REQUESTS", "1000"))
const DEFAULT_PAYLOAD_BYTES = parse(Int, get(ENV, "NATTER_PERF_PAYLOAD_BYTES", "64"))
const DEFAULT_CONCURRENCY = parse(Int, get(ENV, "NATTER_PERF_CONCURRENCY", string(max(1, 2 * Threads.nthreads()))))
const DEFAULT_OUTPUT = get(ENV, "NATTER_PERF_OUTPUT", "performance-report.md")
const DEFAULT_JSON = get(ENV, "NATTER_PERF_JSON", "performance-report.json")
const DEFAULT_TIMEOUT = parse(Float64, get(ENV, "NATTER_PERF_TIMEOUT", "15.0"))
const DEFAULT_ALLOC_ITERATIONS = parse(Int, get(ENV, "NATTER_PERF_ALLOC_ITERATIONS", "1000"))

function arg_value(args::Vector{String}, name::String, default::String)::String
    index = findfirst(==(name), args)
    isnothing(index) && return default
    index == length(args) && throw(ArgumentError("missing value for $name"))
    args[index + 1]
end

function options(args::Vector{String})
    url = arg_value(args, "--url", DEFAULT_URL)
    messages = parse(Int, arg_value(args, "--messages", string(DEFAULT_MESSAGES)))
    requests = parse(Int, arg_value(args, "--requests", string(DEFAULT_REQUESTS)))
    payload_bytes = parse(Int, arg_value(args, "--payload-bytes", string(DEFAULT_PAYLOAD_BYTES)))
    concurrency = parse(Int, arg_value(args, "--concurrency", string(DEFAULT_CONCURRENCY)))
    output = arg_value(args, "--output", DEFAULT_OUTPUT)
    json = arg_value(args, "--json", DEFAULT_JSON)
    timeout = parse(Float64, arg_value(args, "--timeout", string(DEFAULT_TIMEOUT)))
    alloc_iterations = parse(Int, arg_value(args, "--alloc-iterations", string(DEFAULT_ALLOC_ITERATIONS)))

    messages > 0 || throw(ArgumentError("--messages must be positive"))
    requests > 0 || throw(ArgumentError("--requests must be positive"))
    payload_bytes >= 0 || throw(ArgumentError("--payload-bytes must be non-negative"))
    concurrency > 0 || throw(ArgumentError("--concurrency must be positive"))
    timeout > 0 || throw(ArgumentError("--timeout must be positive"))
    alloc_iterations > 0 || throw(ArgumentError("--alloc-iterations must be positive"))

    (; url, messages, requests, payload_bytes, concurrency, output, json, timeout, alloc_iterations)
end

function connect_perf(url::AbstractString; name::AbstractString="natter-perf", kwargs...)
    connect(url;
        name,
        record_stats=false,
        connect_timeout=2.0,
        reconnect_wait=0.02,
        reconnect_max_wait=0.1,
        reconnect_jitter=0.0,
        write_timeout=10.0,
        error_cb=err -> nothing,
        kwargs...,
    )
end

function batch_buffer_size(subject::String, payload::Vector{UInt8}, messages::Int)::Int
    frame = prepare_publish(subject, payload)
    max(1024 * 1024, 2 * Natter._serialized_size(frame) * max(1, messages + 100))
end

function connect_batch_perf(url::AbstractString, subject::String, payload::Vector{UInt8},
                            messages::Int; name::AbstractString)
    size = batch_buffer_size(subject, payload, messages)
    connect_perf(url;
        name,
        pending_size=max(16 * 1024 * 1024, 2 * size),
        write_buffer_size=size,
        write_buffer_latency=1.0,
    )
end

function allocation_bytes_per_call(f::F, iterations::Int)::Float64 where {F}
    f()
    GC.gc()
    bytes = @allocated begin
        for _ in 1:iterations
            f()
        end
    end
    bytes / iterations
end

function elapsed_seconds(f::F)::Float64 where {F}
    started = time_ns()
    f()
    (time_ns() - started) / 1e9
end

function percentile(values::Vector{Float64}, q::Float64)::Float64
    isempty(values) && return NaN
    quantile(values, q)
end

function fmt(value::Real; digits::Int=2)::String
    @sprintf("%.*f", digits, Float64(value))
end

function benchmark_allocations(client, subject::String, payload::Vector{UInt8}, iterations::Int)
    frame = prepare_publish(subject, payload)
    prepare_bytes = allocation_bytes_per_call(iterations) do
        prepare_publish(subject, payload)
    end
    publish_bytes = allocation_bytes_per_call(iterations) do
        publish(client, frame; direct_write=true)
    end
    flush(client; timeout=5.0)
    Dict(
        "prepare_publish_bytes_per_call" => prepare_bytes,
        "prepared_publish_direct_write_bytes_per_call" => publish_bytes,
        "iterations" => iterations,
    )
end

function benchmark_publish_direct(client, subject::String, payload::Vector{UInt8}, messages::Int)
    frame = prepare_publish(subject, payload)
    for _ in 1:min(messages, 100)
        publish(client, frame; direct_write=true)
    end
    flush(client; timeout=5.0)

    seconds = elapsed_seconds() do
        for _ in 1:messages
            publish(client, frame; direct_write=true)
        end
        flush(client; timeout=10.0)
    end
    Dict(
        "messages" => messages,
        "payload_bytes" => length(payload),
        "seconds" => seconds,
        "messages_per_second" => messages / seconds,
        "payload_mib_per_second" => messages * length(payload) / seconds / 1024^2,
    )
end

function benchmark_publish_buffered_batch(url::String, subject::String, payload::Vector{UInt8},
                                          messages::Int)
    client = connect_batch_perf(url, subject, payload, messages; name="natter-perf-publish-batch")
    frame = prepare_publish(subject, payload)
    try
        for _ in 1:min(messages, 100)
            publish(client, frame)
        end
        flush(client; timeout=5.0)

        seconds = elapsed_seconds() do
            for _ in 1:messages
                publish(client, frame)
            end
            flush(client; timeout=10.0)
        end
        Dict(
            "messages" => messages,
            "payload_bytes" => length(payload),
            "seconds" => seconds,
            "messages_per_second" => messages / seconds,
            "payload_mib_per_second" => messages * length(payload) / seconds / 1024^2,
        )
    finally
        close(client)
    end
end

function benchmark_publish_flush_each(client, subject::String, payload::Vector{UInt8}, messages::Int)
    frame = prepare_publish(subject, payload)
    warmup = min(messages, 20)
    for _ in 1:warmup
        publish(client, frame)
        flush(client; timeout=5.0)
    end

    seconds = elapsed_seconds() do
        for _ in 1:messages
            publish(client, frame)
            flush(client; timeout=10.0)
        end
    end
    Dict(
        "messages" => messages,
        "payload_bytes" => length(payload),
        "seconds" => seconds,
        "messages_per_second" => messages / seconds,
        "payload_mib_per_second" => messages * length(payload) / seconds / 1024^2,
    )
end

function benchmark_callback_dispatch(url::String, subject::String, payload::Vector{UInt8},
                                     messages::Int, timeout::Float64; mode::Symbol=:direct)
    sub_client = connect_perf(url; name="natter-perf-callback-sub")
    pub_client = mode == :buffered_batch ?
                 connect_batch_perf(url, subject, payload, messages; name="natter-perf-callback-pub") :
                 connect_perf(url; name="natter-perf-callback-pub", write_buffer_size=0)
    counter = Threads.Atomic{Int}(0)
    try
        pending_bytes_limit = max(1024 * 1024, messages * max(1, length(payload) + 128))
        sub = subscribe(sub_client, subject;
                        pending_msgs_limit=max(1024, messages),
                        pending_bytes_limit=pending_bytes_limit) do _msg
            Threads.atomic_add!(counter, 1)
            nothing
        end
        flush(sub_client; timeout=5.0)
        frame = prepare_publish(subject, payload)

        warmup = min(messages, 100)
        for _ in 1:warmup
            publish(pub_client, frame; direct_write=(mode == :direct))
        end
        flush(pub_client; timeout=5.0)
        result = timedwait(timeout; pollint=0.001) do
            counter[] >= warmup
        end
        result == :timed_out && error("timed out waiting for callback warmup: received $(counter[]) of $warmup")
        counter[] = 0

        seconds = elapsed_seconds() do
            for _ in 1:messages
                publish(pub_client, frame; direct_write=(mode == :direct))
            end
            flush(pub_client; timeout=10.0)
            result = timedwait(timeout; pollint=0.001) do
                counter[] >= messages
            end
            result == :timed_out && error("timed out waiting for callback dispatch: received $(counter[]) of $messages")
        end

        close(sub)
        Dict(
            "messages" => messages,
            "received" => counter[],
            "seconds" => seconds,
            "messages_per_second" => messages / seconds,
        )
    finally
        close(pub_client)
        close(sub_client)
    end
end

function benchmark_request_latency(url::String, subject::String, payload::Vector{UInt8},
                                   requests::Int)
    service = connect_perf(url; name="natter-perf-request-service")
    client = connect_perf(url; name="natter-perf-request-client")
    latencies = Vector{Float64}(undef, requests)
    try
        sub = subscribe(service, subject) do msg
            isnothing(msg.reply) && return nothing
            respond(service, msg, payload; direct_write=true)
            nothing
        end
        flush(service; timeout=5.0)

        for _ in 1:min(requests, 50)
            request(client, subject, payload; timeout=5.0)
        end

        seconds = elapsed_seconds() do
            for i in 1:requests
                started = time_ns()
                request(client, subject, payload; timeout=5.0)
                latencies[i] = (time_ns() - started) / 1e6
            end
        end

        close(sub)
        Dict(
            "requests" => requests,
            "seconds" => seconds,
            "requests_per_second" => requests / seconds,
            "latency_ms_mean" => mean(latencies),
            "latency_ms_p50" => percentile(latencies, 0.50),
            "latency_ms_p95" => percentile(latencies, 0.95),
            "latency_ms_p99" => percentile(latencies, 0.99),
            "latency_ms_max" => maximum(latencies),
        )
    finally
        close(client)
        close(service)
    end
end

function benchmark_concurrent_publish(url::String, subject::String, payload::Vector{UInt8},
                                      messages::Int, concurrency::Int; mode::Symbol=:direct)
    client = mode == :buffered_batch ?
             connect_batch_perf(url, subject, payload, messages; name="natter-perf-concurrent-publish") :
             connect_perf(url; name="natter-perf-concurrent-publish", write_buffer_size=0)
    frame = prepare_publish(subject, payload)
    per_task = cld(messages, concurrency)
    total = per_task * concurrency
    try
        publish(client, frame; direct_write=(mode == :direct))
        flush(client; timeout=5.0)
        @sync begin
            for _ in 1:concurrency
                Threads.@spawn publish(client, frame; direct_write=(mode == :direct))
            end
        end
        flush(client; timeout=5.0)

        seconds = elapsed_seconds() do
            @sync begin
                for _ in 1:concurrency
                    Threads.@spawn begin
                        for _ in 1:per_task
                            publish(client, frame; direct_write=(mode == :direct))
                        end
                    end
                end
            end
            flush(client; timeout=10.0)
        end
        Dict(
            "messages" => total,
            "concurrency" => concurrency,
            "julia_threads" => Threads.nthreads(),
            "seconds" => seconds,
            "messages_per_second" => total / seconds,
        )
    finally
        close(client)
    end
end

mutable struct TcpProxy
    server::Sockets.TCPServer
    resources::Vector{Any}
    lock::ReentrantLock
    accept_task::Union{Task,Nothing}
    url::String
end

function remember!(proxy::TcpProxy, resource)
    lock(proxy.lock)
    try
        push!(proxy.resources, resource)
    finally
        unlock(proxy.lock)
    end
    resource
end

function close_quietly(resource)
    try
        close(resource)
    catch err
        @debug "Natter performance proxy cleanup failed" exception=(err, catch_backtrace())
    end
    nothing
end

function proxy_resources(proxy::TcpProxy)
    lock(proxy.lock)
    try
        copy(proxy.resources)
    finally
        unlock(proxy.lock)
    end
end

function proxy_pump(from, to)
    try
        while true
            data = readavailable(from)
            isempty(data) && break
            write(to, data)
            flush(to)
        end
    catch err
        @debug "Natter performance proxy pump stopped" exception=(err, catch_backtrace())
    finally
        close_quietly(from)
        close_quietly(to)
    end
    nothing
end

function url_host_port(url::String)
    parsed = URI(url)
    scheme = String(parsed.scheme)
    scheme in ("nats", "") || throw(ArgumentError("performance reconnect benchmark requires a nats:// URL, got $url"))
    host = isempty(String(parsed.host)) ? "127.0.0.1" : String(parsed.host)
    port = isnothing(parsed.port) ? 4222 : parse(Int, String(parsed.port))
    host, port
end

function start_proxy(url::String)
    host, port = url_host_port(url)
    server = Sockets.listen(ip"127.0.0.1", 0)
    _, proxy_port = Sockets.getsockname(server)
    proxy = TcpProxy(server, Any[server], ReentrantLock(), nothing, "nats://127.0.0.1:$(Int(proxy_port))")
    proxy.accept_task = Threads.@spawn begin
        while true
            client_sock = try
                Sockets.accept(server)
            catch err
                @debug "Natter performance proxy accept stopped" exception=(err, catch_backtrace())
                break
            end
            remember!(proxy, client_sock)
            server_sock = try
                Sockets.connect(host, port)
            catch err
                @debug "Natter performance proxy target connect failed" exception=(err, catch_backtrace())
                close_quietly(client_sock)
                continue
            end
            remember!(proxy, server_sock)
            Threads.@spawn proxy_pump(client_sock, server_sock)
            Threads.@spawn proxy_pump(server_sock, client_sock)
        end
    end
    proxy
end

function drop_proxy_connections!(proxy::TcpProxy)
    for resource in reverse(proxy_resources(proxy))
        resource === proxy.server && continue
        close_quietly(resource)
    end
    nothing
end

function stop_proxy!(proxy::TcpProxy)
    for resource in reverse(proxy_resources(proxy))
        close_quietly(resource)
    end
    if !isnothing(proxy.accept_task)
        timedwait(1.0; pollint=0.01) do
            istaskdone(proxy.accept_task)
        end
    end
    nothing
end

function benchmark_reconnect(url::String, subject::String, payload::Vector{UInt8}, timeout::Float64)
    proxy = start_proxy(url)
    events = String[]
    client = connect_perf(proxy.url;
        name="natter-perf-reconnect",
        record_stats=true,
        event_cb=event -> push!(events, string(event.kind)),
    )
    try
        publish(client, subject, payload; direct_write=true)
        flush(client; timeout=5.0)
        drop_proxy_connections!(proxy)

        attempts = 0
        recovered = false
        started = time_ns()
        deadline = time() + timeout
        while time() < deadline
            attempts += 1
            try
                publish(client, subject, payload; direct_write=true)
                flush(client; timeout=0.25)
                recovered = true
                break
            catch err
                @debug "Natter performance reconnect probe not recovered yet" exception=(err, catch_backtrace())
                sleep(0.01)
            end
        end
        seconds = (time_ns() - started) / 1e9
        recovered || error("timed out waiting for reconnect recovery")
        st = stats(client)
        Dict(
            "seconds" => seconds,
            "recovery_ms" => seconds * 1000,
            "attempts" => attempts,
            "reconnects_recorded" => st.reconnects,
            "events" => events,
        )
    finally
        close(client)
        stop_proxy!(proxy)
    end
end

function write_json(path::String, report)
    mkpath(dirname(abspath(path)))
    open(path, "w") do io
        JSON3.pretty(io, report)
        println(io)
    end
    nothing
end

function write_markdown(path::String, report)
    mkpath(dirname(abspath(path)))
    open(path, "w") do io
        println(io, "# Natter Performance Report")
        println(io)
        println(io, "Generated: `$(report["generated_at"])`")
        println(io)
        println(io, "This is a non-gating throughput and latency snapshot. Each benchmark runs its warmup before the timed region, so Julia startup, package loading, JIT compilation, and benchmark warmup are not included in the reported timings. Compare results only across matching publish semantics and similar runners, Julia versions, thread counts, NATS versions, and payload sizes.")
        println(io)
        println(io, "## Environment")
        println(io)
        println(io, "| Field | Value |")
        println(io, "| :--- | :--- |")
        for (name, value) in report["environment"]
            println(io, "| $name | `$value` |")
        end
        println(io)
        println(io, "## Results")
        println(io)
        println(io, "| Area | Metric | Value |")
        println(io, "| :--- | :--- | ---: |")

        allocations = report["benchmarks"]["allocations"]
        println(io, "| Allocations | `prepare_publish` bytes/call | $(fmt(allocations["prepare_publish_bytes_per_call"])) |")
        println(io, "| Allocations | prepared `publish(...; direct_write=true)` bytes/call | $(fmt(allocations["prepared_publish_direct_write_bytes_per_call"])) |")

        publish_direct = report["benchmarks"]["publish_direct"]
        println(io, "| Publish direct write | messages/s | $(fmt(publish_direct["messages_per_second"])) |")
        println(io, "| Publish direct write | payload MiB/s | $(fmt(publish_direct["payload_mib_per_second"])) |")

        publish_batch = report["benchmarks"]["publish_buffered_batch"]
        println(io, "| Publish buffered batch | messages/s | $(fmt(publish_batch["messages_per_second"])) |")
        println(io, "| Publish buffered batch | payload MiB/s | $(fmt(publish_batch["payload_mib_per_second"])) |")

        publish_flush_each = report["benchmarks"]["publish_flush_each"]
        println(io, "| Publish + flush each | messages/s | $(fmt(publish_flush_each["messages_per_second"])) |")
        println(io, "| Publish + flush each | payload MiB/s | $(fmt(publish_flush_each["payload_mib_per_second"])) |")

        callback_direct = report["benchmarks"]["callback_dispatch_direct"]
        println(io, "| Callback dispatch direct write | messages/s | $(fmt(callback_direct["messages_per_second"])) |")

        callback_batch = report["benchmarks"]["callback_dispatch_buffered_batch"]
        println(io, "| Callback dispatch buffered batch | messages/s | $(fmt(callback_batch["messages_per_second"])) |")

        request = report["benchmarks"]["request_reply"]
        println(io, "| Request/reply | requests/s | $(fmt(request["requests_per_second"])) |")
        println(io, "| Request/reply | p50 latency ms | $(fmt(request["latency_ms_p50"], digits=3)) |")
        println(io, "| Request/reply | p95 latency ms | $(fmt(request["latency_ms_p95"], digits=3)) |")
        println(io, "| Request/reply | p99 latency ms | $(fmt(request["latency_ms_p99"], digits=3)) |")
        println(io, "| Request/reply | max latency ms | $(fmt(request["latency_ms_max"], digits=3)) |")

        concurrent_direct = report["benchmarks"]["concurrent_publish_direct"]
        println(io, "| Concurrent publish direct write | concurrency | $(concurrent_direct["concurrency"]) |")
        println(io, "| Concurrent publish direct write | messages/s | $(fmt(concurrent_direct["messages_per_second"])) |")

        concurrent_batch = report["benchmarks"]["concurrent_publish_buffered_batch"]
        println(io, "| Concurrent publish buffered batch | concurrency | $(concurrent_batch["concurrency"]) |")
        println(io, "| Concurrent publish buffered batch | messages/s | $(fmt(concurrent_batch["messages_per_second"])) |")

        reconnect = report["benchmarks"]["reconnect"]
        println(io, "| Reconnect | recovery ms | $(fmt(reconnect["recovery_ms"], digits=3)) |")
        println(io, "| Reconnect | retry loop attempts | $(reconnect["attempts"]) |")
        println(io)
    end
    nothing
end

function main(args::Vector{String}=ARGS)
    opts = options(args)
    Random.seed!(0x6e6174746572)

    payload = fill(UInt8('x'), opts.payload_bytes)
    prefix = "natter.perf.$(Dates.format(now(UTC), "yyyymmddHHMMSS")).$(getpid())"

    client = connect_perf(opts.url; name="natter-perf-main", write_buffer_size=0)
    benchmarks = Dict{String,Any}()
    try
        benchmarks["allocations"] = benchmark_allocations(client, "$prefix.alloc", payload, opts.alloc_iterations)
        benchmarks["publish_direct"] = benchmark_publish_direct(client, "$prefix.publish.direct", payload, opts.messages)
        benchmarks["publish_flush_each"] = benchmark_publish_flush_each(client, "$prefix.publish.flush", payload, opts.messages)
    finally
        close(client)
    end

    benchmarks["publish_buffered_batch"] = benchmark_publish_buffered_batch(opts.url, "$prefix.publish.batch", payload, opts.messages)
    benchmarks["callback_dispatch_direct"] = benchmark_callback_dispatch(opts.url, "$prefix.callback.direct", payload, opts.messages, opts.timeout; mode=:direct)
    benchmarks["callback_dispatch_buffered_batch"] = benchmark_callback_dispatch(opts.url, "$prefix.callback.batch", payload, opts.messages, opts.timeout; mode=:buffered_batch)
    benchmarks["request_reply"] = benchmark_request_latency(opts.url, "$prefix.request", payload, opts.requests)
    benchmarks["concurrent_publish_direct"] = benchmark_concurrent_publish(opts.url, "$prefix.concurrent.direct", payload, opts.messages, opts.concurrency; mode=:direct)
    benchmarks["concurrent_publish_buffered_batch"] = benchmark_concurrent_publish(opts.url, "$prefix.concurrent.batch", payload, opts.messages, opts.concurrency; mode=:buffered_batch)
    benchmarks["reconnect"] = benchmark_reconnect(opts.url, "$prefix.reconnect", payload, opts.timeout)

    environment = Dict(
        "julia_version" => string(VERSION),
        "julia_threads" => string(Threads.nthreads()),
        "julia_maxthreadid" => string(Threads.maxthreadid()),
        "natter_version" => string(pkgversion(Natter)),
        "url" => opts.url,
        "messages" => string(opts.messages),
        "requests" => string(opts.requests),
        "payload_bytes" => string(opts.payload_bytes),
        "concurrency" => string(opts.concurrency),
    )
    report = Dict(
        "generated_at" => string(now(UTC)),
        "environment" => environment,
        "benchmarks" => benchmarks,
    )

    write_json(opts.json, report)
    write_markdown(opts.output, report)
    println(read(opts.output, String))
    nothing
end

main()
