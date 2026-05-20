#!/usr/bin/env julia

using Dates
using JSON3
using Printf

const START_MARKER = "<!-- NATTER_BENCHMARK_TABLE_START -->"
const END_MARKER = "<!-- NATTER_BENCHMARK_TABLE_END -->"

function arg_value(args::Vector{String}, name::String, default::Union{String,Nothing}=nothing)
    index = findfirst(==(name), args)
    if isnothing(index)
        isnothing(default) && throw(ArgumentError("missing required argument $name"))
        return default
    end
    index == length(args) && throw(ArgumentError("missing value for $name"))
    args[index + 1]
end

function read_report(path::AbstractString)
    isfile(path) || throw(ArgumentError("benchmark report does not exist: $path"))
    JSON3.read(read(path, String))
end

function nested(report, path::AbstractString)
    value = report
    for part in split(path, '.')
        value = value[Symbol(part)]
    end
    Float64(value)
end

function fmt_int(value::Real)
    text = string(round(Int, value))
    first_digit = firstindex(text)
    sign = ""
    if startswith(text, "-")
        sign = "-"
        first_digit = nextind(text, first_digit)
    end
    digits = text[first_digit:lastindex(text)]
    groups = String[]
    stop = lastindex(digits)
    while stop >= firstindex(digits)
        start = max(firstindex(digits), stop - 2)
        pushfirst!(groups, digits[start:stop])
        start == firstindex(digits) && break
        stop = prevind(digits, start)
    end
    sign * join(groups, ",")
end
fmt_ms(value::Real) = @sprintf("%.3f", value)

function fmt_value(value::Real, lower_better::Bool)
    lower_better ? fmt_ms(value) : fmt_int(value)
end

function report_env(report, key::Symbol, default="")
    env = report[:environment]
    haskey(env, key) ? string(env[key]) : default
end

function comparison_markdown(reports)
    rows = [
        (; label="Publish batch buffered msg/s", lower_better=false,
         natter="benchmarks.publish_buffered_batch.messages_per_second",
         other="benchmarks.publish_batch.messages_per_second"),
        (; label="Publish + flush each msg/s", lower_better=false,
         natter="benchmarks.publish_flush_each.messages_per_second",
         other="benchmarks.publish_flush_each.messages_per_second"),
        (; label="Callback dispatch inline batch msg/s", lower_better=false,
         natter="benchmarks.callback_dispatch_buffered_batch_inline.messages_per_second",
         other="benchmarks.callback_dispatch.messages_per_second"),
        (; label="Request/reply req/s", lower_better=false,
         natter="benchmarks.request_reply.requests_per_second",
         other="benchmarks.request_reply.requests_per_second"),
        (; label="Request p50 latency ms", lower_better=true,
         natter="benchmarks.request_reply.latency_ms_p50",
         other="benchmarks.request_reply.latency_ms_p50"),
        (; label="Request p95 latency ms", lower_better=true,
         natter="benchmarks.request_reply.latency_ms_p95",
         other="benchmarks.request_reply.latency_ms_p95"),
    ]

    natter_env = reports.natter[:environment]
    messages = string(natter_env[:messages])
    requests = string(natter_env[:requests])
    payload = string(natter_env[:payload_bytes])
    url = string(natter_env[:url])
    generated = string(reports.natter[:generated_at])
    julia_flags = report_env(reports.natter, :julia_flags, "--startup-file=no -O3 --check-bounds=no -C native")
    nats_image = report_env(reports.natter, :nats_image, "nats:2.11")
    trials = report_env(reports.natter, :trials, "1")

    lines = String[]
    push!(lines, "Benchmark parameters: `$messages` messages, `$requests` requests, `$payload` byte payload, `$trials` trials per client, URL `$url`. Timed regions exclude startup, package loading, compilation/build time, dependency downloads, and benchmark warmup.")
    push!(lines, "")
    push!(lines, "Benchmarks use each client's common high-level publish, subscribe, request, and flush APIs. Batch publish reuses stable subject and payload values in every client: Natter.jl uses `prepare_publish`, Rust uses prebuilt `Subject` and `Bytes`, and Go/Python reuse their subject string and payload buffer. Natter.jl callback dispatch uses `callback_mode=:inline` in this comparison; the Natter-only report also includes task-backed callback rows. The table reports median-of-`$trials` results for each metric.")
    push!(lines, "")
    push!(lines, "Flush semantics differ by client: Natter.jl, Go, and Python flush with a server PING/PONG round trip; Rust `async-nats` flush waits for the client writer/socket flush. Treat the Rust publish-plus-flush-each value as client-flush throughput, while request/reply rows are server round trips for all clients.")
    push!(lines, "")
    push!(lines, "Optimization modes: Natter.jl runs with `julia $julia_flags`; Go runs from an explicit `go build -trimpath` binary; Rust runs from `cargo build --release`; the NATS server uses the official `$nats_image` image.")
    push!(lines, "")
    push!(lines, "| Metric | Natter.jl | Go nats.go | Rust nats.rs | Python nats.py |")
    push!(lines, "| :--- | ---: | ---: | ---: | ---: |")
    for row in rows
        natter = nested(reports.natter, row.natter)
        python = nested(reports.python, row.other)
        rust = nested(reports.rust, row.other)
        go = nested(reports.go, row.other)
        push!(lines, "| $(row.label) | $(fmt_value(natter, row.lower_better)) | $(fmt_value(go, row.lower_better)) | $(fmt_value(rust, row.lower_better)) | $(fmt_value(python, row.lower_better)) |")
    end
    push!(lines, "")
    push!(lines, "Generated from benchmark JSON artifacts at `$generated`.")
    join(lines, "\n")
end

function replace_marked_section(path::AbstractString, content::AbstractString)
    text = read(path, String)
    start = findfirst(START_MARKER, text)
    stop = findfirst(END_MARKER, text)
    if isnothing(start) || isnothing(stop) || last(start) > first(stop)
        throw(ArgumentError("missing benchmark table markers in $path"))
    end
    replacement = string(START_MARKER, "\n", content, "\n", END_MARKER)
    updated = string(text[firstindex(text):prevind(text, first(start))],
                     replacement,
                     text[nextind(text, last(stop)):lastindex(text)])
    write(path, updated)
    nothing
end

function main(args=ARGS)
    reports = (;
        natter=read_report(arg_value(args, "--natter")),
        python=read_report(arg_value(args, "--python")),
        rust=read_report(arg_value(args, "--rust")),
        go=read_report(arg_value(args, "--go")),
    )
    table = comparison_markdown(reports)

    output = arg_value(args, "--output", "")
    if !isempty(output)
        mkpath(dirname(output))
        write(output, table * "\n")
    end

    readme = arg_value(args, "--update-readme", "")
    isempty(readme) || replace_marked_section(readme, table)

    docs = arg_value(args, "--update-docs", "")
    isempty(docs) || replace_marked_section(docs, table)

    print(table)
    println()
end

main()
