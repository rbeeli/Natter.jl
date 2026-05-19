#!/usr/bin/env julia

using Dates
using JSON3

function arg_value(args::Vector{String}, name::String, default::Union{String,Nothing}=nothing)
    index = findfirst(==(name), args)
    if isnothing(index)
        isnothing(default) && throw(ArgumentError("missing required argument $name"))
        return default
    end
    index == length(args) && throw(ArgumentError("missing value for $name"))
    args[index + 1]
end

function split_args(args::Vector{String})
    separator = findfirst(==("--"), args)
    isnothing(separator) && throw(ArgumentError("expected -- before input report paths"))
    args[1:separator - 1], args[separator + 1:end]
end

function to_julia(value)
    if value isa JSON3.Object
        return Dict(string(k) => to_julia(v) for (k, v) in pairs(value))
    elseif value isa JSON3.Array
        return Any[to_julia(v) for v in value]
    end
    value
end

function read_report(path::AbstractString)
    isfile(path) || throw(ArgumentError("benchmark report does not exist: $path"))
    to_julia(JSON3.read(read(path, String)))
end

is_rate_key(key::AbstractString) = endswith(key, "_per_second")

function equivalent_values(values::Vector)
    first_value = first(values)
    all(==(first_value), values)
end

function aggregate_values(key::AbstractString, values::Vector)
    equivalent_values(values) && return first(values)

    if all(v -> v isa Real, values)
        return is_rate_key(key) ? maximum(values) : minimum(values)
    elseif all(v -> v isa AbstractDict, values)
        keys_union = sort!(collect(reduce(union, (Set(keys(v)) for v in values))))
        return Dict(k => aggregate_values(k, Any[v[k] for v in values if haskey(v, k)])
                    for k in keys_union)
    elseif all(v -> v isa AbstractVector, values)
        return first(values)
    end
    first(values)
end

function aggregate_reports(client::String, paths::Vector{String})
    reports = read_report.(paths)
    isempty(reports) && throw(ArgumentError("at least one input report is required"))

    environment = Dict{String,Any}(get(first(reports), "environment", Dict{String,Any}()))
    environment["client"] = get(environment, "client", client)
    environment["trials"] = string(length(reports))
    environment["aggregate"] = "best_of_$(length(reports))_minimum_time_per_metric"
    environment["trial_reports"] = [basename(path) for path in paths]

    Dict(
        "generated_at" => string(now(UTC)),
        "environment" => environment,
        "benchmarks" => aggregate_values("benchmarks", Any[report["benchmarks"] for report in reports]),
    )
end

function write_json(path::AbstractString, report)
    mkpath(dirname(path))
    open(path, "w") do io
        JSON3.pretty(io, report)
        println(io)
    end
    nothing
end

function main(args::Vector{String}=ARGS)
    options, inputs = split_args(args)
    client = arg_value(options, "--client")
    output = arg_value(options, "--output")
    report = aggregate_reports(client, inputs)
    write_json(output, report)
    println(read(output, String))
end

main()
