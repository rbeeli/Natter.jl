using TestItems

@testmodule TestHelpers begin
    using Natter
    using Random

    const N = Natter

    bytes(s) = Vector{UInt8}(codeunits(s))

    function thrown_exception(f)
        try
            f()
        catch err
            return err
        end
        throw(AssertionError("expected exception"))
    end

    function fetch_task_result(task::Task)
        try
            return fetch(task)
        catch err
            err isa TaskFailedException || rethrow()
            exceptions = Base.current_exceptions(task)
            isempty(exceptions) && rethrow()
            throw(first(exceptions).exception)
        end
    end

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

    function active_pull_deliveries(psub)
        @lock psub.close_lock copy(psub.active_deliveries)
    end

    function active_pull_delivery(psub)
        deliveries = active_pull_deliveries(psub)
        length(deliveries) == 1 || throw(AssertionError("expected exactly one active pull delivery"))
        only(deliveries)
    end

    function wait_for_pull_delivery(psub; timeout::Real=1.0)
        timedwait(timeout; pollint=0.001) do
            !isempty(active_pull_deliveries(psub))
        end == :timed_out && throw(AssertionError("timed out waiting for pull delivery"))
        active_pull_delivery(psub)
    end

    function fake_client(; opts=N.ConnectOptions(), status=N.ConnectionStatus.DISCONNECTED,
                         info=N.ServerInfo(; headers=true), read_io=nothing, write_io=nothing)
        N.Client(
            opts,
            N.Server[],
            nothing,
            nothing,
            status,
            info,
            nothing,
            read_io,
            write_io,
            ReentrantLock(),
            ReentrantLock(),
            Base.Threads.Condition(ReentrantLock()),
            N.FlushSignal(),
            nothing,
            0,
            Dict{Int,N.Subscription}(),
            nothing,
            ReentrantLock(),
            IOBuffer(),
            0,
            N.PongWaiterQueue(),
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

@testmodule IntegrationHelpers begin
    using Natter
    using Sockets

    integration_timeout() = parse(Float64, get(ENV, "NATTER_INTEGRATION_TIMEOUT", "5.0"))
    integration_connect_timeout() =
        parse(Float64, get(ENV, "NATTER_INTEGRATION_CONNECT_TIMEOUT", "10.0"))
    chaos_iterations() = parse(Int, get(ENV, "NATTER_CHAOS_ITERATIONS", "3"))
    stress_seconds() = parse(Float64, get(ENV, "NATTER_STRESS_SECONDS", "15.0"))

    function publish_and_flush(client, subject::AbstractString, data=nothing; timeout::Real=integration_timeout(), kwargs...)
        publish(client, subject, data; kwargs...)
        flush(client; timeout)
        nothing
    end

    function _proxy_close(resource, operation::String)
        try
            close(resource)
        catch err
            @debug "Natter integration proxy cleanup failed" operation exception=(err, catch_backtrace())
        end
        nothing
    end

    function _remember_proxy_resource!(resources::Vector{Any}, resource_lock::ReentrantLock, resource)
        lock(resource_lock)
        try
            push!(resources, resource)
        finally
            unlock(resource_lock)
        end
        resource
    end

    function _proxy_resources_snapshot(resources::Vector{Any}, resource_lock::ReentrantLock)
        lock(resource_lock)
        try
            return copy(resources)
        finally
            unlock(resource_lock)
        end
    end

    function _drain_proxy_gate!(gate::Channel{Bool})
        while isready(gate)
            try
                take!(gate)
            catch err
                @debug "Natter integration proxy gate drain stopped" exception=(err, catch_backtrace())
                break
            end
        end
        nothing
    end

    function _proxy_pump(from, to; before_write=nothing)
        try
            while true
                data = readavailable(from)
                isempty(data) && break
                isnothing(before_write) || before_write()
                write(to, data)
                flush(to)
            end
        catch err
            @debug "Natter integration proxy pump stopped" exception=(err, catch_backtrace())
        finally
            _proxy_close(from, "close proxy source")
            _proxy_close(to, "close proxy destination")
        end
        nothing
    end

    function start_tcp_proxy(target_host::AbstractString, target_port::Int; released::Bool=true,
                             downstream_released::Bool=true)
        server = Sockets.listen(ip"127.0.0.1", 0)
        _, proxy_port = Sockets.getsockname(server)
        resources = Any[server]
        resource_lock = ReentrantLock()
        release_gate = Channel{Bool}(1)
        release_state = Ref(released)
        downstream_gate = Channel{Bool}(1)
        downstream_state = Ref(downstream_released)

        function release!()
            if !release_state[]
                release_state[] = true
                isready(release_gate) || put!(release_gate, true)
            end
            nothing
        end

        function pause_new_connections!()
            release_state[] = false
            _drain_proxy_gate!(release_gate)
            nothing
        end

        function pause_downstream!()
            downstream_state[] = false
            nothing
        end

        function resume_downstream!()
            if !downstream_state[]
                downstream_state[] = true
                isready(downstream_gate) || put!(downstream_gate, true)
            end
            nothing
        end

        function wait_downstream!()
            if !downstream_state[]
                try
                    take!(downstream_gate)
                catch err
                    @debug "Natter integration proxy downstream wait stopped" exception=(err, catch_backtrace())
                end
            end
            nothing
        end

        function drop_connections!()
            for resource in reverse(_proxy_resources_snapshot(resources, resource_lock))
                resource === server && continue
                _proxy_close(resource, "drop proxy connection")
            end
            nothing
        end

        accept_task = @async begin
            while true
                client_sock = try
                    Sockets.accept(server)
                catch err
                    @debug "Natter integration proxy accept stopped" exception=(err, catch_backtrace())
                    break
                end
                _remember_proxy_resource!(resources, resource_lock, client_sock)

                if !release_state[]
                    try
                        take!(release_gate)
                    catch err
                        @debug "Natter integration proxy release wait stopped" exception=(err, catch_backtrace())
                        _proxy_close(client_sock, "close unreleased proxy client")
                        break
                    end
                end

                server_sock = try
                    Sockets.connect(String(target_host), target_port)
                catch err
                    @debug "Natter integration proxy target connect failed" exception=(err, catch_backtrace())
                    _proxy_close(client_sock, "close proxy client after target connect failure")
                    continue
                end
                _remember_proxy_resource!(resources, resource_lock, server_sock)

                @async _proxy_pump(client_sock, server_sock)
                @async _proxy_pump(server_sock, client_sock; before_write=wait_downstream!)
            end
        end

        function stop!()
            release!()
            for resource in reverse(_proxy_resources_snapshot(resources, resource_lock))
                _proxy_close(resource, "stop proxy")
            end
            resume_downstream!()
            timedwait(0.5; pollint=0.01) do
                istaskdone(accept_task)
            end
            nothing
        end

        (; url="nats://127.0.0.1:$(Int(proxy_port))",
         release=release!, pause_new_connections=pause_new_connections!,
         pause_downstream=pause_downstream!, resume_downstream=resume_downstream!,
         drop_connections=drop_connections!, stop=stop!)
    end
end
