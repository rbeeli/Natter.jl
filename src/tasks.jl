function _run_spawned_task(name::Symbol, f::F) where {F}
    task_local_storage(:natter_task_name, name) do
        f()
    end
end

function _spawn_control(f::F, name::Symbol) where {F}
    Threads.@spawn :interactive _run_spawned_task(name, f)
end

function _spawn_work(f::F, name::Symbol) where {F}
    Threads.@spawn :default _run_spawned_task(name, f)
end

function _spawn_sticky(f::F, name::Symbol) where {F}
    task = Task(() -> _run_spawned_task(name, f))
    task.sticky = true
    schedule(task)
    task
end

_spawn_control(name::Symbol, f::F) where {F} = _spawn_control(f, name)
_spawn_work(name::Symbol, f::F) where {F} = _spawn_work(f, name)
_spawn_sticky(name::Symbol, f::F) where {F} = _spawn_sticky(f, name)
