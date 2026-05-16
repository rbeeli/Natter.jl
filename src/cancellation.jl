mutable struct CancellationRegistration
    condition::Base.GenericCondition{ReentrantLock}
end

mutable struct CancellationState
    lock::ReentrantLock
    condition::Base.GenericCondition{ReentrantLock}
    cancelled::Bool
    registrations::Vector{CancellationRegistration}
end

function CancellationState()
    lock = ReentrantLock()
    CancellationState(lock, Base.Threads.Condition(lock), false, CancellationRegistration[])
end

"""
    CancellationToken

Read-only cancellation handle accepted by blocking Natter operations.
Create one with `CancellationSource()` and `cancellation_token(source)`.
"""
struct CancellationToken
    state::CancellationState
end

"""
    CancellationSource()

Owner-side cancellation handle. Call `cancel!(source)` to request cooperative
cancellation for operations using `cancellation_token(source)`.
"""
struct CancellationSource
    token::CancellationToken
end

CancellationSource() = CancellationSource(CancellationToken(CancellationState()))

cancellation_token(source::CancellationSource)::CancellationToken = source.token

const MaybeCancellationToken = Union{CancellationToken,Nothing}

iscancelled(::Nothing)::Bool = false

function iscancelled(token::CancellationToken)::Bool
    state = token.state
    lock(state.condition)
    try
        state.cancelled
    finally
        unlock(state.condition)
    end
end

function _throw_if_cancelled(token::MaybeCancellationToken)
    iscancelled(token) && throw(CancelledError())
    nothing
end

function _delete_cancellation_registration_locked!(registrations::Vector{CancellationRegistration},
                                                   registration::CancellationRegistration)
    for i in eachindex(registrations)
        if registrations[i] === registration
            deleteat!(registrations, i)
            return true
        end
    end
    false
end

function _register_cancellation_waiter(token::Nothing,
                                       condition::Base.GenericCondition{ReentrantLock})
    nothing
end

function _register_cancellation_waiter(token::CancellationToken,
                                       condition::Base.GenericCondition{ReentrantLock})
    state = token.state
    registration = CancellationRegistration(condition)
    lock(state.condition)
    try
        state.cancelled && throw(CancelledError())
        push!(state.registrations, registration)
    finally
        unlock(state.condition)
    end
    registration
end

function _deregister_cancellation_waiter!(::Nothing, registration)
    nothing
end

function _deregister_cancellation_waiter!(token::CancellationToken,
                                          registration::Union{CancellationRegistration,Nothing})
    isnothing(registration) && return nothing
    state = token.state
    lock(state.condition)
    try
        _delete_cancellation_registration_locked!(state.registrations, registration)
    finally
        unlock(state.condition)
    end
    nothing
end

function _notify_cancellation_registration(registration::CancellationRegistration)
    condition = registration.condition
    lock(condition)
    try
        notify(condition; all=true)
    finally
        unlock(condition)
    end
    nothing
end

function cancel!(source::CancellationSource)::Bool
    state = source.token.state
    registrations = CancellationRegistration[]
    lock(state.condition)
    try
        state.cancelled && return false
        state.cancelled = true
        append!(registrations, state.registrations)
        empty!(state.registrations)
        notify(state.condition; all=true)
    finally
        unlock(state.condition)
    end
    for registration in registrations
        _notify_cancellation_registration(registration)
    end
    true
end
