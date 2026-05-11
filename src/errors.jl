abstract type NatterError <: Exception end

struct TimeoutError <: NatterError
    message::String
end
TimeoutError() = TimeoutError("operation timed out")

struct NoRespondersError <: NatterError
    subject::String
end

struct ConnectionClosedError <: NatterError
    message::String
end
ConnectionClosedError() = ConnectionClosedError("connection is closed")

struct ConnectionReconnectingError <: NatterError
    message::String
end
ConnectionReconnectingError() = ConnectionReconnectingError("connection is reconnecting")

struct ConnectionDrainingError <: NatterError
    message::String
end
ConnectionDrainingError() = ConnectionDrainingError("connection is draining")

struct ProtocolError <: NatterError
    message::String
end

struct AuthorizationError <: NatterError
    message::String
end
AuthorizationError() = AuthorizationError("authorization failed")

struct NoServersError <: NatterError
    message::String
end
NoServersError() = NoServersError("no servers available")

struct MaxPayloadError <: NatterError
    limit::Int
    actual::Int
end

struct OutboundBufferLimitError <: NatterError
    limit::Int
    actual::Int
end

struct SlowConsumerError <: NatterError
    subject::String
    sid::Int
    message::String
end

struct JetStreamError <: NatterError
    code::Int
    err_code::Union{Int,Nothing}
    description::String
end

struct UnsupportedFeatureError <: NatterError
    feature::String
end

struct CleanupError <: NatterError
    operation::String
    cause::Any
end

function Base.showerror(io::IO, err::TimeoutError)
    print(io, "Natter.TimeoutError: ", err.message)
end
function Base.showerror(io::IO, err::NoRespondersError)
    print(io, "Natter.NoRespondersError: no responders for subject ", err.subject)
end
function Base.showerror(io::IO, err::ConnectionClosedError)
    print(io, "Natter.ConnectionClosedError: ", err.message)
end
function Base.showerror(io::IO, err::ConnectionReconnectingError)
    print(io, "Natter.ConnectionReconnectingError: ", err.message)
end
function Base.showerror(io::IO, err::ConnectionDrainingError)
    print(io, "Natter.ConnectionDrainingError: ", err.message)
end
function Base.showerror(io::IO, err::ProtocolError)
    print(io, "Natter.ProtocolError: ", err.message)
end
function Base.showerror(io::IO, err::AuthorizationError)
    print(io, "Natter.AuthorizationError: ", err.message)
end
function Base.showerror(io::IO, err::NoServersError)
    print(io, "Natter.NoServersError: ", err.message)
end
function Base.showerror(io::IO, err::MaxPayloadError)
    print(io, "Natter.MaxPayloadError: payload ", err.actual, " exceeds server limit ", err.limit)
end
function Base.showerror(io::IO, err::OutboundBufferLimitError)
    print(io, "Natter.OutboundBufferLimitError: pending bytes ", err.actual, " exceeds limit ", err.limit)
end
function Base.showerror(io::IO, err::SlowConsumerError)
    print(io, "Natter.SlowConsumerError: ", err.message, " subject=", err.subject, " sid=", err.sid)
end
function Base.showerror(io::IO, err::JetStreamError)
    print(io, "Natter.JetStreamError: code=", err.code)
    isnothing(err.err_code) || print(io, " err_code=", err.err_code)
    print(io, " ", err.description)
end
function Base.showerror(io::IO, err::UnsupportedFeatureError)
    print(io, "Natter.UnsupportedFeatureError: ", err.feature)
end
function Base.showerror(io::IO, err::CleanupError)
    print(io, "Natter.CleanupError: ", err.operation, " failed: ")
    showerror(io, err.cause)
end
