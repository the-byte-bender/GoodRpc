namespace GoodRpc;

/// <summary>
/// Error codes sent over the wire when an RPC fails.
/// </summary>
public enum RpcErrorCode : byte
{
    Unknown = 0,
    MethodNotFound = 1,
    HandlerException = 2,
    Timeout = 3,
    Disconnected = 4,
    SerializationError = 5,
}

[MemoryPack.MemoryPackable]
public sealed partial record RpcError(RpcErrorCode Code, string? Message = null);

/// <summary>
/// Thrown on the client/proxy side when an RPC call fails.
/// </summary>
public class RpcException : Exception
{
    public RpcErrorCode Code { get; }

    public RpcException(RpcErrorCode code, string? message = null)
        : base(message ?? $"RPC error: {code}")
    {
        Code = code;
    }
}

/// <summary>
/// Thrown when an RPC call times out.
/// </summary>
public sealed class RpcTimeoutException : RpcException
{
    public RpcTimeoutException()
        : base(RpcErrorCode.Timeout) { }
}

/// <summary>
/// Thrown when the peer disconnects while calls are in flight.
/// </summary>
public sealed class RpcDisconnectedException : RpcException
{
    public RpcDisconnectedException()
        : base(RpcErrorCode.Disconnected) { }
}
