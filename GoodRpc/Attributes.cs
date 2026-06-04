namespace GoodRpc;

/// <summary>
/// Marks an interface as an RPC service contract.
/// </summary>
[AttributeUsage(AttributeTargets.Interface)]
public sealed class RpcServiceAttribute : Attribute { }

/// <summary>
/// Per-method configuration for an RPC endpoint.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class RpcMethodAttribute : Attribute
{
    /// <summary>
    /// Logical channel / priority lane. Defaults to 0.
    /// Transports may use this for QoS, priority, or separate streams.
    /// </summary>
    public byte Channel { get; set; }

    /// <summary>
    /// Delivery guarantee. Defaults to ReliableOrdered.
    /// Transports that don't support a mode may demote to ReliableOrdered.
    /// </summary>
    public DeliveryType Delivery { get; set; }

    /// <summary>
    /// Per-call timeout in milliseconds. 0 = no timeout / use default.
    /// Only meaningful for Task/Task&lt;T&gt; methods.
    /// </summary>
    public int TimeoutMs { get; set; }

    /// <summary>
    /// Hint to apply compression to the payload.
    /// </summary>
    public bool Compress { get; set; }
}
