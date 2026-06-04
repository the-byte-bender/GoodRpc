using MemoryPack;

namespace GoodRpc;

/// <summary>
/// Opaque peer identifier assigned by the transport.
/// </summary>
[MemoryPackable]
public readonly partial record struct PeerId(long Value);

/// <summary>
/// Delivery guarantees for an RPC message.
/// </summary>
public enum DeliveryType : byte
{
    /// <summary>
    /// Messages are guaranteed to arrive and be processed by remote, and to be processed in the order they were sent.
    /// </summary>
    /// <remarks>
    /// Every Transport must be guaranteed to support ReliableOrdered. Transports may promote unsupported modes to ReliableOrdered, but must never throw or lose messages for unsupported modes.
    /// </remarks>
    ReliableOrdered = 0,

    /// <summary>
    /// Messages are guaranteed to arrive and be processed by remote, but may be processed in any order.
    /// </summary>
    /// <remarks>
    /// Transports that don't support ReliableUnordered must silently behave as ReliableOrdered.
    /// </remarks>
    ReliableUnordered = 1,

    /// <summary>
    /// Messages may be lost and may be processed in any order. No delivery guarantees.
    /// </summary>
    /// <remarks>
    /// Transports that don't support Unreliable must behave as ReliableUnordered, ReliableOrdered, or UnreliableOrdered.
    /// </remarks>
    Unreliable = 2,

    /// <summary>
    /// Messages may be lost but received messages are guaranteed to be processed in the order they were sent.
    /// </summary>
    /// <remarks>
    /// Transports that don't support UnreliableOrdered must behave as ReliableOrdered.
    /// </remarks>
    UnreliableOrdered = 3,
}

/// <summary>
/// Per-method send configuration.
/// Transports that don't support a feature safely ignore it.
/// </summary>
public readonly record struct RpcSendOptions(
    byte Channel = 0,
    DeliveryType Delivery = DeliveryType.ReliableOrdered,
    int TimeoutMs = 0,
    bool Compress = false
);

/// <summary>
/// Context the transport observes about a received message.
/// Set by the transport when firing OnIncomingMessage.
/// </summary>
public readonly record struct RpcReceiveContext(
    byte Channel = 0,
    DeliveryType Delivery = DeliveryType.ReliableOrdered
);

/// <summary>
/// A message received from a specific peer.
/// </summary>
public readonly record struct RpcMessage(
    PeerId Peer,
    ReadOnlyMemory<byte> Payload,
    RpcReceiveContext Context
);
