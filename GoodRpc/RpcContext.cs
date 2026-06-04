namespace GoodRpc;

/// <summary>
/// Ambient context set by the transport and dispatcher before invoking a handler.
/// </summary>
public static class RpcContext
{
    private static readonly AsyncLocal<PeerId> _peer = new();
    private static readonly AsyncLocal<CancellationToken> _cancellation = new();
    private static readonly AsyncLocal<byte> _channel = new();
    private static readonly AsyncLocal<DeliveryType> _delivery = new();

    /// <summary>The peer that sent the current request/notification.</summary>
    public static PeerId CurrentPeer
    {
        get => _peer.Value;
        set => _peer.Value = value;
    }

    /// <summary>Cancellation token linked to the peer's connection lifetime.</summary>
    public static CancellationToken CurrentCancellation
    {
        get => _cancellation.Value;
        set => _cancellation.Value = value;
    }

    /// <summary>The channel the current message arrived on.</summary>
    public static byte CurrentChannel
    {
        get => _channel.Value;
        set => _channel.Value = value;
    }

    /// <summary>The delivery type of the current message.</summary>
    public static DeliveryType CurrentDelivery
    {
        get => _delivery.Value;
        set => _delivery.Value = value;
    }

    public static void Reset()
    {
        _peer.Value = default;
        _cancellation.Value = default;
        _channel.Value = 0;
        _delivery.Value = DeliveryType.ReliableOrdered;
    }
}
