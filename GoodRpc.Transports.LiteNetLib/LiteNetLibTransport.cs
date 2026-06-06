using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using LiteNetLib;
using LiteNetLib.Utils;

namespace GoodRpc.Transports.LiteNetLib;

/// <summary>
/// LiteNetLib transport for GoodRpc.
/// </summary>
public sealed class LiteNetLibTransport : RpcTransport, INetEventListener
{
    private long _nextPeerId;
    private readonly ConcurrentDictionary<NetPeer, PeerId> _peerToId = new();
    private readonly ConcurrentDictionary<PeerId, NetPeer> _idToPeer = new();
    private readonly INetEventListener? _userListener;

    private bool _suppressHandling;

    /// <summary>
    /// Create a transport with no raw listener.
    /// </summary>
    public LiteNetLibTransport()
    {
        _userListener = null;
    }

    /// <summary>
    /// Create a transport that forwards callbacks to <paramref name="userListener"/>
    /// before performing RPC handling.
    /// </summary>
    /// <param name="userListener">
    /// Optional raw <see cref="INetEventListener"/>. Runs first on every callback.
    /// Call <see cref="SuppressHandling"/> from within your listener to skip
    /// RPC dispatch for the current callback.
    /// </param>
    public LiteNetLibTransport(INetEventListener userListener)
    {
        _userListener = userListener ?? throw new ArgumentNullException(nameof(userListener));
    }

    /// <summary>
    /// Call from within your raw <see cref="INetEventListener"/> callback
    /// to prevent the transport from processing the current message as RPC.
    /// Has no effect when called outside a callback.
    /// </summary>
    public void SuppressHandling() => _suppressHandling = true;

    /// <summary>
    /// Get the GoodRpc <see cref="PeerId"/> mapped to a LiteNetLib
    /// <see cref="NetPeer"/>, if the peer is currently connected and tracked.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public PeerId? GetPeerId(NetPeer peer) => _peerToId.TryGetValue(peer, out var id) ? id : null;

    /// <summary>
    /// Get the LiteNetLib <see cref="NetPeer"/> mapped to a GoodRpc
    /// <see cref="PeerId"/>, if the peer is currently connected and tracked.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public NetPeer? GetNetPeer(PeerId id) => _idToPeer.TryGetValue(id, out var peer) ? peer : null;

    /// <inheritdoc />
    public override ValueTask SendAsync(
        PeerId peer,
        ReadOnlyMemory<byte> message,
        RpcSendOptions options,
        CancellationToken ct = default
    )
    {
        if (!_idToPeer.TryGetValue(peer, out var np))
            return ValueTask.CompletedTask;

        np.Send(message.Span, options.Channel, MapDelivery(options.Delivery));
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public override ValueTask DisconnectAsync(PeerId peer, CancellationToken ct = default)
    {
        if (_idToPeer.TryGetValue(peer, out var np))
            np.Disconnect();
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    protected override ValueTask DisposeAsyncCore()
    {
        _peerToId.Clear();
        _idToPeer.Clear();
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    void INetEventListener.OnPeerConnected(NetPeer peer)
    {
        _userListener?.OnPeerConnected(peer);

        var id = new PeerId(Interlocked.Increment(ref _nextPeerId));
        _peerToId[peer] = id;
        _idToPeer[id] = peer;
        OnPeerConnected(id);
    }

    /// <inheritdoc />
    void INetEventListener.OnPeerDisconnected(NetPeer peer, DisconnectInfo disconnectInfo)
    {
        _userListener?.OnPeerDisconnected(peer, disconnectInfo);

        if (_peerToId.TryRemove(peer, out var id))
        {
            _idToPeer.TryRemove(id, out _);
            OnPeerDisconnected(id);
        }
    }

    /// <inheritdoc />
    void INetEventListener.OnNetworkError(IPEndPoint endPoint, SocketError socketError) =>
        _userListener?.OnNetworkError(endPoint, socketError);

    /// <inheritdoc />
    void INetEventListener.OnNetworkReceive(
        NetPeer peer,
        NetPacketReader reader,
        byte channelNumber,
        DeliveryMethod deliveryMethod
    )
    {
        var savedPosition = reader.Position;

        _suppressHandling = false;
        _userListener?.OnNetworkReceive(peer, reader, channelNumber, deliveryMethod);

        if (_suppressHandling)
            return;

        reader.SetPosition(savedPosition);

        if (!_peerToId.TryGetValue(peer, out var id))
            return;

        var payload = reader.PeekRemainingBytesMemory();

        OnIncomingMessage(
            id,
            payload,
            new RpcReceiveContext(channelNumber, MapDelivery(deliveryMethod))
        );
    }

    /// <inheritdoc />
    void INetEventListener.OnNetworkReceiveUnconnected(
        IPEndPoint remoteEndPoint,
        NetPacketReader reader,
        UnconnectedMessageType messageType
    ) => _userListener?.OnNetworkReceiveUnconnected(remoteEndPoint, reader, messageType);

    /// <inheritdoc />
    void INetEventListener.OnNetworkLatencyUpdate(NetPeer peer, int latency) =>
        _userListener?.OnNetworkLatencyUpdate(peer, latency);

    /// <inheritdoc />
    void INetEventListener.OnConnectionRequest(ConnectionRequest request)
    {
        if (_userListener != null)
            _userListener.OnConnectionRequest(request);
        else
            request.Accept();
    }

    /// <inheritdoc />
    void INetEventListener.OnMessageDelivered(NetPeer peer, object userData) =>
        _userListener?.OnMessageDelivered(peer, userData);

    /// <inheritdoc />
    void INetEventListener.OnNtpResponse(NtpPacket packet) => _userListener?.OnNtpResponse(packet);

    /// <inheritdoc />
    void INetEventListener.OnPeerAddressChanged(NetPeer peer, IPEndPoint previousAddress) =>
        _userListener?.OnPeerAddressChanged(peer, previousAddress);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static DeliveryMethod MapDelivery(DeliveryType delivery) =>
        delivery switch
        {
            DeliveryType.ReliableUnordered => DeliveryMethod.ReliableUnordered,
            DeliveryType.Unreliable => DeliveryMethod.Unreliable,
            DeliveryType.UnreliableOrdered => DeliveryMethod.Sequenced,
            _ => DeliveryMethod.ReliableOrdered,
        };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static DeliveryType MapDelivery(DeliveryMethod delivery) =>
        delivery switch
        {
            DeliveryMethod.ReliableUnordered => DeliveryType.ReliableUnordered,
            DeliveryMethod.Unreliable => DeliveryType.Unreliable,
            DeliveryMethod.Sequenced => DeliveryType.UnreliableOrdered,
            _ => DeliveryType.ReliableOrdered,
        };
}
