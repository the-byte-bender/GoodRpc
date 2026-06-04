namespace GoodRpc;

/// <summary>
/// Abstract base for all transports.
/// </summary>
public abstract class RpcTransport : IAsyncDisposable
{
    private readonly Dictionary<PeerId, List<Action<RpcMessage>>> _perPeer = new();
    private readonly List<Action<RpcMessage>> _global = new();

    public event Action<PeerId>? Connected;
    public event Action<PeerId>? Disconnected;

    /// <summary>
    /// Send a message to a specific peer.
    /// </summary>
    public abstract ValueTask SendAsync(
        PeerId peer,
        ReadOnlyMemory<byte> message,
        RpcSendOptions options,
        CancellationToken ct = default
    );

    /// <summary>
    /// Force-disconnect a peer.
    /// </summary>
    public abstract ValueTask DisconnectAsync(PeerId peer, CancellationToken ct = default);

    /// <summary>
    /// Shut down the transport completely.
    /// </summary>
    public abstract ValueTask DisposeAsync();

    /// <summary>
    /// Call when a message arrives from a peer. Routes to per-peer and global subscribers.
    /// </summary>
    protected void OnIncomingMessage(
        PeerId peer,
        ReadOnlyMemory<byte> payload,
        RpcReceiveContext context
    )
    {
        var msg = new RpcMessage(peer, payload, context);

        if (_perPeer.TryGetValue(peer, out var handlers))
        {
            foreach (var h in handlers)
                h(msg);
        }

        foreach (var h in _global)
            h(msg);
    }

    /// <summary>
    /// Call when a new peer connects.
    /// </summary>
    protected void OnPeerConnected(PeerId peer)
    {
        Connected?.Invoke(peer);
    }

    /// <summary>
    /// Call when a peer disconnects. Automatically drops all per-peer subscriptions.
    /// </summary>
    protected void OnPeerDisconnected(PeerId peer)
    {
        _perPeer.Remove(peer, out _);
        Disconnected?.Invoke(peer);
    }

    /// <summary>
    /// Subscribe to messages from a specific peer. Returns a disposable that unsubscribes.
    /// </summary>
    public IDisposable Subscribe(PeerId peer, Action<RpcMessage> handler)
    {
        if (!_perPeer.TryGetValue(peer, out var list))
            _perPeer[peer] = list = new List<Action<RpcMessage>>(1);

        list.Add(handler);

        return new Unsubscriber(() =>
        {
            if (_perPeer.TryGetValue(peer, out var lst))
                lst.Remove(handler);
        });
    }

    /// <summary>
    /// Subscribe to messages from ALL peers. Used by dispatchers.
    /// Returns a disposable that unsubscribes.
    /// </summary>
    public IDisposable SubscribeAll(Action<RpcMessage> handler)
    {
        _global.Add(handler);
        return new Unsubscriber(() => _global.Remove(handler));
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            _perPeer.Clear();
            _global.Clear();
            Connected = null;
            Disconnected = null;
        }
    }

    private sealed class Unsubscriber : IDisposable
    {
        private Action? _action;

        public Unsubscriber(Action action) => _action = action;

        public void Dispose()
        {
            var a = Interlocked.Exchange(ref _action, null);
            a?.Invoke();
        }
    }
}
