using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using MemoryPack;

namespace GoodRpc;

/// <summary>
/// Common infrastructure shared by all generated dispatchers.
/// </summary>
public abstract class RpcDispatcherBase<TInterface> : IDisposable
    where TInterface : class
{
    private readonly RpcTransport _transport;
    private readonly IDisposable _subscription;
    private int _disposed;
    private readonly ConcurrentDictionary<PeerId, CancellationTokenSource> _peerCts = new();
    private readonly ConcurrentDictionary<(PeerId, int), CancellationTokenSource> _inFlight = new();

    /// <summary>
    /// The user-supplied handler implementation.
    /// </summary>
    protected TInterface Handler { get; }

    protected RpcDispatcherBase(RpcTransport transport, TInterface handler)
    {
        _transport = transport;
        Handler = handler;
        _subscription = transport.SubscribeAll(OnMessage);
        transport.Disconnected += OnDisconnected;
    }

    /// <summary>
    /// Handle a request message.
    /// </summary>
    protected abstract ValueTask DispatchRequestAsync(
        PeerId peer,
        int seq,
        ulong hash,
        ReadOnlyMemory<byte> payload
    );

    /// <summary>
    /// Handle a notification message (void method).
    /// </summary>
    protected abstract void DispatchNotification(
        PeerId peer,
        ulong hash,
        ReadOnlyMemory<byte> payload
    );

    /// <summary>
    /// Set up RPC context for an incoming request.
    /// Returns the peer-scoped <see cref="CancellationToken"/>.
    /// </summary>
    protected CancellationToken BeginRequest(PeerId peer, byte channel, DeliveryType delivery)
    {
        var peerCts = _peerCts.GetOrAdd(peer, static _ => new CancellationTokenSource());
        RpcContext.CurrentPeer = peer;
        RpcContext.CurrentChannel = channel;
        RpcContext.CurrentDelivery = delivery;
        RpcContext.CurrentCancellation = peerCts.Token;
        return peerCts.Token;
    }

    /// <summary>
    /// Set up RPC context AND allocate a linked CTS for the per-request
    /// <see cref="CancellationToken"/> parameter.
    /// </summary>
    protected CancellationToken BeginRequest(
        PeerId peer,
        int seq,
        byte channel,
        DeliveryType delivery,
        bool hasCancellationToken
    )
    {
        var peerCts = _peerCts.GetOrAdd(peer, static _ => new CancellationTokenSource());
        RpcContext.CurrentPeer = peer;
        RpcContext.CurrentChannel = channel;
        RpcContext.CurrentDelivery = delivery;

        if (!hasCancellationToken)
        {
            RpcContext.CurrentCancellation = peerCts.Token;
            return peerCts.Token;
        }

        var requestCts = CancellationTokenSource.CreateLinkedTokenSource(peerCts.Token);
        _inFlight[(peer, seq)] = requestCts;
        RpcContext.CurrentCancellation = requestCts.Token;
        return requestCts.Token;
    }

    /// <summary>
    /// Set up RPC context for an incoming notification (<c>void</c> method).
    /// </summary>
    protected void BeginNotification(PeerId peer, byte channel, DeliveryType delivery)
    {
        RpcContext.CurrentPeer = peer;
        RpcContext.CurrentChannel = channel;
        RpcContext.CurrentDelivery = delivery;
        RpcContext.CurrentCancellation = _peerCts
            .GetOrAdd(peer, static _ => new CancellationTokenSource())
            .Token;
    }

    /// <summary>
    /// Write the 5-byte success-response header into <paramref name="writer"/>.
    /// </summary>
    protected static void WriteResponseHeader(ArrayBufferWriter<byte> writer, int seq)
    {
        var span = writer.GetSpan(5);
        span[0] = 1;
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(1), seq);
        writer.Advance(5);
    }

    /// <summary>
    /// Send a success response.
    /// </summary>
    protected ValueTask SendResponseAsync(
        PeerId peer,
        ArrayBufferWriter<byte> writer,
        RpcSendOptions opts
    )
    {
        return _transport.SendAsync(peer, writer.WrittenMemory, opts);
    }

    /// <summary>
    /// Send an error response for a request that failed in the handler.
    /// </summary>
    protected ValueTask SendErrorAsync(PeerId peer, int seq, RpcErrorCode code)
    {
        var w = new ArrayBufferWriter<byte>();
        var s = w.GetSpan(5);
        s[0] = 2;
        BinaryPrimitives.WriteInt32LittleEndian(s.Slice(1), seq);
        w.Advance(5);
        MemoryPackSerializer.Serialize(w, new RpcError(code));
        return _transport.SendAsync(peer, w.WrittenMemory, new RpcSendOptions());
    }

    private void OnMessage(RpcMessage msg)
    {
        if (_disposed != 0)
            return;

        var span = msg.Payload.Span;
        if (span.Length < 1)
            return;

        switch (span[0])
        {
            case 0: // request
                if (span.Length < 13)
                    return;
                var seq = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(1));
                var hash = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(5));
                var payload = msg.Payload.Slice(13);
                _ = HandleRequestAsync(msg.Peer, seq, hash, payload);
                break;

            case 3: // notification
                if (span.Length < 13)
                    return;
                var notifHash = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(5));
                var notifPayload = msg.Payload.Slice(13);
                try
                {
                    DispatchNotification(msg.Peer, notifHash, notifPayload);
                }
                catch { }
                break;

            case 6: // cancellation
                if (span.Length < 5)
                    return;
                var cancelSeq = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(1));
                if (_inFlight.TryGetValue((msg.Peer, cancelSeq), out var cts))
                    cts.Cancel();
                break;
        }
    }

    private async ValueTask HandleRequestAsync(
        PeerId peer,
        int seq,
        ulong hash,
        ReadOnlyMemory<byte> payload
    )
    {
        try
        {
            await DispatchRequestAsync(peer, seq, hash, payload);
        }
        catch (OperationCanceledException) { }
        catch (Exception)
        {
            try
            {
                await SendErrorAsync(peer, seq, RpcErrorCode.HandlerException);
            }
            catch { }
        }
        finally
        {
            if (_inFlight.TryRemove((peer, seq), out var cts))
                cts.Dispose();
            RpcContext.Reset();
        }
    }

    private void OnDisconnected(PeerId peer)
    {
        if (_peerCts.TryRemove(peer, out var cts))
        {
            cts.Cancel();
            cts.Dispose();
        }
    }

    public virtual void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        _subscription.Dispose();
        _transport.Disconnected -= OnDisconnected;

        foreach (var peer in _peerCts.Keys.ToArray())
        {
            if (_peerCts.TryRemove(peer, out var cts))
            {
                cts.Cancel();
                cts.Dispose();
            }
        }
        _peerCts.Clear();

        foreach (var key in _inFlight.Keys.ToArray())
        {
            if (_inFlight.TryRemove(key, out var cts))
                cts.Dispose();
        }
        _inFlight.Clear();
    }
}
