using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using MemoryPack;

namespace GoodRpc;

/// <summary>
/// Common infrastructure shared by all generated proxies.
/// </summary>
public abstract class RpcProxyBase : IDisposable
{
    private readonly RpcTransport _transport;
    private readonly PeerId _peer;
    private readonly IDisposable _subscription;
    private int _nextSeq;
    private int _disposed;
    private readonly ConcurrentDictionary<int, Pending> _pending = new();

    private readonly record struct Pending(
        TaskCompletionSource<byte[]> Tcs,
        CancellationTokenSource Cts,
        CancellationTokenRegistration TimeoutReg,
        CancellationTokenRegistration UserCtReg
    );

    protected RpcProxyBase(RpcTransport transport, PeerId peer)
    {
        _transport = transport;
        _peer = peer;
        _subscription = transport.Subscribe(peer, OnMessage);
        transport.Disconnected += OnDisconnected;
    }

    /// <summary>
    /// Allocate the next sequence number for an outgoing request.
    /// </summary>
    protected int AllocSeq() => Interlocked.Increment(ref _nextSeq);

    /// <summary>
    /// Write the 13-byte wire header into <paramref name="writer"/>.
    /// </summary>
    protected static void WriteHeader(
        ArrayBufferWriter<byte> writer,
        byte type,
        int seq,
        ulong hash
    )
    {
        var span = writer.GetSpan(13);
        span[0] = type;
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(1), seq);
        BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(5), hash);
        writer.Advance(13);
    }

    /// <summary>
    /// Fire-and-forget send. For <c>void</c> RPC methods.
    /// </summary>
    protected void SendNotification(ArrayBufferWriter<byte> writer, RpcSendOptions opts)
    {
        _ = _transport.SendAsync(_peer, writer.WrittenMemory, opts);
    }

    /// <summary>
    /// Send a request whose response will carry a payload.
    /// Returns the raw response bytes which the caller deserializes.
    /// The caller MUST call <see cref="CompleteRequest"/> in a finally block.
    /// </summary>
    protected async Task<byte[]> SendRequestAsync(
        int seq,
        ArrayBufferWriter<byte> writer,
        RpcSendOptions opts,
        int timeoutMs,
        CancellationToken userCt = default
    )
    {
        var tcs = new TaskCompletionSource<byte[]>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        var cts =
            timeoutMs > 0 ? new CancellationTokenSource(timeoutMs) : new CancellationTokenSource();

        _pending[seq] = new Pending(tcs, cts, default, default);

        var timeoutReg = default(CancellationTokenRegistration);
        if (timeoutMs > 0)
        {
            timeoutReg = cts.Token.Register(() =>
            {
                if (_pending.TryGetValue(seq, out var p))
                    p.Tcs.TrySetException(new RpcTimeoutException());
            });
        }

        var userCtReg = default(CancellationTokenRegistration);
        if (userCt.CanBeCanceled)
        {
            userCtReg = userCt.Register(() =>
            {
                if (_pending.TryGetValue(seq, out var p))
                {
                    SendCancelMessage(seq);
                    p.Tcs.TrySetCanceled(p.Cts.Token);
                }
            });
        }

        _pending[seq] = new Pending(tcs, cts, timeoutReg, userCtReg);

        await _transport.SendAsync(_peer, writer.WrittenMemory, opts);

        return await tcs.Task;
    }

    /// <summary>
    /// Clean up bookkeeping after a request completes (success or failure).
    /// Must be called in a finally block after awaiting <see cref="SendRequestAsync"/>.
    /// </summary>
    protected void CompleteRequest(int seq)
    {
        if (_pending.TryRemove(seq, out var p))
        {
            p.TimeoutReg.Dispose();
            p.UserCtReg.Dispose();
            p.Cts.Cancel();
            p.Cts.Dispose();
        }
    }

    private void OnMessage(RpcMessage msg)
    {
        if (_disposed != 0)
            return;

        var span = msg.Payload.Span;
        if (span.Length < 5)
            return;

        var type = span[0];
        if (type is not (1 or 2)) // only care about responses
            return;

        var seq = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(1));
        if (!_pending.TryGetValue(seq, out var call))
            return;

        var payload = span.Slice(5).ToArray();
        if (type == 1)
            call.Tcs.TrySetResult(payload);
        else
            call.Tcs.TrySetException(CreateErrorException(payload));
    }

    private void OnDisconnected(PeerId peer)
    {
        if (!peer.Equals(_peer))
            return;

        Cleanup();
    }

    private void Cleanup()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        _subscription.Dispose();
        _transport.Disconnected -= OnDisconnected;

        foreach (var seq in _pending.Keys.ToArray())
        {
            if (_pending.TryRemove(seq, out var c))
            {
                c.Tcs.TrySetException(new RpcDisconnectedException());
                c.TimeoutReg.Dispose();
                c.UserCtReg.Dispose();
                c.Cts.Cancel();
                c.Cts.Dispose();
            }
        }
        _pending.Clear();
    }

    private void SendCancelMessage(int seq)
    {
        var w = new ArrayBufferWriter<byte>();
        var s = w.GetSpan(5);
        s[0] = 6;
        BinaryPrimitives.WriteInt32LittleEndian(s.Slice(1), seq);
        w.Advance(5);
        _ = _transport.SendAsync(_peer, w.WrittenMemory, new RpcSendOptions());
    }

    private static RpcException CreateErrorException(byte[] payload)
    {
        try
        {
            var err = MemoryPackSerializer.Deserialize<RpcError>(payload);
            if (err is not null)
                return new RpcException(err.Code, err.Message);
            return new RpcException(RpcErrorCode.Unknown);
        }
        catch
        {
            return new RpcException(RpcErrorCode.Unknown);
        }
    }

    /// <summary>
    /// Explicitly dispose the proxy.
    /// </summary>
    /// <remarks>
    /// It's completely safe to never manually dispose proxies; they will self-clean on disconnection of the associated peer.  Dispose is only needed if you want to proactively remove the proxy while the connection is still up.
    /// </remarks>
    public virtual void Dispose() => Cleanup();
}
