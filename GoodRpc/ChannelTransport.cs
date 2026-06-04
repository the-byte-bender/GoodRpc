using System.Threading.Channels;

namespace GoodRpc;

/// <summary>
/// In-process transport using System.Threading.Channels.
/// </summary>
public sealed class ChannelTransport : RpcTransport
{
    private readonly Channel<RawEnvelope> _sender;
    private readonly Channel<RawEnvelope> _receiver;
    private readonly PeerId _remotePeer;
    private readonly CancellationTokenSource _cts = new();
    private readonly CancellationToken _ct;

    private readonly TaskCompletionSource _loopCompletion = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    private int _started;
    private int _disposed;

    internal readonly record struct RawEnvelope(
        ReadOnlyMemory<byte> Payload,
        RpcReceiveContext Context
    );

    internal ChannelTransport(
        PeerId remotePeer,
        Channel<RawEnvelope> sender,
        Channel<RawEnvelope> receiver
    )
    {
        _remotePeer = remotePeer;
        _sender = sender;
        _receiver = receiver;
        _ct = _cts.Token;
    }

    /// <summary>The single remote peer this transport communicates with.</summary>
    public PeerId RemotePeer => _remotePeer;

    /// <summary>
    /// Fire Connected and start the message pump. Returns when disposed or disconnected.
    /// </summary>
    public async Task RunAsync()
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
        {
            throw new InvalidOperationException("The transport has already been started.");
        }

        if (Volatile.Read(ref _disposed) != 0)
        {
            _loopCompletion.TrySetResult();
            throw new ObjectDisposedException(nameof(ChannelTransport));
        }

        OnPeerConnected(_remotePeer);
        try
        {
            await foreach (var envelope in _receiver.Reader.ReadAllAsync(_ct))
            {
                OnIncomingMessage(_remotePeer, envelope.Payload, envelope.Context);
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            OnPeerDisconnected(_remotePeer);
            _loopCompletion.TrySetResult();
        }
    }

    public override ValueTask SendAsync(
        PeerId peer,
        ReadOnlyMemory<byte> message,
        RpcSendOptions options,
        CancellationToken ct = default
    )
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            throw new ObjectDisposedException(nameof(ChannelTransport));
        }

        var copy = message.ToArray();
        var context = new RpcReceiveContext(options.Channel, options.Delivery);
        _sender.Writer.TryWrite(new RawEnvelope(copy, context));
        return ValueTask.CompletedTask;
    }

    public override ValueTask DisconnectAsync(PeerId peer, CancellationToken ct = default)
    {
        if (Volatile.Read(ref _disposed) == 0)
        {
            try
            {
                _cts.Cancel();
            }
            catch (ObjectDisposedException) { }
        }
        return ValueTask.CompletedTask;
    }

    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        try
        {
            _cts.Cancel();
        }
        catch (ObjectDisposedException) { }

        _sender.Writer.TryComplete();

        if (Volatile.Read(ref _started) != 0)
        {
            await _loopCompletion.Task;
        }

        _cts.Dispose();
    }

    /// <summary>
    /// Create a connected pair of ChannelTransports.
    /// </summary>
    public static (ChannelTransport A, ChannelTransport B) CreatePair()
    {
        var ab = Channel.CreateUnbounded<RawEnvelope>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = true }
        );
        var ba = Channel.CreateUnbounded<RawEnvelope>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = true }
        );

        var peerA = new PeerId(1);
        var peerB = new PeerId(2);

        return (new ChannelTransport(peerB, ab, ba), new ChannelTransport(peerA, ba, ab));
    }
}
