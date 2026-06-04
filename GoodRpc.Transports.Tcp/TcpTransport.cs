using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;

namespace GoodRpc.Transports.Tcp;

public sealed class TcpTransport : RpcTransport
{
    private sealed class PeerState
    {
        public readonly PipeReader Reader;
        public readonly PipeWriter Writer;
        public readonly SemaphoreSlim WriteLock = new(1, 1);
        public readonly CancellationTokenSource Cts = new();

        public PeerState(NetworkStream stream)
        {
            Reader = PipeReader.Create(
                stream,
                new StreamPipeReaderOptions(pool: MemoryPool<byte>.Shared)
            );
            Writer = PipeWriter.Create(
                stream,
                new StreamPipeWriterOptions(pool: MemoryPool<byte>.Shared)
            );
        }
    }

    private readonly ConcurrentDictionary<PeerId, PeerState> _peers = new();
    private readonly Socket _socket;
    private readonly Mode _mode;
    private readonly CancellationTokenSource _shutdownCts = new();
    private readonly TaskCompletionSource _loopCompletion = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private int _nextPeerId;
    private int _disposed;
    private int _started;

    private PeerId? _clientPeer;

    private enum Mode
    {
        Client,
        Server,
    }

    public Socket Socket => _socket;

    /// <summary>
    /// The remote peer. Only valid in client mode.
    /// </summary>
    /// <Remarks>
    /// Timing the access of this property near the beginning of a connection can be tricky. A better approach is to subscribe to Connected before calling RunAsync, which will provide the peer as soon as it becomes available.
    /// </Remarks>
    public PeerId RemotePeer =>
        _mode == Mode.Client && _clientPeer.HasValue
            ? _clientPeer.Value
            : throw new InvalidOperationException(
                "RemotePeer is only available in client mode after RunAsync."
            );

    private TcpTransport(Socket socket, Mode mode)
    {
        _socket = socket;
        _mode = mode;
    }

    public async Task RunAsync()
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
            throw new InvalidOperationException("Already started.");
        if (Volatile.Read(ref _disposed) != 0)
        {
            _loopCompletion.TrySetResult();
            throw new ObjectDisposedException(nameof(TcpTransport));
        }

        try
        {
            if (_mode == Mode.Client)
            {
                var peer = new PeerId(1);
                _clientPeer = peer;
                var stream = new NetworkStream(_socket, ownsSocket: true);
                var state = new PeerState(stream);
                _peers[peer] = state;
                OnPeerConnected(peer);
                await ReadLoopAsync(peer, state, state.Cts.Token);
            }
            else
            {
                await AcceptLoopAsync(_shutdownCts.Token);
            }
        }
        finally
        {
            _loopCompletion.TrySetResult();
        }
    }

    public static async Task<TcpTransport> ConnectAsync(
        string host,
        int port,
        CancellationToken ct = default
    )
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            await socket.ConnectAsync(host, port, ct);
            return new TcpTransport(socket, Mode.Client);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    public static TcpTransport FromSocket(Socket s)
    {
        if (!s.Connected)
            throw new ArgumentException("Socket must be connected.", nameof(s));
        return new TcpTransport(s, Mode.Client);
    }

    public static TcpTransport Listen(int port) => Listen(new IPEndPoint(IPAddress.Any, port), 128);

    public static TcpTransport Listen(IPEndPoint ep, int backlog = 128)
    {
        var s = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        s.Bind(ep);
        s.Listen(backlog);
        return new TcpTransport(s, Mode.Server);
    }

    public static TcpTransport FromListener(Socket s)
    {
        if (!s.IsBound)
            throw new ArgumentException("Socket must be bound.", nameof(s));
        return new TcpTransport(s, Mode.Server);
    }

    public override async ValueTask SendAsync(
        PeerId peer,
        ReadOnlyMemory<byte> message,
        RpcSendOptions options,
        CancellationToken ct = default
    )
    {
        if (!_peers.TryGetValue(peer, out var st))
            throw new InvalidOperationException($"Peer {peer.Value} not connected.");

        await st.WriteLock.WaitAsync(ct);
        bool drop = false;
        try
        {
            var w = st.Writer;
            var span = w.GetSpan(4);
            BinaryPrimitives.WriteInt32LittleEndian(span, message.Length);
            w.Advance(4);
            w.Write(message.Span);
            await w.FlushAsync(ct);
        }
        catch
        {
            drop = true; // pipe corrupted
            throw;
        }
        finally
        {
            st.WriteLock.Release();
            if (drop)
                DropPeer(peer);
        }
    }

    public override ValueTask DisconnectAsync(PeerId peer, CancellationToken ct = default)
    {
        DropPeer(peer);
        return ValueTask.CompletedTask;
    }

    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        _shutdownCts.Cancel();
        _shutdownCts.Dispose();

        foreach (var peer in _peers.Keys.ToArray())
            DropPeer(peer);

        _socket.Dispose();

        if (Volatile.Read(ref _started) != 0)
            await _loopCompletion.Task;
    }

    private void DropPeer(PeerId peer)
    {
        if (_peers.TryRemove(peer, out var st))
        {
            st.Cts.Cancel();
            try
            {
                st.Reader.Complete();
            }
            catch { }
            try
            {
                st.Writer.Complete();
            }
            catch { }
            st.Cts.Dispose();
            OnPeerDisconnected(peer);
        }
    }

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            Socket client;
            try
            {
                client = await _socket.AcceptAsync(ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }
            catch (SocketException)
            {
                continue;
            }
            catch (IOException)
            {
                continue;
            }

            client.NoDelay = true;
            var peer = new PeerId(Interlocked.Increment(ref _nextPeerId));
            var stream = new NetworkStream(client, ownsSocket: true);
            var st = new PeerState(stream);
            _peers[peer] = st;
            OnPeerConnected(peer);
            _ = ReadLoopAsync(peer, st, st.Cts.Token);
        }
    }

    private enum FrameResult
    {
        None,
        Ok,
        Invalid,
    }

    private async Task ReadLoopAsync(PeerId peer, PeerState st, CancellationToken ct)
    {
        try
        {
            var reader = st.Reader;
            while (!ct.IsCancellationRequested)
            {
                var result = await reader.ReadAsync(ct);
                var buffer = result.Buffer;
                var ok = true;
                try
                {
                    while (true)
                    {
                        var fr = TryReadFrame(ref buffer, out var payload);
                        if (fr == FrameResult.Ok)
                            OnIncomingMessage(peer, payload, default);
                        else if (fr == FrameResult.Invalid)
                            ok = false;
                        else
                            break; // Need more data
                        if (!ok)
                            break;
                    }
                }
                finally
                {
                    if (ok)
                        reader.AdvanceTo(buffer.Start, buffer.End);
                }
                if (!ok || result.IsCompleted)
                    break;
            }
        }
        catch (OperationCanceledException) { }
        catch (IOException) { }
        catch (ObjectDisposedException) { }
        finally
        {
            DropPeer(peer);
        }
    }

    private static FrameResult TryReadFrame(ref ReadOnlySequence<byte> buf, out byte[]? payload)
    {
        payload = null;
        if (buf.Length < 4)
            return FrameResult.None;

        Span<byte> lenSpan = stackalloc byte[4];
        buf.Slice(0, 4).CopyTo(lenSpan);
        int len = BinaryPrimitives.ReadInt32LittleEndian(lenSpan);
        if (len < 0 || len > 64 * 1024 * 1024)
            return FrameResult.Invalid;
        if (buf.Length < 4 + len)
            return FrameResult.None;

        payload = buf.Slice(4, len).ToArray();
        buf = buf.Slice(4 + len);
        return FrameResult.Ok;
    }
}
