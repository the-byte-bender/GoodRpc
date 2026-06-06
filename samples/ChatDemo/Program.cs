using GoodRpc;
using GoodRpc.Transports.LiteNetLib;
using LiteNetLib;

namespace ChatDemo;

public static class Program
{
    private const string ConnectionKey = "ChatDemo";
    private const int DefaultPort = 9050;

    public static async Task Main(string[] args)
    {
        if (args.Contains("--server"))
        {
            var port = ParsePort(args, DefaultPort);
            await RunServer(port);
        }
        else if (args.Contains("--client"))
        {
            var port = ParsePort(args, DefaultPort);
            await RunClient(port);
        }
        else
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  ChatDemo --server [port]    Run the chat server");
            Console.WriteLine("  ChatDemo --client [port]    Connect as a chat client");
            Console.WriteLine($"  Default port: {DefaultPort}");
        }
    }

    private static async Task RunServer(int port)
    {
        var transport = new LiteNetLibTransport();
        var handler = new ChatServerHandler(transport);
        using var dispatcher = transport.CreateDispatcher<IChatServer>(handler);
        dispatcher.HandlerError += (peer, hash, ex) =>
            Console.Error.WriteLine(
                $"[Server] Handler error in method {hash:X16} from peer {peer.Value}: {ex}"
            );

        var manager = new NetManager(transport);

        Console.WriteLine($"[Server] Listening on port {port}...");
        Console.WriteLine("[Server] Press Ctrl+C to stop.");
        Console.WriteLine();

        manager.Start(port);

        try
        {
            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            while (!cts.Token.IsCancellationRequested)
            {
                manager.PollEvents();
                await Task.Delay(1, cts.Token);
            }
        }
        catch (OperationCanceledException) { }

        Console.WriteLine("[Server] Shutting down...");
        manager.Stop();
        await transport.DisposeAsync();
    }

    private static async Task RunClient(int port)
    {
        var transport = new LiteNetLibTransport();
        var handler = new ChatClientHandler();
        using var dispatcher = transport.CreateDispatcher<IChatClient>(handler);

        var connected = new TaskCompletionSource<PeerId>();
        transport.Connected += peer => connected.TrySetResult(peer);

        var manager = new NetManager(transport);
        manager.Start();

        Console.WriteLine($"[Client] Connecting to 127.0.0.1:{port}...");
        manager.Connect("127.0.0.1", port, ConnectionKey);

        using var cts = new CancellationTokenSource();
        PeerId serverPeer;

        try
        {
            using var connectCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cts.Token,
                connectCts.Token
            );

            serverPeer = await WaitForConnection(manager, connected, linkedCts.Token);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("[Client] Connection timed out.");
            manager.Stop();
            return;
        }

        Console.WriteLine($"[Client] Connected! (peer {serverPeer.Value})");

        var server = transport.CreateProxy<IChatServer>(serverPeer);

        _ = Task.Run(
            async () =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    manager.PollEvents();
                    await Task.Delay(1, cts.Token);
                }
            },
            cts.Token
        );

        var disconnected = new TaskCompletionSource();
        transport.Disconnected += peer =>
        {
            if (peer.Equals(serverPeer))
                disconnected.TrySetResult();
        };

        while (true)
        {
            Console.Write("Choose a display name: ");
            var name = Console.ReadLine()?.Trim() ?? "";

            if (string.IsNullOrEmpty(name))
                continue;

            bool ok = await server.Register(name);
            if (ok)
            {
                Console.WriteLine($"[Client] You are '{name}'. Type /quit to leave.");
                Console.WriteLine();
                break;
            }

            Console.WriteLine($"[Client] Name '{name}' is taken. Try another.");
        }

        while (true)
        {
            var readTask = Task.Run(() => Console.ReadLine());
            var completed = await Task.WhenAny(readTask, disconnected.Task);

            if (completed == disconnected.Task)
                break;

            var line = readTask.Result;
            if (line is null)
                break;

            line = line.Trim();
            if (line.Equals("/quit", StringComparison.OrdinalIgnoreCase))
                break;

            if (line.Length == 0)
                continue;

            try
            {
                await server.SendMessage(line);
            }
            catch (RpcDisconnectedException)
            {
                break;
            }
        }

        if (disconnected.Task.IsCompleted)
            Console.WriteLine("[Client] Disconnected from server.");

        cts.Cancel();
        Console.WriteLine("[Client] Disconnecting...");
        await transport.DisconnectAsync(serverPeer);
        manager.Stop();
        await transport.DisposeAsync();
    }

    private static async Task<PeerId> WaitForConnection(
        NetManager manager,
        TaskCompletionSource<PeerId> connected,
        CancellationToken ct
    )
    {
        while (!ct.IsCancellationRequested)
        {
            manager.PollEvents();

            if (connected.Task.IsCompleted)
                return await connected.Task;

            await Task.Delay(1, ct);
        }

        throw new OperationCanceledException(ct);
    }

    private static int ParsePort(string[] args, int defaultPort)
    {
        for (int i = 0; i < args.Length; i++)
        {
            if (
                args[i] is "--server" or "--client"
                && i + 1 < args.Length
                && int.TryParse(args[i + 1], out var port)
                && port > 0
                && port <= 65535
            )
            {
                return port;
            }
        }
        return defaultPort;
    }
}
