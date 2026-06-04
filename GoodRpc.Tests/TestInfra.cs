using MemoryPack;

namespace GoodRpc.Tests;

[MemoryPackable]
public readonly partial record struct EmptyDto;

[MemoryPackable]
public readonly partial record struct AddRequest(int A, int B);

[MemoryPackable]
public readonly partial record struct AddResult(int Sum);

[MemoryPackable]
public readonly partial record struct Greeting(string Name, int Count, string Message);

[RpcService]
public interface ICalculator
{
    void Notify(string msg);
    ValueTask Ping();
    ValueTask<int> Add(int a, int b);
    ValueTask<AddResult> AddDto(AddRequest req);
    ValueTask EchoVoid();
}

[RpcService]
public interface IPingService
{
    ValueTask<string> Ping(string name);
    void Shout(string msg);
}

[RpcService]
public interface IGreeter
{
    ValueTask<Greeting> Greet(string name, int count);
    void FireAndForget();
}

public sealed class CalculatorHandler : ICalculator
{
    public string? LastNotification { get; private set; }
    public bool PingCalled { get; private set; }
    public PeerId? LastCallerPeer { get; private set; }

    public void Notify(string msg) => LastNotification = msg;

    public ValueTask Ping()
    {
        PingCalled = true;
        LastCallerPeer = RpcContext.CurrentPeer;
        return ValueTask.CompletedTask;
    }

    public ValueTask<int> Add(int a, int b) => new(a + b);

    public ValueTask<AddResult> AddDto(AddRequest req) => new(new AddResult(req.A + req.B));

    public ValueTask EchoVoid() => default;
}

public sealed class PingHandler : IPingService
{
    public string? LastShout { get; private set; }

    public ValueTask<string> Ping(string name) => new($"pong {name}");

    public void Shout(string msg) => LastShout = msg;
}

public sealed class GreeterHandler : IGreeter
{
    public bool FireAndForgetCalled { get; private set; }

    public ValueTask<Greeting> Greet(string name, int count) =>
        new(new Greeting(name, count, $"Hello {name} x{count}!"));

    public void FireAndForget() => FireAndForgetCalled = true;
}

[RpcService]
public interface ICancellationService
{
    Task<string> SlowOp(int msDelay, CancellationToken ct);
}

public sealed class CancellationHandler : ICancellationService
{
    public CancellationToken? HandlerCt { get; private set; }
    public bool SameAsRpcContext { get; private set; }

    public async Task<string> SlowOp(int msDelay, CancellationToken ct)
    {
        HandlerCt = ct;
        SameAsRpcContext = ct == RpcContext.CurrentCancellation;
        await Task.Delay(msDelay, ct);
        return "done";
    }
}

[RpcService]
public interface ICtOnly
{
    ValueTask<string> Ping(CancellationToken ct);
}

public sealed class CtOnlyHandler : ICtOnly
{
    public CancellationToken? HandlerCt { get; private set; }

    public ValueTask<string> Ping(CancellationToken ct)
    {
        HandlerCt = ct;
        return new("pong");
    }
}

[RpcService]
public interface IEchoService
{
    Task<int> Echo(int value, int msDelay, CancellationToken ct);
}

public sealed class EchoHandler : IEchoService
{
    public int CallCount;
    public bool WasCanceled;

    public async Task<int> Echo(int value, int msDelay, CancellationToken ct)
    {
        Interlocked.Increment(ref CallCount);
        try
        {
            await Task.Delay(msDelay, ct);
        }
        catch (OperationCanceledException)
        {
            WasCanceled = true;
            throw;
        }
        return value;
    }
}

[RpcService]
public interface IEchoNoCt
{
    Task<int> Echo(int value, int msDelay);
}

public sealed class EchoNoCtHandler : IEchoNoCt
{
    public int CallCount;
    public bool RpcContextWasCanceled;

    public async Task<int> Echo(int value, int msDelay)
    {
        Interlocked.Increment(ref CallCount);
        try
        {
            await Task.Delay(msDelay, RpcContext.CurrentCancellation);
        }
        catch (OperationCanceledException)
        {
            RpcContextWasCanceled = true;
            throw;
        }
        return value;
    }
}

public sealed class LoopbackTransport : RpcTransport
{
    private LoopbackTransport? _twin;

    public PeerId RemotePeer { get; }

    private LoopbackTransport(PeerId remotePeer)
    {
        RemotePeer = remotePeer;
    }

    public void Connect() => OnPeerConnected(RemotePeer);

    public override ValueTask SendAsync(
        PeerId peer,
        ReadOnlyMemory<byte> message,
        RpcSendOptions options,
        CancellationToken ct = default
    )
    {
        var copy = message.ToArray();
        var ctx = new RpcReceiveContext(options.Channel, options.Delivery);
        _twin!.OnIncomingMessage(peer, copy, ctx);
        return ValueTask.CompletedTask;
    }

    public override ValueTask DisconnectAsync(PeerId peer, CancellationToken ct = default)
    {
        OnPeerDisconnected(peer);
        return ValueTask.CompletedTask;
    }

    public override ValueTask DisposeAsync()
    {
        OnPeerDisconnected(RemotePeer);
        return ValueTask.CompletedTask;
    }

    public static (LoopbackTransport A, LoopbackTransport B) CreatePair()
    {
        var a = new LoopbackTransport(new PeerId(2));
        var b = new LoopbackTransport(new PeerId(1));
        a._twin = b;
        b._twin = a;
        return (a, b);
    }
}
