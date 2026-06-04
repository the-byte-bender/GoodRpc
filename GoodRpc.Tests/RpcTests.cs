namespace GoodRpc.Tests;

public sealed class RpcTests
{
    private static (
        TInterface Proxy,
        IDisposable Dispatcher,
        LoopbackTransport ClientTx,
        LoopbackTransport ServerTx
    ) Setup<TInterface>(TInterface handler)
        where TInterface : class
    {
        var (client, server) = LoopbackTransport.CreatePair();
        var proxy = client.CreateProxy<TInterface>(client.RemotePeer);
        var dispatcher = server.CreateDispatcher(handler);
        client.Connect();
        server.Connect();
        return (proxy, dispatcher, client, server);
    }

    [Fact]
    public async Task Task_ZeroParams_HandlerCalled()
    {
        var handler = new CalculatorHandler();
        var (proxy, disp, _, _) = Setup<ICalculator>(handler);
        await proxy.Ping();
        Assert.True(handler.PingCalled);
    }

    [Fact]
    public async Task TaskOfT_Add_ReturnsSum()
    {
        var (proxy, disp, _, _) = Setup<ICalculator>(new CalculatorHandler());
        Assert.Equal(10, await proxy.Add(3, 7));
    }

    [Fact]
    public async Task TaskOfT_DtoParam_ReturnsDto()
    {
        var (proxy, disp, _, _) = Setup<ICalculator>(new CalculatorHandler());
        Assert.Equal(13, (await proxy.AddDto(new AddRequest(5, 8))).Sum);
    }

    [Fact]
    public async Task TaskOfT_StringResult_Works()
    {
        var (proxy, disp, _, _) = Setup<IPingService>(new PingHandler());
        Assert.Equal("pong world", await proxy.Ping("world"));
    }

    [Fact]
    public async Task TaskOfT_TwoParams_Tuple_Works()
    {
        var (proxy, disp, _, _) = Setup<IGreeter>(new GreeterHandler());
        var r = await proxy.Greet("Ahmed", 3);
        Assert.Equal("Ahmed", r.Name);
        Assert.Equal(3, r.Count);
    }

    [Fact]
    public async Task Void_OneParam_HandlerReceivesValue()
    {
        var handler = new CalculatorHandler();
        var (proxy, disp, _, _) = Setup<ICalculator>(handler);
        proxy.Notify("hello");
        Assert.Equal("hello", handler.LastNotification);
    }

    [Fact]
    public async Task Void_ZeroParams_HandlerCalled()
    {
        var handler = new GreeterHandler();
        var (proxy, disp, _, _) = Setup<IGreeter>(handler);
        proxy.FireAndForget();
        Assert.True(handler.FireAndForgetCalled);
    }

    [Fact]
    public async Task MultipleSequentialCalls_AllComplete()
    {
        var (proxy, disp, _, _) = Setup<ICalculator>(new CalculatorHandler());
        Assert.Equal(3, await proxy.Add(1, 2));
        Assert.Equal(30, await proxy.Add(10, 20));
    }

    [Fact]
    public async Task TwoDispatchers_SameTransport_Work()
    {
        var (client, server) = LoopbackTransport.CreatePair();
        var calcDisp = server.CreateDispatcher<ICalculator>(new CalculatorHandler());
        var pingDisp = server.CreateDispatcher<IPingService>(new PingHandler());
        var calc = client.CreateProxy<ICalculator>(client.RemotePeer);
        var ping = client.CreateProxy<IPingService>(client.RemotePeer);
        client.Connect();
        server.Connect();
        Assert.Equal(12, await calc.Add(5, 7));
        Assert.Equal("pong test", await ping.Ping("test"));
    }

    [Fact]
    public async Task DoubleDispose_Proxy_IsSafe()
    {
        var (proxy, disp, _, _) = Setup<ICalculator>(new CalculatorHandler());
        await proxy.Ping();
        (proxy as IDisposable)?.Dispose();
        (proxy as IDisposable)?.Dispose();
    }

    [Fact]
    public async Task RpcContext_HandlerSeesCurrentPeer()
    {
        var handler = new CalculatorHandler();
        var (proxy, disp, client, _) = Setup<ICalculator>(handler);
        await proxy.Ping();
        Assert.NotNull(handler.LastCallerPeer);
        Assert.Equal(client.RemotePeer, handler.LastCallerPeer!.Value);
    }

    [Fact]
    public async Task CancellationToken_Normal_Completes()
    {
        var handler = new CancellationHandler();
        var (proxy, disp, _, _) = Setup<ICancellationService>(handler);
        var result = await proxy.SlowOp(1, CancellationToken.None);
        Assert.Equal("done", result);
    }

    [Fact]
    public async Task CancellationToken_Canceled_Throws()
    {
        var handler = new CancellationHandler();
        var (proxy, disp, _, _) = Setup<ICancellationService>(handler);

        using var cts = new CancellationTokenSource();
        var call = proxy.SlowOp(2000, cts.Token);
        cts.Cancel();

        var ex = await Record.ExceptionAsync(() => call);
        Assert.IsAssignableFrom<OperationCanceledException>(ex);
    }

    [Fact]
    public async Task CancellationToken_Timeout_Throws()
    {
        var handler = new CancellationHandler();
        var (proxy, disp, _, _) = Setup<ICancellationService>(handler);

        using var cts = new CancellationTokenSource(100);
        var ex = await Record.ExceptionAsync(() => proxy.SlowOp(2000, cts.Token));
        Assert.IsAssignableFrom<OperationCanceledException>(ex);
    }

    [Fact]
    public async Task CancellationTokenOnly_Normal_Completes()
    {
        var handler = new CtOnlyHandler();
        var (proxy, disp, _, _) = Setup<ICtOnly>(handler);
        var result = await proxy.Ping(CancellationToken.None);
        Assert.Equal("pong", result);
    }

    [Fact]
    public async Task RpcContextAndHandlerCt_AreSameToken()
    {
        var handler = new CancellationHandler();
        var (proxy, disp, _, _) = Setup<ICancellationService>(handler);
        await proxy.SlowOp(1, CancellationToken.None);
        Assert.NotNull(handler.HandlerCt);
        Assert.True(
            handler.SameAsRpcContext,
            "Handler's CancellationToken param should match RpcContext.CurrentCancellation"
        );
    }

    [Fact]
    public async Task Echo_WithCt_Normal_ReturnsValue()
    {
        var handler = new EchoHandler();
        var (proxy, disp, _, _) = Setup<IEchoService>(handler);
        var result = await proxy.Echo(42, 10, CancellationToken.None);
        Assert.Equal(42, result);
    }

    [Fact]
    public async Task Echo_WithCt_Concurrent_BothSucceed()
    {
        var handler = new EchoHandler();
        var (proxy, disp, _, _) = Setup<IEchoService>(handler);

        var t1 = proxy.Echo(1, 400, CancellationToken.None);
        var t2 = proxy.Echo(2, 400, CancellationToken.None);

        var results = await Task.WhenAll(t1, t2);
        Assert.Equal(1, results[0]);
        Assert.Equal(2, results[1]);
    }

    [Fact]
    public async Task Echo_WithCt_CancelOne_OtherCompletes()
    {
        var handler = new EchoHandler();
        var (proxy, disp, _, _) = Setup<IEchoService>(handler);

        using var cts1 = new CancellationTokenSource(100);
        using var cts2 = new CancellationTokenSource();

        var t1 = Record.ExceptionAsync(() => proxy.Echo(1, 2000, cts1.Token));
        var t2 = proxy.Echo(2, 100, cts2.Token);

        var result = await t2;
        Assert.Equal(2, result);

        var ex1 = await t1;
        Assert.IsAssignableFrom<OperationCanceledException>(ex1);
    }

    [Fact]
    public async Task Echo_WithCt_DisconnectPeer_CancelsAll()
    {
        var handler = new EchoHandler();
        var (client, server) = LoopbackTransport.CreatePair();
        using var disp = server.CreateDispatcher<IEchoService>(handler);
        var proxy = client.CreateProxy<IEchoService>(client.RemotePeer);
        client.Connect();
        server.Connect();

        var t1 = proxy.Echo(1, 2000, CancellationToken.None);
        var t2 = proxy.Echo(2, 2000, CancellationToken.None);

        await Task.Delay(50);

        await server.DisconnectAsync(client.RemotePeer);
        await client.DisconnectAsync(client.RemotePeer);

        var ex1 = await Record.ExceptionAsync(() => t1);
        var ex2 = await Record.ExceptionAsync(() => t2);

        Assert.IsAssignableFrom<RpcDisconnectedException>(ex1);
        Assert.IsAssignableFrom<RpcDisconnectedException>(ex2);
    }

    [Fact]
    public async Task Echo_NoCt_DisconnectPeer_CancelsAll()
    {
        var handler = new EchoNoCtHandler();
        var (client, server) = LoopbackTransport.CreatePair();
        using var disp = server.CreateDispatcher<IEchoNoCt>(handler);
        var proxy = client.CreateProxy<IEchoNoCt>(client.RemotePeer);
        client.Connect();
        server.Connect();

        var t1 = proxy.Echo(1, 2000);
        var t2 = proxy.Echo(2, 2000);

        await Task.Delay(50);

        await server.DisconnectAsync(client.RemotePeer);
        await client.DisconnectAsync(client.RemotePeer);

        var ex1 = await Record.ExceptionAsync(() => t1);
        var ex2 = await Record.ExceptionAsync(() => t2);

        Assert.IsAssignableFrom<RpcDisconnectedException>(ex1);
        Assert.IsAssignableFrom<RpcDisconnectedException>(ex2);
    }

    [Fact]
    public async Task Echo_WithCt_CancelServer_SeesCancellation()
    {
        var handler = new EchoHandler();
        var (proxy, disp, _, _) = Setup<IEchoService>(handler);

        using var cts = new CancellationTokenSource(100);
        var ex = await Record.ExceptionAsync(() => proxy.Echo(1, 2000, cts.Token));

        Assert.IsAssignableFrom<OperationCanceledException>(ex);
        Assert.True(
            handler.WasCanceled,
            "Server handler must observe cancellation when client cancels via CT"
        );
    }

    [Fact]
    public async Task Echo_NoCt_DisconnectServer_SeesCancellation()
    {
        var handler = new EchoNoCtHandler();
        var (client, server) = LoopbackTransport.CreatePair();
        using var disp = server.CreateDispatcher<IEchoNoCt>(handler);
        var proxy = client.CreateProxy<IEchoNoCt>(client.RemotePeer);
        client.Connect();
        server.Connect();

        var call = proxy.Echo(1, 2000);
        await Task.Delay(50);

        await server.DisconnectAsync(client.RemotePeer);
        await client.DisconnectAsync(client.RemotePeer);

        var ex = await Record.ExceptionAsync(() => call);
        Assert.IsAssignableFrom<RpcDisconnectedException>(ex);
        Assert.True(
            handler.RpcContextWasCanceled,
            "Server handler must observe cancellation via RpcContext when peer disconnects"
        );
    }
}
