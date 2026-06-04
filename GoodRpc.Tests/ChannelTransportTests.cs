namespace GoodRpc.Tests;

public sealed class ChannelTransportTests
{
    private static async Task Teardown(ChannelTransport a, ChannelTransport b)
    {
        var t1 = a.DisposeAsync().AsTask();
        var t2 = b.DisposeAsync().AsTask();
        await Task.WhenAll(t1, t2);
    }

    [Fact]
    public async Task RoundTrip_RequestResponse_Works()
    {
        var (client, server) = ChannelTransport.CreatePair();
        try
        {
            var handler = new CalculatorHandler();
            var proxy = client.CreateProxy<ICalculator>(client.RemotePeer);
            var disp = server.CreateDispatcher<ICalculator>(handler);
            _ = Task.Run(() => client.RunAsync());
            _ = Task.Run(() => server.RunAsync());

            var result = await proxy.Add(3, 7);
            Assert.Equal(10, result);
        }
        finally
        {
            await Teardown(client, server);
        }
    }

    [Fact]
    public async Task RoundTrip_Notification_Works()
    {
        var handler = new CalculatorHandler();
        var (client, server) = ChannelTransport.CreatePair();
        try
        {
            var proxy = client.CreateProxy<ICalculator>(client.RemotePeer);
            var disp = server.CreateDispatcher<ICalculator>(handler);
            _ = Task.Run(() => client.RunAsync());
            _ = Task.Run(() => server.RunAsync());

            proxy.Notify("hello");
            await Task.Delay(100);
            Assert.Equal("hello", handler.LastNotification);
        }
        finally
        {
            await Teardown(client, server);
        }
    }

    [Fact]
    public async Task CreatePair_BothSides_CanStart()
    {
        var (c, s) = ChannelTransport.CreatePair();
        try
        {
            Assert.NotNull(c);
            Assert.NotNull(s);
            Assert.Equal(2L, c.RemotePeer.Value);
            Assert.Equal(1L, s.RemotePeer.Value);
        }
        finally
        {
            await Teardown(c, s);
        }
    }

    [Fact]
    public async Task RunAsync_FiresConnected_Then_DisconnectedOnDispose()
    {
        var connected = false;
        var disconnected = false;

        var (client, server) = ChannelTransport.CreatePair();
        try
        {
            client.Connected += _ => connected = true;
            client.Disconnected += _ => disconnected = true;

            var proxy = client.CreateProxy<ICalculator>(client.RemotePeer);
            var disp = server.CreateDispatcher<ICalculator>(new CalculatorHandler());

            var clientTask = client.RunAsync();
            await Task.Delay(100);

            Assert.True(connected);
            Assert.False(disconnected);

            await client.DisposeAsync();
            await clientTask;
            await Task.Delay(50);

            Assert.True(disconnected);
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    [Fact]
    public async Task DisposeAsync_BeforeRunAsync_DoesNotThrow()
    {
        var (client, server) = ChannelTransport.CreatePair();
        await Teardown(client, server);
    }

    [Fact]
    public async Task DoubleDispose_IsSafe()
    {
        var (client, server) = ChannelTransport.CreatePair();
        try
        {
            _ = Task.Run(() => client.RunAsync());
            await Task.Delay(50);

            await client.DisposeAsync();
            await client.DisposeAsync();
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    [Fact]
    public async Task RunAsync_Throws_IfCalledTwice()
    {
        var (client, server) = ChannelTransport.CreatePair();
        try
        {
            _ = Task.Run(() => client.RunAsync());
            await Task.Delay(50);

            await Assert.ThrowsAsync<InvalidOperationException>(() => client.RunAsync());
        }
        finally
        {
            await Teardown(client, server);
        }
    }

    [Fact]
    public async Task SendBeforeRunAsync_BuffersAndDelivers()
    {
        var handler = new CalculatorHandler();
        var (client, server) = ChannelTransport.CreatePair();
        try
        {
            var proxy = client.CreateProxy<ICalculator>(client.RemotePeer);
            var disp = server.CreateDispatcher<ICalculator>(handler);

            proxy.Notify("buffered");

            _ = Task.Run(() => server.RunAsync());
            await Task.Delay(200);
            Assert.Equal("buffered", handler.LastNotification);
        }
        finally
        {
            await Teardown(client, server);
        }
    }
}
