# GoodRpc

Simple, pluggable, AOT-friendly RPC for .NET.

Please note that we're still in pre-stable 0.x versions, so bugs might exist, performance might not be at its absolute best yet, and expect breaking changes and some missing features or documentation. Feedback and contributions are very welcome!

## Features

- Source-generated proxies and dispatchers for your interfaces with zero reflection or dynamic invocation.
- Pluggable transports. Your rpc works the same way whether it's in-process, over TCP, or something else entirely. You can build your own transport, too!
- Abstractions that allow the same code to work and take advantage of any topology: client-server, server-clients, peer-to-peer, peer-to-peer mesh, etc. Note that the transport must support the topology you want to use, the abstractions just allow your handlers and callers to be agnostic.
- Supports return types of void, ValueTask, Task, ValueTask<T>, and Task<T>, with any amount of parameters. (IAsyncEnumerable<T> return type coming soon!)
- Cancellation token support that passes through the wire to the handler for canceling calls in-flight, automatically detected and synchronized if the last parameter of the method is a CancellationToken.
- Metadata attributes that let you configure timeouts, delivery guarantees, and more on a per-method basis that fall back to safe defaults if the transport doesn't support them.
- Ambient context that flows with the call that provide the callers peer id and other information so your handler can safely know who called it.
- Super fast serialization provided by [MemoryPack](https://github.com/Cysharp/MemoryPack) (primitives, most collections, value tuples, and built-in types are supported out of the box, and you can add support for your own types by using the [MemoryPackable] attribute on their definitions).

## Getting Started

`dotnet add package GoodRpc`

```csharp
using GoodRpc;
```

GoodRpc uses a few terms that are important to understand:

- Transport: The underlying communication mechanism that sends bytes between peers. Examples include in-process channels, TCP, WebSockets, etc. The transport is responsible for providing peer identification and a way to send messages to specific peers.
- Peer: An endpoint associated with a remote transport that can send and receive RPC calls. A peer can be a client, a server, or both at the same time (in a peer-to-peer topology). A peer is represented by a `PeerId` struct that uniquely identifies it within a transport.
- Proxy: An automatically generated implementation of your RPC interface that you use to make calls. The proxy takes care of serializing the call, sending it over the transport, and deserializing the response when it comes, and it synchronizes errors and cancellation when applicable. A proxy is always associated with a single peer that it sends calls to.
- Dispatcher: The counterpart to the proxy that you use on the receiving end. You give the dispatcher an implementation of your RPC interface, and it takes care of deserializing incoming calls, invoking your handler, and serializing the response back to the caller. A Dispatcher is associated with a Transport, and it handles calls from any peer that can send messages to that transport. Because calls can come from any connected peer, your implementation can access the caller's peer id and other information via the ambient RpcContext for identification, authorization, logging, etc.

Don't worry if this sounds complicated, the examples below will show how it all fits together.

### Define your RPC interface

```csharp
[RpcService]
public interface IMyRpc
{
    ValueTask<int> Add(int a, int b);
    ValueTask<int> Subtract(int a, int b);
    ValueTask<PeerId> GetMyPeerId();
}
```

### Implement the interface

```csharp
public class MyRpcHandler : IMyRpc
{
    public ValueTask<int> Add(int a, int b) => new(a + b);
    public ValueTask<int> Subtract(int a, int b) => new(a - b);
    public ValueTask<PeerId> GetMyPeerId() => new(RpcContext.CurrentPeer);
    // Note how we can access the caller's peer id from the ambient RpcContext, which the dispatcher populates for us before invoking this.
    // This is how you can implement caller identification, authorization, logging, and other concerns that depend on the caller's identity if you have multiple connected peers,
    // like in a game server where you might want to know what user / player this is, or a p2p mesh where you want to know which peer is calling.
}
```

### Set up the transport.

This is the part that is different and specific per transport. Individual transports should document how to set them up and start them.
The stable part is that you subscribe to the `Connected` event of the transport right after you construct it, before calling methods that configure and start it up, and also register any handlers through the `CreateDispatcher` method of the transport, passing it an implementation of your RPC interface.

In here we will set up a simple in-process client and server situation using the built-in `ChannelTransport`

```csharp
var pair = ChannelTransport.CreatePair();
await using var a = pair.A;
await using var b = pair.B;
// This gives us 2 connected transports. They're fully duplex, so either one can be the client or server, or both can have a dispatcher and mirroring proxies for two-way rpc!
// For this example we'll stay one-way, and make a the client and b the server.

// This is the global setup that you do for any transport.
// Tell the server to handle rpc requests for the IMyRpc interface using our MyRpcHandler implementation.
b.CreateDispatcher<IMyRpc>(new MyRpcHandler());
// .CreateDispatcher also returns a disposable that you can optionally keep a reference to, in case you want to unregister the handler earlier than the transport's lifetime. In most cases we don't want that.

// This is the per-peer setup that you do for each peer you want to call rpc methods on. We subscribe even for the client, because we'll get a peer representing the connection to the server.
a.Connected += async (peer) =>
{
    // Create a proxy for this peer, so we can call rpc methods on it.
    var proxy = a.CreateProxy<IMyRpc>(peer);
    // Now we can call rpc methods on the proxy as if it was a local implementation of IMyRpc, and the transport and dispatcher at the other end will take care of the rest.
    var sum = await proxy.Add(5, 7);
    Console.WriteLine($"5 + 7 = {sum}");
    var diff = await proxy.Subtract(10, 3);
    Console.WriteLine($"10 - 3 = {diff}");
    var peerId = await proxy.GetMyPeerId(); // This specific connection's peer id will be returned as the server sees it!
    Console.WriteLine($"My peer id on the server is {peerId}");
    // We're done, disconnect from server.
    await a.DisconnectAsync(peer);
}; // There is also a Disconnected event you can subscribe to for cleanup.
// If the client had its own methods for the server to call, it can have another rpc marked interface that it implements (pretty common for games). Then we would also subscribe to .Connected on the server and create a proxy for each incoming connection in there, just like we did for the client, and the client would create a dispatcher for its own handler implementation, just like we did for the server. Then both ends can call each other freely.

// After setting up our handler and event connections, we start the transport. This is specific to the transport you're using because some might have different methods to connect as a client or server, or might have methods that require configuration, etc. Check the documentation for the transport you're using for details.
// For the ChannelTransport, we just need to start both ends. with RunAsync, which will start listening to the channel and processing the rpc calls. RunAsync returns a task that completes when the transport shuts down, so we can await it to keep the program running until then.
_ = b.RunAsync();
_ = a.RunAsync();

Console.WriteLine("Press any key to exit...");
Console.ReadKey();
// In a proper program you would have a more robust lifetime management and shutdown procedure, but this is just an example.
```

Note how only the transport construction and startup is specific to the transport you use; everything else, from your interface and handler definition down to the Connected event and proxy/dispatcher wiring, is identical across any transport.

For more details on all features, check the XML docs on the public API.

## License

GoodRpc is licensed under the Mozilla Public License 2.0. See [LICENSE](LICENSE) for details.
