namespace GoodRpc;

/// <summary>
/// Extension methods on RpcTransport for creating proxies and dispatchers.
/// </summary>
public static class RpcTransportExtensions
{
    /// <summary>
    /// Create a proxy for calling methods on a specific peer.
    /// The proxy immediately subscribes to the transport and is ready to use.
    /// </summary>
    public static TInterface CreateProxy<TInterface>(this RpcTransport transport, PeerId peer)
        where TInterface : class => RpcRegistry.GetProxy<TInterface>(transport, peer);

    /// <summary>
    /// Create a dispatcher that routes incoming messages to the given handler.
    /// The dispatcher immediately subscribes to the transport and is ready to use.
    /// </summary>
    public static IDisposable CreateDispatcher<TInterface>(
        this RpcTransport transport,
        TInterface handler
    )
        where TInterface : class => RpcRegistry.CreateDispatcher(transport, handler);
}
