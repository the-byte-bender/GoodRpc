namespace GoodRpc;

/// <summary>
/// Global registry of proxy factories and dispatcher factories.
/// </summary>
/// <remarks>
/// The source generator emits module initializers that register generated factories here.
/// </remarks>
public static class RpcRegistry
{
    /// <summary>
    /// Register a proxy factory for an interface type.
    /// </summary>
    public static void RegisterProxy<TInterface>(Func<RpcTransport, PeerId, TInterface> factory)
        where TInterface : class
    {
        Slot<TInterface>.ProxyFactory = factory;
    }

    /// <summary>
    /// Register a dispatcher factory for an interface type.
    /// </summary>
    public static void RegisterDispatcher<TInterface>(
        Func<RpcTransport, TInterface, IDisposable> factory
    )
        where TInterface : class
    {
        Slot<TInterface>.DispatcherFactory = factory;
    }

    /// <summary>
    /// Create a proxy for calling methods on a specific peer.
    /// </summary>
    public static TInterface GetProxy<TInterface>(RpcTransport transport, PeerId peer)
        where TInterface : class
    {
        var factory = Slot<TInterface>.ProxyFactory;
        if (factory is null)
            throw new InvalidOperationException(
                $"No proxy registered for {typeof(TInterface).Name}. "
                    + $"Make sure the interface has [RpcService] and the source generator ran."
            );

        return factory(transport, peer);
    }

    /// <summary>
    /// Create a dispatcher that routes incoming messages to the given handler.
    /// </summary>
    public static IDisposable CreateDispatcher<TInterface>(
        RpcTransport transport,
        TInterface handler
    )
        where TInterface : class
    {
        var factory = Slot<TInterface>.DispatcherFactory;
        if (factory is null)
            throw new InvalidOperationException(
                $"No dispatcher registered for {typeof(TInterface).Name}. "
                    + $"Make sure the interface has [RpcService] and the source generator ran."
            );

        return factory(transport, handler);
    }

    private static class Slot<TInterface>
        where TInterface : class
    {
        public static Func<RpcTransport, PeerId, TInterface>? ProxyFactory;
        public static Func<RpcTransport, TInterface, IDisposable>? DispatcherFactory;
    }
}
