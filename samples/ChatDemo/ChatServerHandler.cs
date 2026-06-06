using GoodRpc;

namespace ChatDemo;

internal sealed class ChatServerHandler : IChatServer
{
    private readonly RpcTransport _transport;
    private readonly Dictionary<PeerId, (string Name, IChatClient Client)> _users = new();

    public ChatServerHandler(RpcTransport transport)
    {
        _transport = transport;
        transport.Connected += OnPeerConnected;
        transport.Disconnected += OnPeerDisconnected;
    }

    private void OnPeerConnected(PeerId peer)
    {
        _users[peer] = (Name: "", Client: _transport.CreateProxy<IChatClient>(peer));
    }

    private void OnPeerDisconnected(PeerId peer)
    {
        if (_users.Remove(peer, out var user) && user.Name is { Length: > 0 } name)
        {
            foreach (var (other, u) in _users)
                if (u.Name.Length > 0)
                    u.Client.OnUserLeft(name);
        }
    }

    // ── IChatServer ────────────────────────────────────────────

    public ValueTask<bool> Register(string name)
    {
        var peer = RpcContext.CurrentPeer;

        if (
            string.IsNullOrWhiteSpace(name)
            || _users.Values.Any(u => u.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
        )
            return new(false);

        var client = _users[peer].Client;
        _users[peer] = (name, client);

        Console.WriteLine($"[Server] '{name}' registered (peer {peer.Value})");

        foreach (var (other, u) in _users)
            if (!other.Equals(peer) && u.Name.Length > 0)
                u.Client.OnUserJoined(name);

        return new(true);
    }

    public ValueTask SendMessage(string message)
    {
        var peer = RpcContext.CurrentPeer;

        if (!_users.TryGetValue(peer, out var user) || user.Name is not { Length: > 0 } sender)
            return ValueTask.CompletedTask;

        Console.WriteLine($"[Chat] <{sender}> {message}");

        foreach (var (_, u) in _users)
            if (u.Name.Length > 0)
                u.Client.OnChatMessage(sender, message);

        return ValueTask.CompletedTask;
    }
}
