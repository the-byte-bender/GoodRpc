using GoodRpc;

namespace ChatDemo;

/// <summary>
/// RPC interface the server exposes to every client.
/// </summary>
[RpcService]
public interface IChatServer
{
    /// <summary>
    /// Register a display name. Returns false if the name is already taken.
    /// </summary>
    ValueTask<bool> Register(string name);

    /// <summary>
    /// Send a chat message. Broadcasts to all registered clients.
    /// </summary>
    ValueTask SendMessage(string message);
}

/// <summary>
/// RPC interface the server calls on each connected client.
/// </summary>
[RpcService]
public interface IChatClient
{
    /// <summary>
    /// a user sent a chat message.
    /// </summary>
    void OnChatMessage(string sender, string message);

    /// <summary>
    /// a user joined the chat.
    /// </summary>
    void OnUserJoined(string name);

    /// <summary>
    /// a user left the chat.
    /// </summary>
    void OnUserLeft(string name);
}
