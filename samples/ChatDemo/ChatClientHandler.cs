namespace ChatDemo;

internal sealed class ChatClientHandler : IChatClient
{
    public void OnChatMessage(string sender, string message)
    {
        Console.WriteLine($"  <{sender}> {message}");
    }

    public void OnUserJoined(string name)
    {
        Console.WriteLine($"  *** {name} joined the chat ***");
    }

    public void OnUserLeft(string name)
    {
        Console.WriteLine($"  *** {name} left the chat ***");
    }
}
