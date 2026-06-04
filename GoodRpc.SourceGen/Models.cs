using System.Collections.Immutable;

namespace GoodRpc.SourceGen;

internal enum ReturnStyle
{
    Voids,
    Task,
    TaskOfT,
    ValueTask,
    ValueTaskOfT,
    Invalid = -1,
}

internal readonly record struct RpcMethodModel(
    string Name,
    ulong Hash,
    string? InnerTypeFQN,
    ReturnStyle ReturnStyle,
    ImmutableArray<string> ParamTypeFQNs,
    bool HasCancellationToken,
    byte Channel,
    int Delivery,
    int TimeoutMs,
    bool Compress,
    string ReturnTypeDisplay
);

internal readonly record struct RpcInterfaceModel(
    string FullyQualifiedName,
    string Name,
    ImmutableArray<RpcMethodModel> Methods
);
