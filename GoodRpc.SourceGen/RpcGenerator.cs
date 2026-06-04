using System.Collections.Immutable;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace GoodRpc.SourceGen;

[Generator]
public sealed class RpcGenerator : IIncrementalGenerator
{
    private static readonly SymbolDisplayFormat FullyQualified =
        SymbolDisplayFormat.FullyQualifiedFormat;

    private static readonly DiagnosticDescriptor UnreliableNeedsVoid = new(
#pragma warning disable RS2008 // Enable analyzer release tracking
        id: "GOODRPC001",
#pragma warning restore RS2008 // Enable analyzer release tracking
        title: "Unreliable delivery requires void return",
        messageFormat: "Method '{0}' on interface '{1}' has delivery '{2}' but returns '{3}'. Only 'void' is allowed for unreliable delivery types because a response may never arrive.",
        category: "GoodRpc.Design",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true
    );

    private static readonly DiagnosticDescriptor InvalidReturnType = new(
#pragma warning disable RS2008 // Enable analyzer release tracking
        id: "GOODRPC002",
#pragma warning restore RS2008 // Enable analyzer release tracking
        title: "Invalid RPC return type",
        messageFormat: "Method '{0}' on interface '{1}' returns '{2}'. RPC methods must return 'void', 'Task', 'Task<T>', 'ValueTask', or 'ValueTask<T>'.",
        category: "GoodRpc.Design",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true
    );

    private static readonly DiagnosticDescriptor VoidWithCancellationToken = new(
#pragma warning disable RS2008 // Enable analyzer release tracking
        id: "GOODRPC003",
#pragma warning restore RS2008 // Enable analyzer release tracking
        title: "CancellationToken not supported on void methods",
        messageFormat: "Method '{0}' on interface '{1}' returns 'void' and has a CancellationToken parameter. CancellationToken is only supported on 'Task' and 'Task<T>' methods.",
        category: "GoodRpc.Design",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true
    );

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var rpcInterfaces = context
            .SyntaxProvider.ForAttributeWithMetadataName(
                "GoodRpc.RpcServiceAttribute",
                predicate: static (node, _) => node is InterfaceDeclarationSyntax,
                transform: static (ctx, ct) => BuildModel(ctx, ct)
            )
            .Where(static m => m is not null)
            .Select(static (m, _) => m!.Value)
            .WithTrackingName("FindRpcInterfaces");

        context.RegisterSourceOutput(rpcInterfaces, EmitSource);
    }

    private static RpcInterfaceModel? BuildModel(
        GeneratorAttributeSyntaxContext ctx,
        CancellationToken ct
    )
    {
        var iface = (INamedTypeSymbol)ctx.TargetSymbol;
        var methods = ImmutableArray.CreateBuilder<RpcMethodModel>();

        foreach (var member in iface.GetMembers())
        {
            if (member is not IMethodSymbol m)
                continue;
            ct.ThrowIfCancellationRequested();

            var (style, innerFqn) = ClassifyReturn(m.ReturnType);

            var attr = m.GetAttributes()
                .FirstOrDefault(a => a.AttributeClass?.Name == "RpcMethodAttribute");

            var hasCt = HasCancellationTokenLastParam(m);

            methods.Add(
                new RpcMethodModel(
                    Name: m.Name,
                    Hash: HashMethod(m),
                    InnerTypeFQN: innerFqn,
                    ReturnStyle: style,
                    ParamTypeFQNs: m.Parameters.Select(p => p.Type.ToDisplayString(FullyQualified))
                        .ToImmutableArray(),
                    HasCancellationToken: hasCt,
                    Channel: GetAttrArg(attr, "Channel", (byte)0),
                    Delivery: GetAttrArg(attr, "Delivery", 0),
                    TimeoutMs: GetAttrArg(attr, "TimeoutMs", 0),
                    Compress: GetAttrArg(attr, "Compress", false),
                    ReturnTypeDisplay: m.ReturnType.ToDisplayString(FullyQualified)
                )
            );
        }

        if (methods.Count == 0)
            return null;

        return new RpcInterfaceModel(
            FullyQualifiedName: iface.ToDisplayString(FullyQualified),
            Name: iface.Name,
            Methods: methods.ToImmutable()
        );
    }

    private static (ReturnStyle Style, string? InnerFqn) ClassifyReturn(ITypeSymbol type)
    {
        if (type.SpecialType == SpecialType.System_Void)
            return (ReturnStyle.Voids, null);

        if (type is not INamedTypeSymbol named)
            return (ReturnStyle.Invalid, null);

        var rawName = named.ConstructedFrom.ToDisplayString();

        if (rawName == "System.Threading.Tasks.Task")
            return (ReturnStyle.Task, null);

        if (rawName == "System.Threading.Tasks.Task<TResult>")
            return (ReturnStyle.TaskOfT, named.TypeArguments[0].ToDisplayString(FullyQualified));

        if (rawName == "System.Threading.Tasks.ValueTask")
            return (ReturnStyle.ValueTask, null);

        if (rawName == "System.Threading.Tasks.ValueTask<TResult>")
            return (
                ReturnStyle.ValueTaskOfT,
                named.TypeArguments[0].ToDisplayString(FullyQualified)
            );

        return (ReturnStyle.Invalid, null);
    }

    private static ulong HashMethod(IMethodSymbol m)
    {
        var desc =
            $"{m.ContainingType.ToDisplayString(FullyQualified)}.{m.Name}"
            + $"({string.Join(",", m.Parameters.Select(p => p.Type.ToDisplayString(FullyQualified)))}):"
            + $"{m.ReturnType.ToDisplayString(FullyQualified)}";

        return Fnv1a64(Encoding.UTF8.GetBytes(desc));
    }

    private static ulong Fnv1a64(byte[] data)
    {
        const ulong offset = 14695981039346656037;
        const ulong prime = 1099511628211;
        ulong hash = offset;
        foreach (var b in data)
        {
            hash ^= b;
            hash *= prime;
        }
        return hash;
    }

    private static T GetAttrArg<T>(AttributeData? attr, string key, T @default)
        where T : struct
    {
        if (attr is null)
            return @default;
        foreach (var kv in attr.NamedArguments)
        {
            if (kv.Key == key && !kv.Value.IsNull)
                return (T)Convert.ChangeType(kv.Value.Value!, typeof(T));
        }
        return @default;
    }

    private static bool HasCancellationTokenLastParam(IMethodSymbol m)
    {
        if (m.Parameters.Length == 0)
            return false;
        var last = m.Parameters[m.Parameters.Length - 1];
        return last.Type.ToDisplayString(FullyQualified)
            == "global::System.Threading.CancellationToken";
    }

    private void EmitSource(SourceProductionContext ctx, RpcInterfaceModel model)
    {
        var valid = ImmutableArray.CreateBuilder<RpcMethodModel>();

        foreach (var m in model.Methods)
        {
            if (m.ReturnStyle == ReturnStyle.Invalid)
            {
                ctx.ReportDiagnostic(
                    Diagnostic.Create(
                        InvalidReturnType,
                        Location.None,
                        m.Name,
                        model.Name,
                        m.ReturnTypeDisplay
                    )
                );
                continue;
            }

            if (m.ReturnStyle != ReturnStyle.Voids && (m.Delivery == 2 || m.Delivery == 3)) // Unreliable=2, UnreliableOrdered=3
            {
                ctx.ReportDiagnostic(
                    Diagnostic.Create(
                        UnreliableNeedsVoid,
                        Location.None,
                        m.Name,
                        model.Name,
                        m.Delivery == 2 ? "Unreliable" : "UnreliableOrdered",
                        m.ReturnTypeDisplay
                    )
                );
                continue;
            }

            if (m.HasCancellationToken && m.ReturnStyle == ReturnStyle.Voids)
            {
                ctx.ReportDiagnostic(
                    Diagnostic.Create(VoidWithCancellationToken, Location.None, m.Name, model.Name)
                );
                continue;
            }

            valid.Add(m);
        }

        if (valid.Count == 0)
            return;

        var cleanModel = model with { Methods = valid.ToImmutable() };
        var source = Emitter.Generate(cleanModel);
        ctx.AddSource($"{model.Name}.g.cs", source);
    }
}
