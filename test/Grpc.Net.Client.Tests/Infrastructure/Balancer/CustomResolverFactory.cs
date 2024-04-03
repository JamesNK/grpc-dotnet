#if SUPPORT_LOAD_BALANCING
using Grpc.Net.Client.Balancer;

namespace Grpc.Tests.Shared;

public class CustomResolverFactory : ResolverFactory
{
    public override string Name => "test";

    public override Resolver Create(ResolverOptions options)
    {
        return new CustomResolver(options.LoggerFactory);
    }
}
#endif
