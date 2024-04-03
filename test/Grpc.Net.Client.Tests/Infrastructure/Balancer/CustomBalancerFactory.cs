#if SUPPORT_LOAD_BALANCING
using Grpc.Net.Client.Balancer;

namespace Grpc.Tests.Shared;

public class CustomBalancerFactory : LoadBalancerFactory
{
    public override string Name => "test";

    public override LoadBalancer Create(LoadBalancerOptions options)
    {
        return new CustomBalancer(options.Controller, options.LoggerFactory);
    }
}
#endif
