#if SUPPORT_LOAD_BALANCING
using Grpc.Net.Client.Balancer;
using Microsoft.Extensions.Logging;

namespace Grpc.Tests.Shared;

public class CustomBalancer(
    IChannelControlHelper controller,
    ILoggerFactory loggerFactory)
    : SubchannelsLoadBalancer(controller, loggerFactory)
{
    protected override SubchannelPicker CreatePicker(IReadOnlyList<Subchannel> readySubchannels)
    {
        return new CustomPicker(readySubchannels);
    }
}
#endif
