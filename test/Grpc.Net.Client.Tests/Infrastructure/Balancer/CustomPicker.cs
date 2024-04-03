#if SUPPORT_LOAD_BALANCING
using Grpc.Net.Client.Balancer;

namespace Grpc.Tests.Shared;

internal class CustomPicker(IReadOnlyList<Subchannel> subchannels) : SubchannelPicker
{
    public override PickResult Pick(PickContext context)
    {
        return PickResult.ForSubchannel(subchannels[Random.Shared.Next(0, subchannels.Count)]);
    }
}
#endif
