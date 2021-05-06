using System.Collections.Generic;
using Grpc.Net.Client.Balancer;

namespace Client.Balancer
{
    public interface ISubChannelReporter
    {
        ConnectivityState State { get; }
        List<SubChannel> SubChannels { get; }
    }
}
