using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client.Balancer;

namespace Client.Balancer
{
    public class ReportingChannelControlHelper : IChannelControlHelper, ISubChannelReporter
    {
        private readonly IChannelControlHelper _channelControlHelper;

        public ReportingChannelControlHelper(IChannelControlHelper channelControlHelper)
        {
            _channelControlHelper = channelControlHelper;
            SubChannels = new List<SubChannel>();
        }

        public ConnectivityState State => _channelControlHelper.State;

        public List<SubChannel> SubChannels { get; }

        public SubChannel CreateSubChannel(SubChannelOptions options)
        {
            var subChannel = _channelControlHelper.CreateSubChannel(options);
            SubChannels.Add(subChannel);

            return subChannel;
        }

        public void RemoveSubChannel(SubChannel subChannel)
        {
            _channelControlHelper.RemoveSubChannel(subChannel);
        }

        public Task ResolveNowAsync(CancellationToken cancellationToken)
        {
            return _channelControlHelper.ResolveNowAsync(cancellationToken);
        }

        public void UpdateAddresses(SubChannel subChannel, IReadOnlyList<DnsEndPoint> addresses)
        {
            _channelControlHelper.UpdateAddresses(subChannel, addresses);
        }

        public void UpdateState(BalancerState state)
        {
            _channelControlHelper.UpdateState(state);
        }
    }
}
