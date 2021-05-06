using System.Collections.Generic;
using Grpc.Net.Client.Balancer;
using Microsoft.Extensions.Logging;

namespace Client.Balancer
{
    public class ReportingLoadBalancerFactory : LoadBalancerFactory, ISubChannelReporter
    {
        private readonly LoadBalancerFactory _loadBalancerFactory;
        public ISubChannelReporter Reporter { get; private set; } = default!;

        public override string Name { get; } = "reporter";

        public List<SubChannel> SubChannels => Reporter?.SubChannels ?? new List<SubChannel>();
        public ConnectivityState State => Reporter?.State ?? ConnectivityState.Connecting;

        public ReportingLoadBalancerFactory(LoadBalancerFactory loadBalancerFactory)
        {
            _loadBalancerFactory = loadBalancerFactory;
        }

        public override LoadBalancer Create(IChannelControlHelper channelControlHelper, ILoggerFactory loggerFactory, IDictionary<string, object> options)
        {
            var reportingChannelControlHelper = new ReportingChannelControlHelper(channelControlHelper);
            Reporter = reportingChannelControlHelper;

            return _loadBalancerFactory.Create(reportingChannelControlHelper, loggerFactory, options);
        }
    }
}
