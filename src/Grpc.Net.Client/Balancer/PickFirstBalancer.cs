#region Copyright notice and license

// Copyright 2019 The gRPC Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#endregion

#if HAVE_LOAD_BALANCING
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using Grpc.Net.Client.Balancer.Internal;
using Grpc.Net.Client.Configuration;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Balancer
{
    public class PickFirstBalancer : LoadBalancer
    {
        private readonly IChannelControlHelper _channel;
        internal SubChannel? _subChannel;
        private ILogger _logger;

        public PickFirstBalancer(IChannelControlHelper channel, ILoggerFactory loggerFactory)
        {
            _channel = channel;
            _logger = loggerFactory.CreateLogger<PickFirstBalancer>();
        }

        public override void Close()
        {
        }

        public override void ResolverError(Exception exception)
        {
            switch (_channel.State)
            {
                case ConnectivityState.Idle:
                case ConnectivityState.Connecting:
                case ConnectivityState.TransientFailure:
                    _channel.UpdateState(new BalancerState(ConnectivityState.TransientFailure, new FailurePicker(exception)));
                    break;
            }
        }

        public override void UpdateChannelState(ChannelState state)
        {
            if (state.ResolverState.Addresses.Count == 0)
            {
                ResolverError(new InvalidOperationException("Resolver returned no addresses."));
                return;
            }

            if (_subChannel == null)
            {
                try
                {
                    _subChannel = _channel.CreateSubChannel(new SubChannelOptions(state.ResolverState.Addresses));
                }
                catch (Exception ex)
                {
                    _channel.UpdateState(new BalancerState(ConnectivityState.TransientFailure, new FailurePicker(ex)));
                    throw;
                }

                _channel.UpdateState(new BalancerState(ConnectivityState.Idle, EmptyPicker.Instance));
            }
            else
            {
                _channel.UpdateAddresses(_subChannel, state.ResolverState.Addresses);
            }

            _ = _subChannel.ConnectAsync(CancellationToken.None);
        }

        public override void UpdateSubChannelState(SubChannel subChannel, SubChannelState state)
        {
            _logger.LogTrace("Updating sub-channel state.");

            if (_subChannel != subChannel)
            {
                _logger.LogTrace("Ignored state change because of unknown sub-channel.");
                return;
            }

            switch (state.ConnectivityState)
            {
                case ConnectivityState.Ready:
                    _channel.UpdateState(new BalancerState(state.ConnectivityState, new PickFirstPicker(_subChannel, _subChannel.CurrentEndPoint!)));
                    break;
                case ConnectivityState.Idle:
                    _channel.UpdateState(new BalancerState(state.ConnectivityState, EmptyPicker.Instance));
                    _ = _subChannel.ConnectAsync(CancellationToken.None);
                    break;
                case ConnectivityState.Connecting:
                    _channel.UpdateState(new BalancerState(state.ConnectivityState, EmptyPicker.Instance));
                    break;
                case ConnectivityState.TransientFailure:
                    _channel.UpdateState(new BalancerState(state.ConnectivityState, new FailurePicker(state.ConnectionError!)));
                    break;
                case ConnectivityState.Shutdown:
                    _subChannel = null;
                    break;
            }
        }

        private class PickFirstPicker : SubChannelPicker
        {
            private readonly SubChannel _subChannel;
            private readonly DnsEndPoint _endPoint;

            public PickFirstPicker(SubChannel subChannel, DnsEndPoint endPoint)
            {
                _subChannel = subChannel;
                _endPoint = endPoint;
            }

            public override PickResult Pick(PickContext context)
            {
                return new PickResult(_subChannel, _endPoint, c => { });
            }
        }
    }

    public class PickFirstBalancerFactory : LoadBalancerFactory
    {
        public override string Name { get; } = LoadBalancingConfig.PickFirstPolicyName;

        public override LoadBalancer Create(IChannelControlHelper channelControlHelper, ILoggerFactory loggerFactory, IDictionary<string, object> options)
        {
            return new PickFirstBalancer(channelControlHelper, loggerFactory);
        }
    }

}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif