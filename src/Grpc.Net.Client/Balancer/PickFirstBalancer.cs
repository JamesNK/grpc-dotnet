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
using System.Threading;
using Grpc.Net.Client.Balancer.Internal;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Balancer
{
    public class PickFirstBalancer : LoadBalancer
    {
        private readonly ClientChannel _channel;
        internal SubChannel? _subChannel;
        private ILogger _logger;

        public PickFirstBalancer(ClientChannel channel, ILoggerFactory loggerFactory)
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

                _channel.UpdateState(new BalancerState(ConnectivityState.Idle, new PickFirstPicker(_subChannel)));
            }
            else
            {
                _channel.UpdateAddresses(_subChannel, state.ResolverState.Addresses);
            }

            _ = _subChannel.ConnectAsync(CancellationToken.None);
        }

        public override void UpdateSubChannelState(SubChannel subChannel, SubChannelState state)
        {
            _logger.LogInformation("Updating sub-channel state.");

            if (_subChannel != subChannel)
            {
                _logger.LogInformation("Ignored state change because of unknown sub-channel.");
                return;
            }

            switch (_channel.State)
            {
                case ConnectivityState.Ready:
                case ConnectivityState.Idle:
                    _channel.UpdateState(new BalancerState(_channel.State, new PickFirstPicker(_subChannel)));
                    break;
                case ConnectivityState.Connecting:
                    _channel.UpdateState(new BalancerState(ConnectivityState.Connecting, EmptyPicker.Instance));
                    break;
                case ConnectivityState.TransientFailure:
                    _channel.UpdateState(new BalancerState(ConnectivityState.TransientFailure, new FailurePicker(state.ConnectionError!)));
                    break;
                case ConnectivityState.Shutdown:
                    _subChannel = null;
                    break;
            }
        }

        private class PickFirstPicker : SubChannelPicker
        {
            private readonly SubChannel _subConnection;

            public PickFirstPicker(SubChannel subConnection)
            {
                _subConnection = subConnection;
            }

            public override PickResult Pick(PickContext context)
            {
                return new PickResult(_subConnection, c => { });
            }
        }
    }

}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif