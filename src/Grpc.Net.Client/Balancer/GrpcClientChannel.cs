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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

#if HAVE_LOAD_BALANCING
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Grpc.Net.Client.Balancer
{
    public class GrpcClientChannel : ClientChannel, IDisposable
    {
        private readonly AddressResolver _resolver;
        private readonly ILoggerFactory _loggerFactory;
        internal LoadBalancer? _balancer;
        private IDisposable? _resolverSubscription;
        private List<SubChannel> _subChannels;

        public IList<SubChannel> GetSubConnections()
        {
            lock (_subChannels)
            {
                return _subChannels.ToArray();
            }
        }

        public GrpcClientChannel(AddressResolver resolver, ILoggerFactory loggerFactory) : base(loggerFactory)
        {
            _subChannels = new List<SubChannel>();
            _resolver = resolver;
            _loggerFactory = loggerFactory;
        }

        public void ConfigureBalancer(Func<GrpcClientChannel, LoadBalancer> configure)
        {
            _balancer = configure(this);
        }

        public override SubChannel CreateSubChannel(SubChannelOptions options)
        {
            var subConnection = new GrpcSubChannel(this, options.Addresses);

            Logger.LogInformation("Created sub-connection: " + subConnection);

            lock (_subChannels)
            {
                _subChannels.Add(subConnection);
            }

            return subConnection;
        }

        public override void RemoveSubChannel(SubChannel subChannel)
        {
            Logger.LogInformation("Removing sub-channel: " + subChannel);

            lock (_subChannels)
            {
                var removed = _subChannels.Remove(subChannel);
                Debug.Assert(removed);
            }

            subChannel.Shutdown();
        }

        public override Task ResolveNowAsync(CancellationToken cancellationToken)
        {
            return _resolver.RefreshAsync(cancellationToken);
        }

        public override void UpdateAddresses(SubChannel subConnection, IReadOnlyList<DnsEndPoint> addresses)
        {
            ((GrpcSubChannel)subConnection).UpdateAddresses(addresses);
        }

        private void OnResolverError(Exception error)
        {
            throw new NotImplementedException();
        }

        private void OnResolverResult(AddressResolverResult value)
        {
            _balancer!.UpdateChannelState(new ChannelState(value, GrpcAttributes.Empty));
        }

        public void Dispose()
        {
            _resolverSubscription?.Dispose();
            _balancer?.Dispose();
        }

        internal void OnSubConnectionStateChange(GrpcSubChannel subConnection, ConnectivityState state)
        {
            Logger.LogInformation("Sub-connection state change: " + subConnection + " " + state);
            _balancer!.UpdateSubChannelState(subConnection, new SubChannelState { ConnectivityState = state });
        }

        public override Task ConnectAsync(CancellationToken cancellationToken)
        {
            if (_resolverSubscription == null)
            {
                // Default to PickFirstBalancer
                if (_balancer == null)
                {
                    _balancer = new PickFirstBalancer(this, _loggerFactory);
                }

                _resolverSubscription = _resolver.Subscribe(new ResolverObserver(this));
            }

            return Task.CompletedTask;
        }

        private class ResolverObserver : IObserver<AddressResolverResult>
        {
            private readonly GrpcClientChannel _channel;

            public ResolverObserver(GrpcClientChannel channel)
            {
                _channel = channel;
            }

            public void OnCompleted()
            {
            }

            public void OnError(Exception error)
            {
                _channel.OnResolverError(error);
            }

            public void OnNext(AddressResolverResult value)
            {
                _channel.OnResolverResult(value);
            }
        }
    }

}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif