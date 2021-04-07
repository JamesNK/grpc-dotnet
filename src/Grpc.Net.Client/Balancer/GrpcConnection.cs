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
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

#if NET5_0_OR_GREATER
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Grpc.Net.Client.Balancer
{
    internal class GrpcConnection : ClientConnection, IDisposable
    {
        private readonly AddressResolver _resolver;

        internal LoadBalancer? _balancer;
        private IDisposable? _resolverSubscription;

        public GrpcConnection(AddressResolver resolver, ILoggerFactory loggerFactory) : base(loggerFactory)
        {
            _resolver = resolver;
        }

        public void ConfigureBalancer(Func<GrpcConnection, LoadBalancer> configure)
        {
            _balancer = configure(this);
        }

        public override SubConnection CreateSubConnection(SubConnectionOptions options)
        {
            var subConnection = new GrpcSubConnection(this, options.Addresses);

            Logger.LogInformation("Created sub-connection: " + subConnection);

            return subConnection;
        }

        public override void RemoveSubConnection(SubConnection subConnection)
        {
            Logger.LogInformation("Removing sub-connection: " + subConnection);
        }

        public override Task ResolveNowAsync(CancellationToken cancellationToken)
        {
            return _resolver.RefreshAsync(cancellationToken);
        }

        public override void UpdateAddresses(SubConnection subConnection, IReadOnlyList<DnsEndPoint> addresses)
        {
            ((GrpcSubConnection)subConnection).UpdateAddresses(addresses);
        }

        private void OnResolverError(Exception error)
        {
            throw new NotImplementedException();
        }

        private void OnResolverResult(AddressResolverResult value)
        {
            _balancer!.UpdateConnectionState(new ConnectionState(value, GrpcAttributes.Empty));
        }

        public void Dispose()
        {
            _resolverSubscription?.Dispose();
            _balancer?.Dispose();
        }

        internal void OnSubConnectionStateChange(GrpcSubConnection subConnection, ConnectivityState state)
        {
            Logger.LogInformation("Sub-connection state change: " + subConnection + " " + state);
            _balancer!.UpdateSubConnectionState(subConnection, new SubConnectionState { ConnectivityState = state });
        }

        public override Task ConnectAsync(CancellationToken cancellationToken)
        {
            if (_resolverSubscription == null)
            {
                // Default to PickFirstBalancer
                if (_balancer == null)
                {
                    _balancer = new PickFirstBalancer(this);
                }

                _resolverSubscription = _resolver.Subscribe(new ResolverObserver(this));
            }

            return Task.CompletedTask;
        }

        private class ResolverObserver : IObserver<AddressResolverResult>
        {
            private readonly GrpcConnection _connection;

            public ResolverObserver(GrpcConnection connection)
            {
                _connection = connection;
            }

            public void OnCompleted()
            {
            }

            public void OnError(Exception error)
            {
                _connection.OnResolverError(error);
            }

            public void OnNext(AddressResolverResult value)
            {
                _connection.OnResolverResult(value);
            }
        }
    }

}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif