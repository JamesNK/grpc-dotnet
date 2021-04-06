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

#if NET5_0_OR_GREATER
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Grpc.Net.Client.Balancer;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Balancer
{
    internal class GrpcConnection : ClientConnection, IDisposable
    {
        private readonly AddressResolver _resolver;

        internal LoadBalancer? _balancer;
        private IDisposable? _resolverSubscription;

        public override ILoggerFactory LoggerFactory { get; }
        public ILogger Logger { get; }

        public GrpcConnection(AddressResolver resolver, ILoggerFactory loggerFactory)
        {
            _resolver = resolver;
            LoggerFactory = loggerFactory;
            Logger = loggerFactory.CreateLogger<GrpcConnection>();
        }

        public void ConfigureBalancer(Func<GrpcConnection, LoadBalancer> configure)
        {
            _balancer = configure(this);
        }

        public void Start()
        {
            // Default to PickFirstBalancer
            if (_balancer == null)
            {
                _balancer = new PickFirstBalancer(this);
            }

            _resolverSubscription = _resolver.Subscribe(new ResolverObserver(this));
        }

        public override SubConnection CreateSubConnection(SubConnectionOptions options)
        {
            return new GrpcSubConnection(this, options.Addresses);
        }

        public override void RemoveSubConnection(SubConnection subConnection)
        {
            throw new NotImplementedException();
        }

        public override Task ResolveNowAsync()
        {
            return _resolver.RefreshAsync();
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
        }

        internal void OnSubConnectionStateChange(GrpcSubConnection subConnection, ConnectivityState state)
        {
            _balancer!.UpdateSubConnectionState(subConnection, new SubConnectionState { ConnectivityState = state });
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