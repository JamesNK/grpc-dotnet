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

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client.Balancer;
using Grpc.Net.Client.Balancer.Internal;

namespace Grpc.Net.Client.Tests.Infrastructure.Balancer
{
    internal class TestAddressResolverFactory : AddressResolverFactory
    {
        private readonly TestAddressResolver _addressResolver;

        public override string Name { get; } = "test";

        public TestAddressResolverFactory(TestAddressResolver addressResolver)
        {
            _addressResolver = addressResolver;
        }

        public override AddressResolver Create(Uri address, AddressResolverOptions options)
        {
            return _addressResolver;
        }
    }

    internal class TestAddressResolver : AddressResolver, IDisposable
    {
        private readonly Task? _refreshAsyncTask;
        private IObserver<AddressResolverResult>? _observer;
        private IReadOnlyList<DnsEndPoint>? _endPoints;

        public TestAddressResolver(Task? refreshAsyncTask = null)
        {
            _refreshAsyncTask = refreshAsyncTask;
        }

        public void UpdateEndPoints(List<DnsEndPoint> endPoints)
        {
            _endPoints = endPoints;
            _observer?.OnNext(new AddressResolverResult(_endPoints));
        }

        public void Dispose()
        {
            _observer = null;
        }

        public override Task RefreshAsync(CancellationToken cancellationToken)
        {
            return _refreshAsyncTask ?? Task.CompletedTask;
        }

        public override void Shutdown()
        {
        }

        public override IDisposable Subscribe(IObserver<AddressResolverResult> observer)
        {
            _observer = observer;
            _observer.OnNext(new AddressResolverResult(_endPoints ?? Array.Empty<DnsEndPoint>()));
            return this;
        }
    }

    internal class TestSubChannelTransportFactory : ISubChannelTransportFactory
    {
        public List<TestSubChannelTransport> Transports { get; } = new List<TestSubChannelTransport>();

        public ISubChannelTransport Create(SubChannel subChannel)
        {
            var transport = new TestSubChannelTransport(subChannel);
            Transports.Add(transport);

            return transport;
        }
    }

    internal class TestSubChannelTransport : ISubChannelTransport
    {
        public SubChannel SubChannel { get; }

        public DnsEndPoint? CurrentEndPoint { get; private set; }

        public TestSubChannelTransport(SubChannel subChannel)
        {
            SubChannel = subChannel;
        }

        public void Dispose()
        {
        }

        public ValueTask<Stream> GetStreamAsync(DnsEndPoint endPoint, CancellationToken cancellationToken)
        {
            return new ValueTask<Stream>(new MemoryStream());
        }

        public void OnRequestError(Exception ex)
        {
        }

        public void OnRequestSuccess()
        {
        }

        public ValueTask<bool> TryConnectAsync(CancellationToken cancellationToken)
        {
            CurrentEndPoint = SubChannel._addresses[0];
            SubChannel.UpdateConnectivityState(ConnectivityState.Ready);
            return new ValueTask<bool>(true);
        }
    }
}

#endif
