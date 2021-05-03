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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Greet;
using Grpc.Core;
using Grpc.Net.Client.Tests.Infrastructure;
using Grpc.Net.Client.Configuration;
using Grpc.Tests.Shared;
using NUnit.Framework;
using Microsoft.Extensions.Logging.Testing;
using System.Linq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using System.Net;
using System.Collections.Generic;
using Grpc.Net.Client.Balancer.Internal;
using System.IO;
#if HAVE_LOAD_BALANCING
using Grpc.Net.Client.Balancer;
#endif

namespace Grpc.Net.Client.Tests.Balancer
{
    [TestFixture]
    public class AddressResolverTests
    {
        [Test]
        public async Task AddressResolver_ResolveNameFromServices_Success()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddSingleton<AddressResolverFactory>(new StaticAddressResolverFactory(new List<DnsEndPoint>
            {
                new DnsEndPoint("localhost", 80)
            }));
            services.AddSingleton<ISubChannelTransportFactory>(new TestSubChannelTransportFactory());

            var channelOptions = new GrpcChannelOptions
            {
                Credentials = ChannelCredentials.Insecure,
                ServiceProvider = services.BuildServiceProvider()
            };

            // Act
            var channel = GrpcChannel.ForAddress("static://localhost", channelOptions);
            await channel.ConnectAsync();

            // Assert
            var subChannels = channel.ClientChannel.GetSubChannels();
            Assert.AreEqual(1, subChannels.Count);
        }

        private class TestSubChannelTransportFactory : ISubChannelTransportFactory
        {
            public ISubChannelTransport Create(SubChannel subChannel)
            {
                return new TestSubChannelTransport();
            }
        }

        private class TestSubChannelTransport : ISubChannelTransport
        {
            public DnsEndPoint? CurrentEndPoint { get; }

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
                return new ValueTask<bool>(true);
            }
        }
    }
}

#endif
