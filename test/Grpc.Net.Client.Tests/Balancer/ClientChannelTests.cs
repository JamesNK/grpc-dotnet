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
using Microsoft.Extensions.Logging.Abstractions;
using Grpc.Net.Client.Tests.Infrastructure.Balancer;
#if HAVE_LOAD_BALANCING
using Grpc.Net.Client.Balancer;
#endif

namespace Grpc.Net.Client.Tests.Balancer
{
    [TestFixture]
    public class ClientChannelTests
    {
        [Test]
        public async Task PickAsync_ChannelStateChangesWithWaitForReady_WaitsForCorrectEndpoint()
        {
            // Arrange
            var addressResolver = new TestAddressResolver();
            addressResolver.UpdateEndPoints(new List<DnsEndPoint>
            {
                new DnsEndPoint("localhost", 80)
            });

            var transportFactory = new TestSubChannelTransportFactory();
            var clientChannel = new ClientChannel(addressResolver, NullLoggerFactory.Instance, transportFactory);
            clientChannel.ConfigureBalancer(b => new RoundRobinBalancer(b, NullLoggerFactory.Instance));

            // Act
            var pickTask1 = clientChannel.PickAsync(
                new PickContext(new HttpRequestMessage(), waitForReady: false),
                CancellationToken.None).AsTask();

            await clientChannel.ConnectAsync(CancellationToken.None).DefaultTimeout();

            var result1 = await pickTask1.DefaultTimeout();

            // Assert
            Assert.AreEqual(new DnsEndPoint("localhost", 80), result1.EndPoint!);

            addressResolver.UpdateEndPoints(new List<DnsEndPoint>
            {
                new DnsEndPoint("localhost", 80),
                new DnsEndPoint("localhost", 81)
            });

            for (var i = 0; i < transportFactory.Transports.Count; i++)
            {
                transportFactory.Transports[i].SubChannel.UpdateConnectivityState(ConnectivityState.TransientFailure);
            }

            var pickTask2 = clientChannel.PickAsync(
                new PickContext(new HttpRequestMessage(), waitForReady: true),
                CancellationToken.None).AsTask().DefaultTimeout();

            Assert.IsFalse(pickTask2.IsCompleted);

            addressResolver.UpdateEndPoints(new List<DnsEndPoint>
            {
                new DnsEndPoint("localhost", 82)
            });

            var result2 = await pickTask2.DefaultTimeout();
            Assert.AreEqual(new DnsEndPoint("localhost", 82), result2.EndPoint!);
        }
    }
}

#endif
