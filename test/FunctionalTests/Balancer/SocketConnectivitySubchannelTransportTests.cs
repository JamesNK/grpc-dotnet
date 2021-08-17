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

#if SUPPORT_LOAD_BALANCING
#if NET5_0_OR_GREATER

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading.Tasks;
using Greet;
using Grpc.AspNetCore.FunctionalTests.Infrastructure;
using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Balancer;
using Grpc.Net.Client.Balancer.Internal;
using Grpc.Net.Client.Configuration;
using Grpc.Tests.Shared;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace Grpc.AspNetCore.FunctionalTests.Balancer
{
    [TestFixture]
    public class SocketConnectivitySubchannelTransportTests : FunctionalTestBase
    {
        [Test]
        public async Task BlahBlah()
        {
            // Ignore errors
            SetExpectedErrorsFilter(writeContext =>
            {
                return true;
            });

            // Arrange
            //using var endpoint = BalancerHelpers.CreateServer(50051);

            Socket listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            listenSocket.Bind(new IPEndPoint(IPAddress.Any, 50051));
            listenSocket.Listen();

            var serverSocketTask = listenSocket.AcceptAsync();

            var resolver = new TestResolver();
            var transportFactory = new TestSubchannelTransportFactory(TimeSpan.FromMilliseconds(100), LoggerFactory);
            var clientChannel = new ConnectionManager(resolver, disableResolverServiceConfig: false, LoggerFactory, transportFactory, new LoadBalancerFactory[0]);

            IChannelControlHelper helper = clientChannel;

            var subchannel = helper.CreateSubchannel(new SubchannelOptions(new List<DnsEndPoint>
            {
                new DnsEndPoint("127.0.0.1", 50051)
            }));

            subchannel.RequestConnection();

            var serverSocket = await serverSocketTask.DefaultTimeout();

            // Assert
            await TestHelpers.AssertIsTrueRetryAsync(
                () => subchannel.State == ConnectivityState.Ready,
                "Wait for subchannel to fail.").DefaultTimeout();

            //serverSocket.ReceiveAsync

            // Act
            //endpoint.Dispose();

            await Task.Delay(5000);
        }
    }
}

#endif
#endif
