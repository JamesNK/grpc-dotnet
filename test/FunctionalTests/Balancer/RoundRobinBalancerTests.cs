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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Greet;
using Grpc.AspNetCore.FunctionalTests;
using Grpc.AspNetCore.FunctionalTests.Client;
using Grpc.AspNetCore.FunctionalTests.Infrastructure;
using Grpc.Core;
using Grpc.Net.Client.Balancer;
using Grpc.Net.Client.Configuration;
using Grpc.Net.Client.Internal;
using Grpc.Shared;
using Grpc.Tests.Shared;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace Grpc.Net.Client.Tests.Balancer
{
    [TestFixture]
    public class RoundRobinBalancerTests : FunctionalTestBase
    {
        [Test]
        public async Task DisconnectEndpoint_NoCallsMade_SubChannelStateUpdated()
        {
            // Ignore errors
            SetExpectedErrorsFilter(writeContext =>
            {
                return true;
            });

            string? host = null;
            Task<HelloReply> UnaryMethod(HelloRequest request, ServerCallContext context)
            {
                host = context.Host;
                return Task.FromResult(new HelloReply { Message = request.Name });
            }

            // Arrange
            using var endpoint = BalancerHelpers.CreateGrpcEndpoint<HelloRequest, HelloReply>(50250, UnaryMethod, nameof(UnaryMethod));

            var channel = await BalancerHelpers.CreateChannel(LoggerFactory, new RoundRobinConfig(), new[] { endpoint.Address });

            await channel.ConnectAsync().DefaultTimeout();

            SubChannel? subChannel = null;
            await TestHelpers.AssertIsTrueRetryAsync(() =>
            {
                var picker = channel.ClientChannel._picker as RoundRobinPicker;
                if (picker?._subChannels.Count == 1)
                {
                    subChannel = picker?._subChannels[0].SubChannel;
                    return true;
                }
                return false;
            }, "Wait for all sub-channels to be connected.").DefaultTimeout();

            // Act
            endpoint.Dispose();

            // Assert
            await TestHelpers.AssertIsTrueRetryAsync(
                () => subChannel!.State == ConnectivityState.TransientFailure,
                "Wait for sub-channel to fail.").DefaultTimeout();
        }

        [Test]
        public async Task UnaryCall_ReconnectBetweenCalls_Success()
        {
            // Ignore errors
            SetExpectedErrorsFilter(writeContext =>
            {
                return true;
            });

            string? host = null;
            Task<HelloReply> UnaryMethod(HelloRequest request, ServerCallContext context)
            {
                host = context.Host;
                return Task.FromResult(new HelloReply { Message = request.Name });
            }

            // Arrange
            using var endpoint = BalancerHelpers.CreateGrpcEndpoint<HelloRequest, HelloReply>(50250, UnaryMethod, nameof(UnaryMethod));

            var channel = await BalancerHelpers.CreateChannel(LoggerFactory, new RoundRobinConfig(), new[] { endpoint.Address });

            var client = TestClientFactory.Create(channel, endpoint.Method);

            // Act
            var reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" }).ResponseAsync.DefaultTimeout();

            // Assert
            Assert.AreEqual("Balancer", reply.Message);
            Assert.AreEqual("127.0.0.1:50250", host);

            endpoint.Dispose();

            using var endpointNew = BalancerHelpers.CreateGrpcEndpoint<HelloRequest, HelloReply>(50250, UnaryMethod, nameof(UnaryMethod));

            // Act
            reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" }).ResponseAsync.DefaultTimeout();

            // Assert
            Assert.AreEqual("Balancer", reply.Message);
            Assert.AreEqual("127.0.0.1:50250", host);
        }

        [Test]
        public async Task UnaryCall_MultipleCalls_RoundRobin()
        {
            // Ignore errors
            SetExpectedErrorsFilter(writeContext =>
            {
                return true;
            });

            string? host = null;
            Task<HelloReply> UnaryMethod(HelloRequest request, ServerCallContext context)
            {
                host = context.Host;
                return Task.FromResult(new HelloReply { Message = request.Name });
            }

            // Arrange
            using var endpoint1 = BalancerHelpers.CreateGrpcEndpoint<HelloRequest, HelloReply>(50250, UnaryMethod, nameof(UnaryMethod));
            using var endpoint2 = BalancerHelpers.CreateGrpcEndpoint<HelloRequest, HelloReply>(50251, UnaryMethod, nameof(UnaryMethod));

            var channel = await BalancerHelpers.CreateChannel(LoggerFactory, new RoundRobinConfig(), new[] { endpoint1.Address, endpoint2.Address });

            await TestHelpers.AssertIsTrueRetryAsync(() =>
            {
                var picker = channel.ClientChannel._picker as RoundRobinPicker;
                return picker?._subChannels.Count(s => s.SubChannel.State == ConnectivityState.Ready) == 2;
            }, "Wait for all subconnections to be connected.").DefaultTimeout();

            var client = TestClientFactory.Create(channel, endpoint1.Method);

            // Act
            var reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" }).ResponseAsync.DefaultTimeout();
            // Assert
            Assert.AreEqual("Balancer", reply.Message);
            Assert.AreEqual("127.0.0.1:50250", host);

            // Act
            reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" }).ResponseAsync.DefaultTimeout();
            // Assert
            Assert.AreEqual("Balancer", reply.Message);
            Assert.AreEqual("127.0.0.1:50251", host);

            // Act
            reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" }).ResponseAsync.DefaultTimeout();
            // Assert
            Assert.AreEqual("Balancer", reply.Message);
            Assert.AreEqual("127.0.0.1:50250", host);
        }

        [Test]
        public async Task UnaryCall_UnavailableAddress_FallbackToWorkingAddress()
        {
            // Ignore errors
            SetExpectedErrorsFilter(writeContext =>
            {
                return true;
            });

            string? host = null;
            Task<HelloReply> UnaryMethod(HelloRequest request, ServerCallContext context)
            {
                host = context.Host;
                return Task.FromResult(new HelloReply { Message = request.Name });
            }

            // Arrange
            using var endpoint1 = BalancerHelpers.CreateGrpcEndpoint<HelloRequest, HelloReply>(50250, UnaryMethod, nameof(UnaryMethod));
            using var endpoint2 = BalancerHelpers.CreateGrpcEndpoint<HelloRequest, HelloReply>(50251, UnaryMethod, nameof(UnaryMethod));

            var channel = await BalancerHelpers.CreateChannel(LoggerFactory, new RoundRobinConfig(), new[] { endpoint1.Address, endpoint2.Address });

            await TestHelpers.AssertIsTrueRetryAsync(() =>
            {
                var picker = channel.ClientChannel._picker as RoundRobinPicker;
                return picker?._subChannels.Count == 2;
            }, "Wait for all subconnections to be connected.").DefaultTimeout();

            var client = TestClientFactory.Create(channel, endpoint1.Method);

            var reply1 = await client.UnaryCall(new HelloRequest { Name = "Balancer1" });
            Assert.AreEqual("Balancer1", reply1.Message);
            var host1 = host;

            var reply2 = await client.UnaryCall(new HelloRequest { Name = "Balancer2" });
            Assert.AreEqual("Balancer2", reply2.Message);
            var host2 = host;

            Assert.Contains("127.0.0.1:50250", new[] { host1, host2 });
            Assert.Contains("127.0.0.1:50251", new[] { host1, host2 });

            endpoint1.Dispose();

            reply1 = await client.UnaryCall(new HelloRequest { Name = "Balancer" });
            Assert.AreEqual("Balancer", reply1.Message);
            Assert.AreEqual("127.0.0.1:50251", host);
        }
    }
}

#endif