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
using Grpc.Net.Client.Internal;
using Grpc.Shared;
using Grpc.Tests.Shared;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace Grpc.Net.Client.Tests.Balancer
{
    [TestFixture]
    public class RoundRobinBalancerTests : FunctionalTestBase
    {
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

            var grpcConnection = new ClientChannel(new StaticAddressResolver(new[] { new DnsEndPoint(endpoint.Address.Host, endpoint.Address.Port) }), LoggerFactory);
            grpcConnection.ConfigureBalancer(c => new RoundRobinBalancer(c, LoggerFactory));

            var channel = GrpcChannel.ForAddress(endpoint.Address, new GrpcChannelOptions
            {
                LoggerFactory = LoggerFactory,
                HttpHandler = BalancerHelpers.CreateBalancerHandler(grpcConnection, LoggerFactory)
            });

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

            var grpcConnection = new ClientChannel(new StaticAddressResolver(new[]
            {
                new DnsEndPoint(endpoint1.Address.Host, endpoint1.Address.Port),
                new DnsEndPoint(endpoint2.Address.Host, endpoint2.Address.Port)
            }), LoggerFactory);
            grpcConnection.ConfigureBalancer(c => new RoundRobinBalancer(c, LoggerFactory, new TestRandomGenerator()));

            await grpcConnection.ConnectAsync(CancellationToken.None);
            await TestHelpers.AssertIsTrueRetryAsync(() =>
            {
                var picker = grpcConnection._picker as RoundRobinPicker;
                return picker?._subChannels.Count == 2;
            }, "Wait for all subconnections to be connected.").DefaultTimeout();

            var channel = GrpcChannel.ForAddress(endpoint1.Address, new GrpcChannelOptions
            {
                LoggerFactory = LoggerFactory,
                HttpHandler = BalancerHelpers.CreateBalancerHandler(grpcConnection, LoggerFactory)
            });

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

            var grpcConnection = new ClientChannel(new StaticAddressResolver(new[]
            {
                new DnsEndPoint(endpoint1.Address.Host, endpoint1.Address.Port),
                new DnsEndPoint(endpoint2.Address.Host, endpoint2.Address.Port)
            }), LoggerFactory);
            await grpcConnection.ConnectAsync(CancellationToken.None);

            var channel = GrpcChannel.ForAddress(endpoint1.Address, new GrpcChannelOptions
            {
                LoggerFactory = LoggerFactory,
                HttpHandler = BalancerHelpers.CreateBalancerHandler(grpcConnection, LoggerFactory)
            });

            var client = TestClientFactory.Create(channel, endpoint1.Method);

            var reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" });
            Assert.AreEqual("Balancer", reply.Message);
            Assert.AreEqual("127.0.0.1:50250", host);

            endpoint1.Dispose();

            reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" });
            Assert.AreEqual("Balancer", reply.Message);
            Assert.AreEqual("127.0.0.1:50251", host);
        }

        private class TestRandomGenerator : IRandomGenerator
        {
            public int Next(int minValue, int maxValue)
            {
                return 0;
            }
        }
    }
}

#endif