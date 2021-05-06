﻿#region Copyright notice and license

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
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using FunctionalTestsWebsite;
using Google.Protobuf;
using Greet;
using Grpc.AspNetCore.FunctionalTests;
using Grpc.AspNetCore.FunctionalTests.Infrastructure;
using Grpc.Core;
using Grpc.Net.Client.Balancer;
using Grpc.Net.Client.Balancer.Internal;
using Grpc.Net.Client.Configuration;
using Grpc.Net.Client.Web;
using Grpc.Tests.Shared;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace Grpc.Net.Client.Tests.Balancer
{
    [TestFixture]
    public class PickFirstBalancerTests : FunctionalTestBase
    {
        private GrpcChannel CreateGrpcWebChannel(TestServerEndpointName endpointName, Version? version)
        {
            var grpcWebHandler = new GrpcWebHandler(GrpcWebMode.GrpcWeb);
            grpcWebHandler.HttpVersion = version;

            var httpClient = Fixture.CreateClient(endpointName, grpcWebHandler);
            var channel = GrpcChannel.ForAddress(httpClient.BaseAddress!, new GrpcChannelOptions
            {
                HttpClient = httpClient,
                LoggerFactory = LoggerFactory
            });

            return channel;
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

            var channel = await BalancerHelpers.CreateChannel(LoggerFactory, new PickFirstConfig(), new[] { endpoint.Address }).DefaultTimeout();

            var client = TestClientFactory.Create(channel, endpoint.Method);

            // Act
            var reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" }).ResponseAsync.DefaultTimeout();

            // Assert
            Assert.AreEqual("Balancer", reply.Message);
            Assert.AreEqual("127.0.0.1:50250", host);

            endpoint.Dispose();

            using var endpointNew = BalancerHelpers.CreateGrpcEndpoint<HelloRequest, HelloReply>(50250, UnaryMethod, nameof(UnaryMethod));

            reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" }).ResponseAsync.DefaultTimeout();

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

            var channel = await BalancerHelpers.CreateChannel(LoggerFactory, new PickFirstConfig(), new[] { endpoint1.Address, endpoint2.Address }).DefaultTimeout();

            var client = TestClientFactory.Create(channel, endpoint1.Method);

            // Act
            var reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" }).ResponseAsync.DefaultTimeout();

            // Assert
            Assert.AreEqual("Balancer", reply.Message);
            Assert.AreEqual("127.0.0.1:50250", host);

            endpoint1.Dispose();

            reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" }).ResponseAsync.DefaultTimeout();
            Assert.AreEqual("Balancer", reply.Message);
            Assert.AreEqual("127.0.0.1:50251", host);
        }

        [Test]
        public async Task UnaryCall_MultipleStreams_UnavailableAddress_FallbackToWorkingAddress()
        {
            // Ignore errors
            SetExpectedErrorsFilter(writeContext =>
            {
                return true;
            });

            var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
            string? host = null;
            async Task<HelloReply> UnaryMethod(HelloRequest request, ServerCallContext context)
            {
                await tcs.Task;
                host = context.Host;
                return new HelloReply { Message = request.Name };
            }

            // Arrange
            using var endpoint1 = BalancerHelpers.CreateGrpcEndpoint<HelloRequest, HelloReply>(50250, UnaryMethod, nameof(UnaryMethod));
            using var endpoint2 = BalancerHelpers.CreateGrpcEndpoint<HelloRequest, HelloReply>(50251, UnaryMethod, nameof(UnaryMethod));

            var channel = await BalancerHelpers.CreateChannel(LoggerFactory, new PickFirstConfig(), new[] { endpoint1.Address, endpoint2.Address }).DefaultTimeout();
            var balancer = (PickFirstBalancer)channel.ClientChannel._balancer!;

            var client = TestClientFactory.Create(channel, endpoint1.Method);

            // Act
            var callTasks = new List<Task>();
            for (int i = 0; i < 150; i++)
            {
                Logger.LogInformation($"Sending gRPC call {i}");

                callTasks.Add(client.UnaryCall(new HelloRequest { Name = "Balancer" }).ResponseAsync);
            }

            Logger.LogInformation($"Done sending gRPC calls");

            var subChannel = balancer!._subChannel!;
            var transport = (DefaultSubChannelTransport)subChannel.Transport;
            var activeStreams = transport._activeStreams;

            // Assert
            Assert.AreEqual(2, activeStreams.Count);
            Assert.AreEqual(new DnsEndPoint("127.0.0.1", 50250), activeStreams[0].EndPoint);
            Assert.AreEqual(new DnsEndPoint("127.0.0.1", 50250), activeStreams[1].EndPoint);

            tcs.SetResult(null);

            await Task.WhenAll(callTasks).DefaultTimeout();

            endpoint1.Dispose();
            Assert.AreEqual(0, activeStreams.Count);

            var reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" }).ResponseAsync.DefaultTimeout();
            Assert.AreEqual("Balancer", reply.Message);
            Assert.AreEqual("127.0.0.1:50251", host);

            Assert.AreEqual(1, activeStreams.Count);
            Assert.AreEqual(new DnsEndPoint("127.0.0.1", 50251), activeStreams[0].EndPoint);
        }
    }
}

#endif