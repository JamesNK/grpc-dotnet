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
            using var server1 = CreateServer(50150);
            var method1 = server1.DynamicGrpc.AddUnaryMethod<HelloRequest, HelloReply>(UnaryMethod, nameof(UnaryMethod));
            var url1 = server1.GetUrl(TestServerEndpointName.Http2);

            var grpcConnection = new GrpcConnection(new StaticAddressResolver(new[] { new DnsEndPoint(url1.Host, url1.Port) }), LoggerFactory);
            grpcConnection.ConfigureBalancer(c => new RoundRobinBalancer(c));
            grpcConnection.Start();

            var channel = GrpcChannel.ForAddress(url1, new GrpcChannelOptions
            {
                LoggerFactory = LoggerFactory,
                HttpHandler = new BalancerHttpHandler(new SocketsHttpHandler(), grpcConnection)
            });

            var client = TestClientFactory.Create(channel, method1);

            var reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" });

            Assert.AreEqual("Balancer", reply.Message);
            Assert.AreEqual("127.0.0.1:50150", host);

            server1.Dispose();

            using var server1New = CreateServer(50150);
            method1 = server1New.DynamicGrpc.AddUnaryMethod<HelloRequest, HelloReply>(UnaryMethod, nameof(UnaryMethod));

            reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" });

            Assert.AreEqual("Balancer", reply.Message);
            Assert.AreEqual("127.0.0.1:50150", host);
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
            using var server1 = CreateServer(50150);
            var method1 = server1.DynamicGrpc.AddUnaryMethod<HelloRequest, HelloReply>(UnaryMethod, nameof(UnaryMethod));
            var url1 = server1.GetUrl(TestServerEndpointName.Http2);

            using var server2 = CreateServer(50151);
            var method2 = server2.DynamicGrpc.AddUnaryMethod<HelloRequest, HelloReply>(UnaryMethod, nameof(UnaryMethod));
            var url2 = server2.GetUrl(TestServerEndpointName.Http2);

            var grpcConnection = new GrpcConnection(new StaticAddressResolver(new[] { new DnsEndPoint(url1.Host, url1.Port), new DnsEndPoint(url2.Host, url2.Port) }), LoggerFactory);
            grpcConnection.Start();

            var channel = GrpcChannel.ForAddress(url1, new GrpcChannelOptions
            {
                LoggerFactory = LoggerFactory,
                HttpHandler = new BalancerHttpHandler(new SocketsHttpHandler(), grpcConnection)
            });

            var client = TestClientFactory.Create(channel, method1);

            var reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" });
            Assert.AreEqual("Balancer", reply.Message);
            Assert.AreEqual("127.0.0.1:50150", host);

            reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" });
            Assert.AreEqual("Balancer", reply.Message);
            Assert.AreEqual("127.0.0.1:50151", host);

            reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" });
            Assert.AreEqual("Balancer", reply.Message);
            Assert.AreEqual("127.0.0.1:50150", host);
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
            using var server1 = CreateServer(50150);
            var method1 = server1.DynamicGrpc.AddUnaryMethod<HelloRequest, HelloReply>(UnaryMethod, nameof(UnaryMethod));
            var url1 = server1.GetUrl(TestServerEndpointName.Http2);

            using var server2 = CreateServer(50151);
            var method2 = server2.DynamicGrpc.AddUnaryMethod<HelloRequest, HelloReply>(UnaryMethod, nameof(UnaryMethod));
            var url2 = server2.GetUrl(TestServerEndpointName.Http2);

            var grpcConnection = new GrpcConnection(new StaticAddressResolver(new[] { new DnsEndPoint(url1.Host, url1.Port), new DnsEndPoint(url2.Host, url2.Port) }), LoggerFactory);
            grpcConnection.Start();

            var channel = GrpcChannel.ForAddress(url1, new GrpcChannelOptions
            {
                LoggerFactory = LoggerFactory,
                HttpHandler = new BalancerHttpHandler(new SocketsHttpHandler(), grpcConnection)
            });

            var client = TestClientFactory.Create(channel, method1);

            var reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" });
            Assert.AreEqual("Balancer", reply.Message);
            Assert.AreEqual("127.0.0.1:50150", host);

            server1.Dispose();

            reply = await client.UnaryCall(new HelloRequest { Name = "Balancer" });
            Assert.AreEqual("Balancer", reply.Message);
            Assert.AreEqual("127.0.0.1:50151", host);
        }

        private GrpcTestFixture<FunctionalTestsWebsite.Startup> CreateServer(int port)
        {
            return new GrpcTestFixture<FunctionalTestsWebsite.Startup>(ConfigureServices, (options, urls) =>
            {
                urls[TestServerEndpointName.Http2] = $"http://127.0.0.1:{port}";
                options.ListenLocalhost(port, listenOptions =>
                {
                    listenOptions.Protocols = HttpProtocols.Http2;
                });
            });
        }
    }
}

#endif