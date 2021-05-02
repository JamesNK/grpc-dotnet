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
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using FunctionalTestsWebsite;
using Google.Protobuf;
using Grpc.AspNetCore.FunctionalTests.Infrastructure;
using Grpc.Core;
using Grpc.Net.Client.Balancer;
using Grpc.Net.Client.Balancer.Internal;
using Grpc.Net.Client.Configuration;
using Grpc.Net.Client.Internal;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Tests.Balancer
{
    internal static class BalancerHelpers
    {
        public static EndpointContext<TRequest, TResponse> CreateGrpcEndpoint<TRequest, TResponse>(
            int port,
            UnaryServerMethod<TRequest, TResponse> callHandler,
            string methodName,
            HttpProtocols? protocols = null,
            bool? isHttps = null)
            where TRequest : class, IMessage, new()
            where TResponse : class, IMessage, new()
        {
            var server = CreateServer(port, protocols, isHttps);
            var method = server.DynamicGrpc.AddUnaryMethod(callHandler, methodName);
            var url = server.GetUrl(isHttps.GetValueOrDefault(false) ? TestServerEndpointName.Http2WithTls : TestServerEndpointName.Http2);

            return new EndpointContext<TRequest, TResponse>(server, method, url);
        }

        public class EndpointContext<TRequest, TResponse> : IDisposable
            where TRequest : class, IMessage, new()
            where TResponse : class, IMessage, new()
        {
            private readonly GrpcTestFixture<Startup> _server;

            public EndpointContext(GrpcTestFixture<Startup> server, Method<TRequest, TResponse> method, Uri address)
            {
                _server = server;
                Method = method;
                Address = address;
            }

            public Method<TRequest, TResponse> Method { get; }
            public Uri Address { get; }
            public ILoggerFactory LoggerFactory => _server.LoggerFactory;

            public void Dispose()
            {
                _server.Dispose();
            }
        }

        public static GrpcTestFixture<Startup> CreateServer(int port, HttpProtocols? protocols = null, bool? isHttps = null)
        {
            var endpointName = isHttps.GetValueOrDefault(false) ? TestServerEndpointName.Http2WithTls : TestServerEndpointName.Http2;

            return new GrpcTestFixture<Startup>(
                null,
                (options, urls) =>
                {
                    urls[endpointName] = isHttps.GetValueOrDefault(false)
                        ? $"https://127.0.0.1:{port}"
                        : $"http://127.0.0.1:{port}";
                    options.ListenLocalhost(port, listenOptions =>
                    {
                        listenOptions.Protocols = protocols ?? HttpProtocols.Http2;

                        if (isHttps.GetValueOrDefault(false))
                        {
                            var basePath = Path.GetDirectoryName(typeof(InProcessTestServer).Assembly.Location);
                            var certPath = Path.Combine(basePath!, "server1.pfx");
                            listenOptions.UseHttps(certPath, "1111");
                        }
                    });
                },
                endpointName);
        }

        public static BalancerHttpHandler CreateBalancerHandler(ClientChannel grpcConnection, ILoggerFactory loggerFactory, HttpMessageHandler? innerHandler = null)
        {
            return new BalancerHttpHandler(
                innerHandler ?? new SocketsHttpHandler { EnableMultipleHttp2Connections = true },
                grpcConnection);
        }

        public static async Task<GrpcChannel> CreateChannel(ILoggerFactory loggerFactory, LoadBalancingConfig? loadBalancingConfig, params Uri[] endpoints)
        {
            var e = endpoints.Select(i => new DnsEndPoint(i.Host, i.Port)).ToArray();

            var services = new ServiceCollection();
            services.AddSingleton<AddressResolverFactory>(new StaticAddressResolverFactory(e));
            services.AddSingleton<IRandomGenerator>(new TestRandomGenerator());

            var serviceConfig = new ServiceConfig();
            if (loadBalancingConfig != null)
            {
                serviceConfig.LoadBalancingConfigs.Add(loadBalancingConfig);
            }

            var channel = GrpcChannel.ForAddress("static://localhost", new GrpcChannelOptions
            {
                LoggerFactory = loggerFactory,
                Credentials = ChannelCredentials.Insecure,
                ServiceProvider = services.BuildServiceProvider(),
                ServiceConfig = serviceConfig
            });

            await channel.ConnectAsync();
            return channel;
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