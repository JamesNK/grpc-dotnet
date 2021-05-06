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
    public class WaitForReadyTests
    {
        [Test]
        public async Task AddressResolverReturnsNoAddresses_CallWithWaitForReady_Wait()
        {
            // Arrange
            string? authority = null;
            var testMessageHandler = TestHttpMessageHandler.Create(async request =>
            {
                authority = request.RequestUri!.Authority;
                var reply = new HelloReply { Message = "Hello world" };

                var streamContent = await ClientTestHelpers.CreateResponseContent(reply).DefaultTimeout();

                return ResponseUtils.CreateResponse(HttpStatusCode.OK, streamContent);
            });

            var services = new ServiceCollection();

            var addressResolver = new TestAddressResolver();

            services.AddSingleton<AddressResolverFactory>(new TestAddressResolverFactory(addressResolver));
            services.AddSingleton<ISubChannelTransportFactory>(new TestSubChannelTransportFactory());

            var invoker = HttpClientCallInvokerFactory.Create(testMessageHandler, "test://localhost", configure: o =>
            {
                o.Credentials = ChannelCredentials.Insecure;
                o.ServiceProvider = services.BuildServiceProvider();
            });

            // Act
            var call = invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions().WithWaitForReady(), new HelloRequest());

            var responseTask = call.ResponseAsync;

            Assert.IsFalse(responseTask.IsCompleted);
            Assert.IsNull(authority);

            addressResolver.UpdateEndPoints(new List<DnsEndPoint>
            {
                new DnsEndPoint("localhost", 81)
            });

            await responseTask.DefaultTimeout();
            Assert.AreEqual("localhost:81", authority);
        }

        [Test]
        public async Task AddressResolverReturnsNoAddresses_DeadlineWhileWaitForReady_Error()
        {
            // Arrange
            var testMessageHandler = TestHttpMessageHandler.Create(async request =>
            {
                var reply = new HelloReply { Message = "Hello world" };
                var streamContent = await ClientTestHelpers.CreateResponseContent(reply).DefaultTimeout();
                return ResponseUtils.CreateResponse(HttpStatusCode.OK, streamContent);
            });

            var services = new ServiceCollection();
            var addressResolver = new TestAddressResolver();
            services.AddSingleton<AddressResolverFactory>(new TestAddressResolverFactory(addressResolver));
            services.AddSingleton<ISubChannelTransportFactory>(new TestSubChannelTransportFactory());

            var invoker = HttpClientCallInvokerFactory.Create(testMessageHandler, "test://localhost", configure: o =>
            {
                o.Credentials = ChannelCredentials.Insecure;
                o.ServiceProvider = services.BuildServiceProvider();
            });

            // Act
            var callOptions = new CallOptions(deadline: DateTime.UtcNow.AddSeconds(0.2)).WithWaitForReady();
            var call = invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, callOptions, new HelloRequest());

            var ex = await ExceptionAssert.ThrowsAsync<RpcException>(() => call.ResponseAsync).DefaultTimeout();

            Assert.AreEqual(StatusCode.DeadlineExceeded, ex.StatusCode);
            Assert.AreEqual(string.Empty, ex.Status.Detail);
        }

        [Test]
        public async Task AddressResolverReturnsNoAddresses_DisposeWhileWaitForReady_Error()
        {
            // Arrange
            var testMessageHandler = TestHttpMessageHandler.Create(async request =>
            {
                var reply = new HelloReply { Message = "Hello world" };
                var streamContent = await ClientTestHelpers.CreateResponseContent(reply).DefaultTimeout();
                return ResponseUtils.CreateResponse(HttpStatusCode.OK, streamContent);
            });

            var services = new ServiceCollection();
            var addressResolver = new TestAddressResolver();
            services.AddSingleton<AddressResolverFactory>(new TestAddressResolverFactory(addressResolver));
            services.AddSingleton<ISubChannelTransportFactory>(new TestSubChannelTransportFactory());

            var invoker = HttpClientCallInvokerFactory.Create(testMessageHandler, "test://localhost", configure: o =>
            {
                o.Credentials = ChannelCredentials.Insecure;
                o.ServiceProvider = services.BuildServiceProvider();
            });

            // Act
            var callOptions = new CallOptions().WithWaitForReady();
            var call = invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, callOptions, new HelloRequest());

            var exTask = ExceptionAssert.ThrowsAsync<RpcException>(() => call.ResponseAsync);

            call.Dispose();

            var ex = await exTask.DefaultTimeout();

            Assert.AreEqual(StatusCode.Cancelled, ex.StatusCode);
            Assert.AreEqual("gRPC call disposed.", ex.Status.Detail);
        }

        private class TestAddressResolverFactory : AddressResolverFactory
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

        private class TestAddressResolver : AddressResolver, IDisposable
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

        private class TestSubChannelTransportFactory : ISubChannelTransportFactory
        {
            public ISubChannelTransport Create(SubChannel subChannel)
            {
                return new TestSubChannelTransport(subChannel);
            }
        }

        private class TestSubChannelTransport : ISubChannelTransport
        {
            private readonly SubChannel _subChannel;

            public DnsEndPoint? CurrentEndPoint { get; private set; }

            public TestSubChannelTransport(SubChannel subChannel)
            {
                _subChannel = subChannel;
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
                CurrentEndPoint = _subChannel._addresses[0];
                _subChannel.UpdateConnectivityState(ConnectivityState.Ready);
                return new ValueTask<bool>(true);
            }
        }
    }
}

#endif
