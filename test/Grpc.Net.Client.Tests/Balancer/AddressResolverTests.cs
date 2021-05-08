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
using Grpc.Net.Client.Tests.Infrastructure.Balancer;
using System.Diagnostics;
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

            var addressResolver = new TestAddressResolver();
            addressResolver.UpdateEndPoints(new List<DnsEndPoint>
            {
                new DnsEndPoint("localhost", 80)
            });

            services.AddSingleton<AddressResolverFactory>(new TestAddressResolverFactory(addressResolver));
            services.AddSingleton<ISubChannelTransportFactory>(new TestSubChannelTransportFactory());

            var channelOptions = new GrpcChannelOptions
            {
                Credentials = ChannelCredentials.Insecure,
                ServiceProvider = services.BuildServiceProvider()
            };

            // Act
            var channel = GrpcChannel.ForAddress("test://localhost", channelOptions);
            await channel.ConnectAsync();

            // Assert
            var subChannels = channel.ClientChannel.GetSubChannels();
            Assert.AreEqual(1, subChannels.Count);
        }

        [Test]
        public async Task ChangeAddresses_PickFirst_SubChannelAddressesUpdated()
        {
            // Arrange
            var services = new ServiceCollection();

            var addressResolver = new TestAddressResolver();
            addressResolver.UpdateEndPoints(new List<DnsEndPoint>
            {
                new DnsEndPoint("localhost", 80)
            });

            services.AddSingleton<AddressResolverFactory>(new TestAddressResolverFactory(addressResolver));
            services.AddSingleton<ISubChannelTransportFactory>(new TestSubChannelTransportFactory());

            var channelOptions = new GrpcChannelOptions
            {
                Credentials = ChannelCredentials.Insecure,
                ServiceProvider = services.BuildServiceProvider()
            };

            // Act
            var channel = GrpcChannel.ForAddress("test://localhost", channelOptions);
            await channel.ConnectAsync();

            // Assert
            var subChannels = channel.ClientChannel.GetSubChannels();
            Assert.AreEqual(1, subChannels.Count);

            Assert.AreEqual(1, subChannels[0]._addresses.Count);
            Assert.AreEqual(new DnsEndPoint("localhost", 80), subChannels[0]._addresses[0]);
            Assert.AreEqual(ConnectivityState.Ready, subChannels[0].State);

            addressResolver.UpdateEndPoints(new List<DnsEndPoint>
            {
                new DnsEndPoint("localhost", 81)
            });

            var newSubChannels = channel.ClientChannel.GetSubChannels();
            CollectionAssert.AreEqual(subChannels, newSubChannels);

            Assert.AreEqual(1, subChannels[0]._addresses.Count);
            Assert.AreEqual(new DnsEndPoint("localhost", 81), subChannels[0]._addresses[0]);
            Assert.AreEqual(ConnectivityState.Ready, subChannels[0].State);
        }

        [Test]
        public async Task ChangeAddresses_RoundRobin_OldSubChannelDisconnected()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddLogging(b => b.AddProvider(new NUnitLoggerProvider()));

            var addressResolver = new TestAddressResolver();
            addressResolver.UpdateEndPoints(new List<DnsEndPoint>
            {
                new DnsEndPoint("localhost", 80)
            });

            var transportFactory = new TestSubChannelTransportFactory();
            services.AddSingleton<AddressResolverFactory>(new TestAddressResolverFactory(addressResolver));
            services.AddSingleton<ISubChannelTransportFactory>(transportFactory);

            var channelOptions = new GrpcChannelOptions
            {
                Credentials = ChannelCredentials.Insecure,
                ServiceConfig = new ServiceConfig { LoadBalancingConfigs = { new RoundRobinConfig() } },
                ServiceProvider = services.BuildServiceProvider()
            };

            // Act
            var channel = GrpcChannel.ForAddress("test://localhost", channelOptions);
            await channel.ConnectAsync().DefaultTimeout();

            // Assert
            var subChannels = channel.ClientChannel.GetSubChannels();
            Assert.AreEqual(1, subChannels.Count);

            Assert.AreEqual(1, subChannels[0]._addresses.Count);
            Assert.AreEqual(new DnsEndPoint("localhost", 80), subChannels[0]._addresses[0]);

            // Wait for TryConnect to be called so state is connected
            await transportFactory.Transports.Single().TryConnectTask.DefaultTimeout();
            Assert.AreEqual(ConnectivityState.Ready, subChannels[0].State);

            addressResolver.UpdateEndPoints(new List<DnsEndPoint>
            {
                new DnsEndPoint("localhost", 81)
            });
            Assert.AreEqual(ConnectivityState.Shutdown, subChannels[0].State);

            var newSubChannels = channel.ClientChannel.GetSubChannels();
            CollectionAssert.AreNotEqual(subChannels, newSubChannels);
            Assert.AreEqual(1, newSubChannels.Count);

            Assert.AreEqual(1, newSubChannels[0]._addresses.Count);
            Assert.AreEqual(new DnsEndPoint("localhost", 81), newSubChannels[0]._addresses[0]);

            await channel.ClientChannel.PickAsync(new PickContext(new HttpRequestMessage(), waitForReady: false), CancellationToken.None).AsTask().DefaultTimeout();
            Assert.AreEqual(ConnectivityState.Ready, newSubChannels[0].State);
        }

        [Test]
        public async Task AddressResolver_WaitForRefreshAsync_Success()
        {
            // Arrange
            var services = new ServiceCollection();
            var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);

            var addressResolver = new TestAddressResolver(tcs.Task);
            addressResolver.UpdateEndPoints(new List<DnsEndPoint>
            {
                new DnsEndPoint("localhost", 80)
            });

            services.AddSingleton<AddressResolverFactory>(new TestAddressResolverFactory(addressResolver));
            services.AddSingleton<ISubChannelTransportFactory>(new TestSubChannelTransportFactory());

            var channelOptions = new GrpcChannelOptions
            {
                Credentials = ChannelCredentials.Insecure,
                ServiceProvider = services.BuildServiceProvider()
            };

            // Act
            var channel = GrpcChannel.ForAddress("test://localhost", channelOptions);
            var connectTask = channel.ConnectAsync();

            // Assert
            Assert.IsFalse(connectTask.IsCompleted);

            tcs.SetResult(null);

            await connectTask.DefaultTimeout();

            var subChannels = channel.ClientChannel.GetSubChannels();
            Assert.AreEqual(1, subChannels.Count);
        }
    }
}

#endif
