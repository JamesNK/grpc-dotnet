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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client.Balancer;
using Grpc.Net.Client.Balancer.Internal;
using Grpc.Net.Client.Configuration;
using Grpc.Net.Client.Tests.Infrastructure;
using Grpc.Net.Client.Tests.Infrastructure.Balancer;
using Grpc.Tests.Shared;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging.Testing;
using NUnit.Framework;

namespace Grpc.Net.Client.Tests.Balancer;

[TestFixture]
public class ResolverTests
{
    [Test]
    public async Task Refresh_BlockInsideResolveAsync_ResolverNotBlocked()
    {
        // Arrange
        var waitHandle = new ManualResetEvent(false);

        var services = new ServiceCollection();
        var testSink = new TestSink();
        services.AddLogging(b =>
        {
            b.AddProvider(new TestLoggerProvider(testSink));
        });
        services.AddNUnitLogger();
        var loggerFactory = services.BuildServiceProvider().GetRequiredService<ILoggerFactory>();

        var logger = loggerFactory.CreateLogger<ResolverTests>();
        logger.LogInformation("Starting.");

        var lockingResolver = new LockingPollingResolver(loggerFactory, waitHandle);
        lockingResolver.Start(result => { });

        // Act
        logger.LogInformation("Refresh call 1. This should block.");
        var refreshTask1 = Task.Run(lockingResolver.Refresh);

        logger.LogInformation("Refresh call 2. This should complete.");
        var refreshTask2 = Task.Run(lockingResolver.Refresh);

        // Assert
        await Task.WhenAny(refreshTask1, refreshTask2).DefaultTimeout();

        logger.LogInformation("Setting wait handle.");
        waitHandle.Set();

        logger.LogInformation("Finishing.");
    }

    private class LockingPollingResolver : PollingResolver
    {
        private ManualResetEvent? _waitHandle;
        private readonly object _lock = new();

        public LockingPollingResolver(ILoggerFactory loggerFactory, ManualResetEvent waitHandle) : base(loggerFactory)
        {
            _waitHandle = waitHandle;
        }

        protected override Task ResolveAsync(CancellationToken cancellationToken)
        {
            lock (_lock)
            {
                // Block the first caller.
                if (_waitHandle != null)
                {
                    _waitHandle.WaitOne();
                    _waitHandle = null;
                }
            }

            Listener(ResolverResult.ForResult(new List<BalancerAddress>
                {
                    new BalancerAddress("localhost", 80)
                }));

            return Task.CompletedTask;
        }
    }

    [Test]
    public async Task Resolver_ResolveNameFromServices_Success()
    {
        // Arrange
        var services = new ServiceCollection();

        var resolver = new TestResolver();
        resolver.UpdateAddresses(new List<BalancerAddress>
        {
            new BalancerAddress("localhost", 80)
        });

        services.AddSingleton<ResolverFactory>(new TestResolverFactory(resolver));
        services.AddSingleton<ISubchannelTransportFactory>(new TestSubchannelTransportFactory());

        var handler = new TestHttpMessageHandler((r, ct) => default!);
        var channelOptions = new GrpcChannelOptions
        {
            Credentials = ChannelCredentials.Insecure,
            ServiceProvider = services.BuildServiceProvider(),
            HttpHandler = handler
        };

        // Act
        var channel = GrpcChannel.ForAddress("test:///localhost", channelOptions);
        await channel.ConnectAsync();

        // Assert
        var subchannels = channel.ConnectionManager.GetSubchannels();
        Assert.AreEqual(1, subchannels.Count);
    }

    [Test]
    public async Task Resolver_WaitForRefreshAsync_Success()
    {
        // Arrange
        var services = new ServiceCollection();
        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);

        var resolver = new TestResolver(NullLoggerFactory.Instance, () => tcs.Task);
        resolver.UpdateAddresses(new List<BalancerAddress>
        {
            new BalancerAddress("localhost", 80)
        });

        services.AddSingleton<ResolverFactory>(new TestResolverFactory(resolver));
        services.AddSingleton<ISubchannelTransportFactory>(new TestSubchannelTransportFactory());

        var handler = new TestHttpMessageHandler((r, ct) => default!);
        var channelOptions = new GrpcChannelOptions
        {
            Credentials = ChannelCredentials.Insecure,
            ServiceProvider = services.BuildServiceProvider(),
            HttpHandler = handler
        };

        // Act
        var channel = GrpcChannel.ForAddress("test:///localhost", channelOptions);
        var connectTask = channel.ConnectAsync();

        // Assert
        Assert.IsFalse(connectTask.IsCompleted);

        tcs.SetResult(null);

        await connectTask.DefaultTimeout();

        var subchannels = channel.ConnectionManager.GetSubchannels();
        Assert.AreEqual(1, subchannels.Count);
    }

    [Test]
    public async Task Resolver_NoServiceConfigInResult_LoadBalancerUnchanged()
    {
        await Resolver_ServiceConfigInResult(resolvedServiceConfig: null);
    }

    [Test]
    public async Task Resolver_EmptyServiceConfigInResult_LoadBalancerUnchanged()
    {
        await Resolver_ServiceConfigInResult(resolvedServiceConfig: new ServiceConfig());
    }

    [Test]
    public async Task Resolver_MatchingPolicyInResult_LoadBalancerUnchanged()
    {
        await Resolver_ServiceConfigInResult(resolvedServiceConfig: new ServiceConfig
        {
            LoadBalancingConfigs = { new PickFirstConfig() }
        });
    }

    [Test]
    public void ResolverResult_OkStatusWithNoResolver_Error()
    {
        Assert.Throws<ArgumentException>(() => ResolverResult.ForResult(new List<BalancerAddress> { new BalancerAddress("localhost", 80) }, null, Status.DefaultSuccess));
    }

    private async Task Resolver_ServiceConfigInResult(ServiceConfig? resolvedServiceConfig)
    {
        // Arrange
        var services = new ServiceCollection();
        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);

        var resolver = new TestResolver(NullLoggerFactory.Instance, () => tcs.Task);
        var result = ResolverResult.ForResult(new List<BalancerAddress> { new BalancerAddress("localhost", 80) }, resolvedServiceConfig, serviceConfigStatus: null);
        resolver.UpdateResult(result);

        var createdCount = 0;
        var test = new TestLoadBalancerFactory(
            LoadBalancingConfig.PickFirstPolicyName,
            c =>
            {
                createdCount++;
                return new PickFirstBalancer(c, NullLoggerFactory.Instance);
            });

        services.AddSingleton<ResolverFactory>(new TestResolverFactory(resolver));
        services.AddSingleton<ISubchannelTransportFactory>(new TestSubchannelTransportFactory());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<LoadBalancerFactory>(test));

        var handler = new TestHttpMessageHandler((r, ct) => default!);
        var channelOptions = new GrpcChannelOptions
        {
            Credentials = ChannelCredentials.Insecure,
            ServiceProvider = services.BuildServiceProvider(),
            HttpHandler = handler
        };

        // Act
        var channel = GrpcChannel.ForAddress("test:///localhost", channelOptions);
        var connectTask = channel.ConnectAsync();

        // Assert
        Assert.IsFalse(connectTask.IsCompleted);

        tcs.SetResult(null);

        await connectTask.DefaultTimeout();

        var subchannels = channel.ConnectionManager.GetSubchannels();
        Assert.AreEqual(1, subchannels.Count);

        Assert.AreEqual(1, createdCount);

        resolver.UpdateResult(result);

        Assert.AreEqual(1, createdCount);
    }

    [Test]
    public async Task Resolver_ServiceConfigInResult()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNUnitLogger();
        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);

        var resolver = new TestResolver(NullLoggerFactory.Instance, () => tcs.Task);
        var result = ResolverResult.ForResult(new List<BalancerAddress> { new BalancerAddress("localhost", 80) }, serviceConfig: null, serviceConfigStatus: null);
        resolver.UpdateResult(result);

        TestLoadBalancer? firstLoadBalancer = null;
        var firstLoadBalancerCreatedCount = 0;
        var firstLoadBalancerFactory = new TestLoadBalancerFactory(
            LoadBalancingConfig.PickFirstPolicyName,
            c =>
            {
                firstLoadBalancerCreatedCount++;
                firstLoadBalancer = new TestLoadBalancer(new PickFirstBalancer(c, NullLoggerFactory.Instance));
                return firstLoadBalancer;
            });

        TestLoadBalancer? secondLoadBalancer = null;
        var secondLoadBalancerCreatedCount = 0;
        var secondLoadBalancerFactory = new TestLoadBalancerFactory(
            "custom",
            c =>
            {
                secondLoadBalancerCreatedCount++;
                secondLoadBalancer = new TestLoadBalancer(new PickFirstBalancer(c, NullLoggerFactory.Instance));
                return secondLoadBalancer;
            });

        var syncPoint = new SyncPoint(runContinuationsAsynchronously: true);
        var currentConnectivityState = ConnectivityState.Ready;

        services.AddSingleton<ResolverFactory>(new TestResolverFactory(resolver));
        services.AddSingleton<ISubchannelTransportFactory>(new TestSubchannelTransportFactory(async (s, c) =>
        {
            await syncPoint.WaitToContinue();
            return new TryConnectResult(currentConnectivityState);
        }));
        services.Add(ServiceDescriptor.Singleton<LoadBalancerFactory>(firstLoadBalancerFactory));
        services.Add(ServiceDescriptor.Singleton<LoadBalancerFactory>(secondLoadBalancerFactory));
        var serviceProvider = services.BuildServiceProvider();
        var logger = serviceProvider.GetRequiredService<ILoggerProvider>().CreateLogger(GetType().FullName!);

        var handler = new TestHttpMessageHandler((r, ct) => default!);
        var channelOptions = new GrpcChannelOptions
        {
            Credentials = ChannelCredentials.Insecure,
            ServiceProvider = serviceProvider,
            HttpHandler = handler
        };

        // Act
        var channel = GrpcChannel.ForAddress("test:///localhost", channelOptions);
        var connectTask = channel.ConnectAsync();

        // Assert
        Assert.IsFalse(connectTask.IsCompleted);

        tcs.SetResult(null);

        logger.LogInformation("Ensure that channel has processed results.");
        await resolver.HasResolvedTask.DefaultTimeout();

        var subchannels = channel.ConnectionManager.GetSubchannels();
        Assert.AreEqual(1, subchannels.Count);
        Assert.AreEqual(ConnectivityState.Connecting, subchannels[0].State);

        Assert.AreEqual(1, firstLoadBalancerCreatedCount);

        syncPoint!.Continue();

        var pick = await channel.ConnectionManager.PickAsync(new PickContext(), true, CancellationToken.None).AsTask().DefaultTimeout();
        Assert.AreEqual(80, pick.Address.EndPoint.Port);

        logger.LogInformation("Create new SyncPoint so new load balancer is waiting to connect.");
        syncPoint = new SyncPoint(runContinuationsAsynchronously: false);

        result = ResolverResult.ForResult(
            new List<BalancerAddress> { new BalancerAddress("localhost", 81) },
            serviceConfig: new ServiceConfig
            {
                LoadBalancingConfigs = { new LoadBalancingConfig("custom") }
            },
            serviceConfigStatus: null);
        resolver.UpdateResult(result);

        Assert.AreEqual(1, firstLoadBalancerCreatedCount);
        Assert.AreEqual(1, secondLoadBalancerCreatedCount);

        logger.LogInformation("Old address is still used because new load balancer is connecting.");
        pick = await channel.ConnectionManager.PickAsync(new PickContext(), true, CancellationToken.None).AsTask().DefaultTimeout();
        Assert.AreEqual(80, pick.Address.EndPoint.Port);

        Assert.IsFalse(firstLoadBalancer!.Disposed);

        logger.LogInformation("Allow sync point to continue and new load balancer to finish connecting.");
        syncPoint!.Continue();

        logger.LogInformation("Wait for ready subchannel to come from the new resolver.");
        await BalancerWaitHelpers.WaitForSubchannelToBeReadyAsync(logger, channel, validateSubchannel: s => s.CurrentAddress?.EndPoint.Port == 81);

        logger.LogInformation("New address is used.");
        pick = await channel.ConnectionManager.PickAsync(new PickContext(), true, CancellationToken.None).AsTask().DefaultTimeout();
        Assert.AreEqual(81, pick.Address.EndPoint.Port);

        Assert.IsTrue(firstLoadBalancer!.Disposed);

        await connectTask.DefaultTimeout();
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task ResolverOptions_ResolveServiceConfig_LoadBalancerChangedIfNotDisabled(bool disabled)
    {
        // Arrange
        var services = new ServiceCollection();

        var resolver = new TestResolver();
        resolver.UpdateAddresses(
            new List<BalancerAddress> { new BalancerAddress("localhost", 80) },
            new ServiceConfig
            {
                LoadBalancingConfigs = { new RoundRobinConfig() }
            });

        ResolverOptions? resolverOptions = null;
        services.AddSingleton<ResolverFactory>(new TestResolverFactory(o =>
        {
            resolverOptions = o;
            return resolver;
        }));
        services.AddSingleton<ISubchannelTransportFactory>(new TestSubchannelTransportFactory());

        var handler = new TestHttpMessageHandler((r, ct) => default!);
        var channelOptions = new GrpcChannelOptions
        {
            Credentials = ChannelCredentials.Insecure,
            ServiceProvider = services.BuildServiceProvider(),
            HttpHandler = handler,
            DisableResolverServiceConfig = disabled
        };

        // Act
        var channel = GrpcChannel.ForAddress("test:///localhost", channelOptions);
        await channel.ConnectAsync();

        // Assert
        var subchannels = channel.ConnectionManager.GetSubchannels();
        Assert.AreEqual(1, subchannels.Count);

        Assert.AreEqual(disabled, resolverOptions!.DisableServiceConfig);

        if (disabled)
        {
            Assert.IsNotNull(GetInnerLoadBalancer<PickFirstBalancer>(channel));
        }
        else
        {
            Assert.IsNotNull(GetInnerLoadBalancer<RoundRobinBalancer>(channel));
        }
    }

    [Test]
    public async Task ResolveServiceConfig_UnknownPolicyName_LoadBalancerUnchanged()
    {
        // Arrange
        var services = new ServiceCollection();

        var resolver = new TestResolver();
        resolver.UpdateAddresses(
            new List<BalancerAddress> { new BalancerAddress("localhost", 80) },
            new ServiceConfig
            {
                LoadBalancingConfigs = { new LoadBalancingConfig("unknown!") }
            });

        services.AddSingleton<ResolverFactory>(new TestResolverFactory(resolver));
        services.AddSingleton<ISubchannelTransportFactory>(new TestSubchannelTransportFactory());

        var handler = new TestHttpMessageHandler((r, ct) => default!);
        var channelOptions = new GrpcChannelOptions
        {
            Credentials = ChannelCredentials.Insecure,
            ServiceProvider = services.BuildServiceProvider(),
            HttpHandler = handler
        };

        // Act
        var channel = GrpcChannel.ForAddress("test:///localhost", channelOptions);
        await channel.ConnectAsync();

        // Assert
        Assert.IsNotNull(GetInnerLoadBalancer<PickFirstBalancer>(channel));
    }

    [Test]
    public async Task ResolveServiceConfig_ErrorOnFirstResolve_PickError()
    {
        // Arrange
        var services = new ServiceCollection();

        var resolver = new TestResolver();
        resolver.UpdateAddresses(
            new List<BalancerAddress> { new BalancerAddress("localhost", 80) },
            serviceConfig: null,
            serviceConfigStatus: new Status(StatusCode.Internal, "An error!"));

        services.AddSingleton<ResolverFactory>(new TestResolverFactory(resolver));
        services.AddSingleton<ISubchannelTransportFactory>(new TestSubchannelTransportFactory());

        var handler = new TestHttpMessageHandler((r, ct) => default!);
        var channelOptions = new GrpcChannelOptions
        {
            Credentials = ChannelCredentials.Insecure,
            ServiceProvider = services.BuildServiceProvider(),
            HttpHandler = handler
        };

        // Act
        var channel = GrpcChannel.ForAddress("test:///localhost", channelOptions);
        await channel.ConnectionManager.ConnectAsync(waitForReady: false, CancellationToken.None).DefaultTimeout();

        var ex = await ExceptionAssert.ThrowsAsync<RpcException>(async () =>
        {
            await channel.ConnectionManager.PickAsync(new PickContext(), waitForReady: false, CancellationToken.None);
        }).DefaultTimeout();

        // Assert
        Assert.AreEqual(StatusCode.Internal, ex.StatusCode);
        Assert.AreEqual("An error!", ex.Status.Detail);
    }

    [Test]
    public async Task ResolveServiceConfig_ErrorOnSecondResolve_PickSuccess()
    {
        // Arrange
        var services = new ServiceCollection();

        var resolver = new TestResolver();
        resolver.UpdateAddresses(
            new List<BalancerAddress> { new BalancerAddress("localhost", 80) },
            serviceConfig: null,
            serviceConfigStatus: null);

        services.AddSingleton<ResolverFactory>(new TestResolverFactory(resolver));
        services.AddSingleton<ISubchannelTransportFactory>(new TestSubchannelTransportFactory());

        var testSink = new TestSink();
        services.AddLogging(b =>
        {
            b.AddProvider(new TestLoggerProvider(testSink));
        });
        services.AddNUnitLogger();

        var handler = new TestHttpMessageHandler((r, ct) => default!);
        var channelOptions = new GrpcChannelOptions
        {
            Credentials = ChannelCredentials.Insecure,
            ServiceProvider = services.BuildServiceProvider(),
            HttpHandler = handler
        };

        // Act
        var channel = GrpcChannel.ForAddress("test:///localhost", channelOptions);
        await channel.ConnectionManager.ConnectAsync(waitForReady: false, CancellationToken.None).DefaultTimeout();

        resolver.UpdateAddresses(
            new List<BalancerAddress> { new BalancerAddress("localhost", 80) },
            serviceConfig: null,
            serviceConfigStatus: new Status(StatusCode.Internal, "An error!"));

        var result = await channel.ConnectionManager.PickAsync(new PickContext(), waitForReady: false, CancellationToken.None);

        // Assert
        Assert.AreEqual("localhost", result.Address.EndPoint.Host);
        Assert.AreEqual(80, result.Address.EndPoint.Port);

        var pickStartedCount = testSink.Writes.Count(w => w.EventId.Name == "ResolverServiceConfigFallback");
        Assert.AreEqual(1, pickStartedCount);
    }

    public static T? GetInnerLoadBalancer<T>(GrpcChannel channel) where T : LoadBalancer
    {
        var balancer = (ChildHandlerLoadBalancer)channel.ConnectionManager._balancer!;
        return (T?)balancer._current?.LoadBalancer;
    }

    [Test]
    public async Task PickAsync_ErrorWhenInParallelWithUpdateChannelState()
    {
        // Arrange
        var services = new ServiceCollection();

        // add logger
        services.AddNUnitLogger();
        var loggerFactory = services.BuildServiceProvider().GetRequiredService<ILoggerFactory>();
        var logger = loggerFactory.CreateLogger<ResolverTests>();

        // add resolver and balancer
        services.AddSingleton<ResolverFactory, CustomResolverFactory>();
        services.AddSingleton<LoadBalancerFactory, CustomBalancerFactory>();

        var connectionStep = 0;
        services.AddSingleton<ISubchannelTransportFactory>(
            new TestSubchannelTransportFactory((_, _) =>
            {
                // the first call should return `Ready` connection but then one of connections should be broken
                // (as it happens in real life)
                var connectivityState = connectionStep switch
                {
                    > 0 => Random.Shared.NextDouble() switch
                        {
                            // use 0.7 probability of broken connection just to make the bug occur sooner
                            < 0.7 => ConnectivityState.TransientFailure,
                            _ => ConnectivityState.Ready
                        },
                    _ => ConnectivityState.Ready
                };

                Interlocked.Increment(ref connectionStep);

                return Task.FromResult(new TryConnectResult(connectivityState));
            }));

        var channelOptions = new GrpcChannelOptions
        {
            Credentials = ChannelCredentials.Insecure,
            ServiceProvider = services.BuildServiceProvider(),
        };

        // Act & Assert
        var channel = GrpcChannel.ForAddress("test:///test_addr", channelOptions);
        await channel.ConnectionManager.ConnectAsync(waitForReady: false, CancellationToken.None);

        // the point is to perform a lot of `picks` to catch `PickAsync()` and `UpdateChannelState()` simultaneous
        // execution when ` _balancer.UpdateChannelState(state)` runs after `GetPickerAsync()` internals but before
        // the task it returned completed
        var pickAsyncTask = Task.Run(async () =>
        {
            var counter = 0;
            var exceptionsCounter = 0;

            while (counter < 1000)
            {
                // short delay
                await Task.Delay(20);

                try
                {
                    // I used counter to verify that exception is not always occurs at the first or the second
                    // (or any other) predefined step
                    counter++;

                    var (subchannel, address, _) = await channel.ConnectionManager.PickAsync(
                        new PickContext(),
                        waitForReady: false,
                        CancellationToken.None);

                    logger.LogInformation(
                        "[ {Counter} ] PickAsync result: subchannel = `{Subchannel}`, address = `{Address}`",
                        counter,
                        subchannel,
                        address);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "[ {Counter} ] PickAsync Error", counter);

                    exceptionsCounter++;
                    switch (exceptionsCounter)
                    {
                        // in real life renews are not too often so we stop renews here to show that this resolver
                        // state is broken and will forever throw exceptions (at least until the next renew came)
                        case 1:
                            CustomResolver.StopRenew();
                            break;

                        // restart renew to show that renews to _different_ addresses can fix errors
                        case > 5 and < 50:
                            CustomResolver.RestartRenew();
                            break;

                        case > 50:
                            throw;
                    }
                }
            }
        });

        // in some time this will definitely fail
        await pickAsyncTask;
    }
}
#endif
