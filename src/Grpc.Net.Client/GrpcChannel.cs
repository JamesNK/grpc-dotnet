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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Grpc.Core;
using Grpc.Net.Client.Internal;
using Grpc.Net.Client.Configuration;
using GrpcServiceConfig = Grpc.Net.Client.Configuration.ServiceConfig;
using Grpc.Net.Compression;
using Grpc.Shared;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Grpc.Net.Client.Internal.Retry;

namespace Grpc.Net.Client
{
    /// <summary>
    /// Represents a gRPC channel. Channels are an abstraction of long-lived connections to remote servers.
    /// Client objects can reuse the same channel. Creating a channel is an expensive operation compared to invoking
    /// a remote call so in general you should reuse a single channel for as many calls as possible.
    /// </summary>
    public sealed class GrpcChannel : ChannelBase, IDisposable
    {
        internal const int DefaultMaxReceiveMessageSize = 1024 * 1024 * 4; // 4 MB

        private readonly ConcurrentDictionary<IMethod, GrpcMethodInfo> _methodInfoCache;
        private readonly Func<IMethod, GrpcMethodInfo> _createMethodInfoFunc;
        private readonly Dictionary<MethodKey, MethodConfig>? _serviceConfigMethods;
        // Internal for testing
        internal readonly HashSet<IDisposable> ActiveCalls;

        internal Uri Address { get; }
        internal HttpMessageInvoker HttpInvoker { get; }
        internal int? SendMaxMessageSize { get; }
        internal int? ReceiveMaxMessageSize { get; }
        internal ILoggerFactory LoggerFactory { get; }
        internal bool ThrowOperationCanceledOnCancellation { get; }
        internal bool? IsSecure { get; }
        internal List<CallCredentials>? CallCredentials { get; }
        internal Dictionary<string, ICompressionProvider> CompressionProviders { get; }
        internal string MessageAcceptEncoding { get; }
        internal bool Disposed { get; private set; }
        internal GrpcServiceConfig? ServiceConfig { get; }
        internal ChannelRetryThrottling? RetryThrottling { get; }

        // Options that are set in unit tests
        internal ISystemClock Clock = SystemClock.Instance;
        internal IOperatingSystem OperatingSystem = Internal.OperatingSystem.Instance;
        internal bool DisableClientDeadline;
        internal long MaxTimerDueTime = uint.MaxValue - 1; // Max System.Threading.Timer due time

        private bool _shouldDisposeHttpClient;

        internal GrpcChannel(Uri address, GrpcChannelOptions channelOptions) : base(address.Authority)
        {
            _methodInfoCache = new ConcurrentDictionary<IMethod, GrpcMethodInfo>();

            // Dispose the HTTP client/handler if...
            //   1. No client/handler was specified and so the channel created the client itself
            //   2. User has specified a client/handler and set DisposeHttpClient to true
            _shouldDisposeHttpClient = (channelOptions.HttpClient == null && channelOptions.HttpHandler == null)
                || channelOptions.DisposeHttpClient;

            Address = address;
            HttpInvoker = channelOptions.HttpClient ?? CreateInternalHttpInvoker(channelOptions.HttpHandler);
            SendMaxMessageSize = channelOptions.MaxSendMessageSize;
            ReceiveMaxMessageSize = channelOptions.MaxReceiveMessageSize;
            CompressionProviders = ResolveCompressionProviders(channelOptions.CompressionProviders);
            MessageAcceptEncoding = GrpcProtocolHelpers.GetMessageAcceptEncoding(CompressionProviders);
            LoggerFactory = channelOptions.LoggerFactory ?? NullLoggerFactory.Instance;
            ThrowOperationCanceledOnCancellation = channelOptions.ThrowOperationCanceledOnCancellation;
            _createMethodInfoFunc = CreateMethodInfo;
            ActiveCalls = new HashSet<IDisposable>();
            // TODO(JamesNK): Underlying service config data is not copied
            ServiceConfig = channelOptions.ServiceConfig != null ? new GrpcServiceConfig(channelOptions.ServiceConfig) : null;
            RetryThrottling = ServiceConfig?.RetryThrottling != null ? new ChannelRetryThrottling(ServiceConfig.RetryThrottling) : null;
            _serviceConfigMethods = (ServiceConfig != null) ? CreateServiceConfigMethods(ServiceConfig) : null;

            if (channelOptions.Credentials != null)
            {
                var configurator = new DefaultChannelCredentialsConfigurator();
                channelOptions.Credentials.InternalPopulateConfiguration(configurator, null);

                IsSecure = configurator.IsSecure;
                CallCredentials = configurator.CallCredentials;

                ValidateChannelCredentials();
            }
        }

        private static Dictionary<MethodKey, MethodConfig> CreateServiceConfigMethods(GrpcServiceConfig serviceConfig)
        {
            var configs = new Dictionary<MethodKey, MethodConfig>();
            for (var i = 0; i < serviceConfig.MethodConfigs.Count; i++)
            {
                var methodConfig = serviceConfig.MethodConfigs[i];
                for (var j = 0; j < methodConfig.Names.Count; j++)
                {
                    var name = methodConfig.Names[j];
                    var methodKey = new MethodKey(name.Service, name.Method);
                    if (configs.ContainsKey(methodKey))
                    {
                        throw new InvalidOperationException($"Duplicate method config found. Service: '{name.Service}', method: '{name.Method}'.");
                    }
                    configs[methodKey] = methodConfig;
                }
            }

            return configs;
        }

        private static HttpMessageInvoker CreateInternalHttpInvoker(HttpMessageHandler? handler)
        {
            // HttpMessageInvoker should always dispose handler if Disposed is called on it.
            // Decision to dispose invoker is controlled by _shouldDisposeHttpClient.
            if (handler == null)
            {
                handler = HttpHandlerFactory.CreatePrimaryHandler();
            }

#if NET5_0
            handler = HttpHandlerFactory.EnsureTelemetryHandler(handler);
#endif

            // Use HttpMessageInvoker instead of HttpClient because it is faster
            // and we don't need client's features.
            var httpInvoker = new HttpMessageInvoker(handler, disposeHandler: true);

            return httpInvoker;
        }

        internal void RegisterActiveCall(IDisposable grpcCall)
        {
            lock (ActiveCalls)
            {
                ActiveCalls.Add(grpcCall);
            }
        }

        internal void FinishActiveCall(IDisposable grpcCall)
        {
            lock (ActiveCalls)
            {
                ActiveCalls.Remove(grpcCall);
            }
        }

        internal GrpcMethodInfo GetCachedGrpcMethodInfo(IMethod method)
        {
            return _methodInfoCache.GetOrAdd(method, _createMethodInfoFunc);
        }

        private GrpcMethodInfo CreateMethodInfo(IMethod method)
        {
            var uri = new Uri(method.FullName, UriKind.Relative);
            var scope = new GrpcCallScope(method.Type, uri);
            var methodConfig = ResolveMethodConfig(method);

            return new GrpcMethodInfo(scope, new Uri(Address, uri), methodConfig);
        }

        private MethodConfig? ResolveMethodConfig(IMethod method)
        {
            if (_serviceConfigMethods != null)
            {
                MethodConfig? methodConfig;
                if (_serviceConfigMethods.TryGetValue(new MethodKey(method.ServiceName, method.Name), out methodConfig))
                {
                    return methodConfig;
                }
                if (_serviceConfigMethods.TryGetValue(new MethodKey(method.ServiceName, null), out methodConfig))
                {
                    return methodConfig;
                }
                if (_serviceConfigMethods.TryGetValue(new MethodKey(null, null), out methodConfig))
                {
                    return methodConfig;
                }
            }

            return null;
        }

        private static Dictionary<string, ICompressionProvider> ResolveCompressionProviders(IList<ICompressionProvider>? compressionProviders)
        {
            if (compressionProviders == null)
            {
                return GrpcProtocolConstants.DefaultCompressionProviders;
            }

            var resolvedCompressionProviders = new Dictionary<string, ICompressionProvider>(StringComparer.Ordinal);
            for (var i = 0; i < compressionProviders.Count; i++)
            {
                var compressionProvider = compressionProviders[i];
                if (!resolvedCompressionProviders.ContainsKey(compressionProvider.EncodingName))
                {
                    resolvedCompressionProviders.Add(compressionProvider.EncodingName, compressionProvider);
                }
            }

            return resolvedCompressionProviders;
        }

        private void ValidateChannelCredentials()
        {
            if (IsSecure != null)
            {
                if (IsSecure.Value && Address.Scheme == Uri.UriSchemeHttp)
                {
                    throw new InvalidOperationException($"Channel is configured with secure channel credentials and can't use a HttpClient with a '{Address.Scheme}' scheme.");
                }
                if (!IsSecure.Value && Address.Scheme == Uri.UriSchemeHttps)
                {
                    throw new InvalidOperationException($"Channel is configured with insecure channel credentials and can't use a HttpClient with a '{Address.Scheme}' scheme.");
                }
            }
        }

        /// <summary>
        /// Create a new <see cref="CallInvoker"/> for the channel.
        /// </summary>
        /// <returns>A new <see cref="CallInvoker"/>.</returns>
        public override CallInvoker CreateCallInvoker()
        {
            if (Disposed)
            {
                throw new ObjectDisposedException(nameof(GrpcChannel));
            }

            var invoker = new HttpClientCallInvoker(this);

            return invoker;
        }

        /// <summary>
        /// Creates a <see cref="GrpcChannel"/> for the specified address.
        /// </summary>
        /// <param name="address">The address the channel will use.</param>
        /// <returns>A new instance of <see cref="GrpcChannel"/>.</returns>
        public static GrpcChannel ForAddress(string address)
        {
            return ForAddress(address, new GrpcChannelOptions());
        }

        /// <summary>
        /// Creates a <see cref="GrpcChannel"/> for the specified address and configuration options.
        /// </summary>
        /// <param name="address">The address the channel will use.</param>
        /// <param name="channelOptions">The channel configuration options.</param>
        /// <returns>A new instance of <see cref="GrpcChannel"/>.</returns>
        public static GrpcChannel ForAddress(string address, GrpcChannelOptions channelOptions)
        {
            return ForAddress(new Uri(address), channelOptions);
        }

        /// <summary>
        /// Creates a <see cref="GrpcChannel"/> for the specified address.
        /// </summary>
        /// <param name="address">The address the channel will use.</param>
        /// <returns>A new instance of <see cref="GrpcChannel"/>.</returns>
        public static GrpcChannel ForAddress(Uri address)
        {
            return ForAddress(address, new GrpcChannelOptions());
        }

        /// <summary>
        /// Creates a <see cref="GrpcChannel"/> for the specified address and configuration options.
        /// </summary>
        /// <param name="address">The address the channel will use.</param>
        /// <param name="channelOptions">The channel configuration options.</param>
        /// <returns>A new instance of <see cref="GrpcChannel"/>.</returns>
        public static GrpcChannel ForAddress(Uri address, GrpcChannelOptions channelOptions)
        {
            if (address == null)
            {
                throw new ArgumentNullException(nameof(address));
            }

            if (channelOptions == null)
            {
                throw new ArgumentNullException(nameof(channelOptions));
            }

            if (channelOptions.HttpClient != null && channelOptions.HttpHandler != null)
            {
                throw new ArgumentException($"{nameof(GrpcChannelOptions.HttpClient)} and {nameof(GrpcChannelOptions.HttpHandler)} have been configured. " +
                    $"Only one HTTP caller can be specified.");
            }

            return new GrpcChannel(address, channelOptions);
        }

        /// <summary>
        /// Releases the resources used by the <see cref="GrpcChannel"/> class.
        /// Clients created with the channel can't be used after the channel is disposed.
        /// </summary>
        public void Dispose()
        {
            if (Disposed)
            {
                return;
            }

            lock (ActiveCalls)
            {
                if (ActiveCalls.Count > 0)
                {
                    // Disposing a call will remove it from ActiveCalls. Need to take a copy
                    // to avoid enumeration from being modified
                    var activeCallsCopy = ActiveCalls.ToArray();

                    foreach (var activeCall in activeCallsCopy)
                    {
                        activeCall.Dispose();
                    }
                }
            }

            if (_shouldDisposeHttpClient)
            {
                HttpInvoker.Dispose();
            }
            Disposed = true;
        }

        private struct MethodKey : IEquatable<MethodKey>
        {
            public MethodKey(string? service, string? method)
            {
                Service = service;
                Method = method;
            }

            public string? Service { get; }
            public string? Method { get; }

            public override bool Equals(object? obj) => obj is MethodKey n ? Equals(n) : false;

            public bool Equals(MethodKey other) => other.Service == Service && other.Method == Method;

            public override int GetHashCode() =>
                Service?.GetHashCode(StringComparison.Ordinal) ?? 0 ^
                Method?.GetHashCode(StringComparison.Ordinal) ?? 0;
        }
    }
}
