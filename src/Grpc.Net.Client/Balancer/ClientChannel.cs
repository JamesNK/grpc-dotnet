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

#if HAVE_LOAD_BALANCING
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client.Balancer.Internal;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Balancer
{
    public class ClientChannel : IDisposable, IChannelControlHelper
    {
        private ConnectivityState _state;
        private readonly AddressResolver _resolver;
        private readonly ILoggerFactory _loggerFactory;
        private IDisposable? _resolverSubscription;
        private List<SubChannel> _subChannels;

        // Internal for testing
        internal LoadBalancer? _balancer;
        internal SubChannelPicker? _picker;

        private TaskCompletionSource<SubChannelPicker> _nextPickerTcs;
        private readonly SemaphoreSlim _nextPickerLock;
        private readonly object _lock;

        public ClientChannel(AddressResolver resolver, ILoggerFactory loggerFactory)
        {
            _lock = new object();
            _nextPickerLock = new SemaphoreSlim(1);
            _nextPickerTcs = new TaskCompletionSource<SubChannelPicker>(TaskCreationOptions.RunContinuationsAsynchronously);

            Logger = loggerFactory.CreateLogger(GetType());

            _subChannels = new List<SubChannel>();
            _resolver = resolver;
            _loggerFactory = loggerFactory;
        }

        public ConnectivityState State => _state;

        public ILogger Logger { get; }
        public IList<SubChannel> GetSubChannels()
        {
            lock (_subChannels)
            {
                return _subChannels.ToArray();
            }
        }

        public void ConfigureBalancer(Func<IChannelControlHelper, LoadBalancer> configure)
        {
            _balancer = configure(this);
        }

        public SubChannel CreateSubChannel(SubChannelOptions options)
        {
            var subChannel = new SubChannel(this, options.Addresses);
            subChannel.Transport = new ActiveHealthTransport(subChannel);

            Logger.LogInformation("Created sub-channel: " + subChannel);

            lock (_subChannels)
            {
                _subChannels.Add(subChannel);
            }

            return subChannel;
        }

        public void RemoveSubChannel(SubChannel subChannel)
        {
            Logger.LogInformation("Removing sub-channel: " + subChannel);

            lock (_subChannels)
            {
                var removed = _subChannels.Remove(subChannel);
                Debug.Assert(removed);
            }

            subChannel.Shutdown();
        }

        public Task ResolveNowAsync(CancellationToken cancellationToken)
        {
            return _resolver.RefreshAsync(cancellationToken);
        }

        public void UpdateAddresses(SubChannel subConnection, IReadOnlyList<DnsEndPoint> addresses)
        {
            subConnection.UpdateAddresses(addresses);
        }

        private void OnResolverError(Exception error)
        {
            throw new NotImplementedException();
        }

        private void OnResolverResult(AddressResolverResult value)
        {
            _balancer!.UpdateChannelState(new ChannelState(value, GrpcAttributes.Empty));
        }

        public void Dispose()
        {
            _resolverSubscription?.Dispose();
            _balancer?.Dispose();
        }

        internal void OnSubConnectionStateChange(SubChannel subConnection, ConnectivityState state)
        {
            Logger.LogInformation("Sub-connection state change: " + subConnection + " " + state);
            _balancer!.UpdateSubChannelState(subConnection, new SubChannelState { ConnectivityState = state });
        }

        public Task ConnectAsync(CancellationToken cancellationToken)
        {
            if (_resolverSubscription == null)
            {
                // Default to PickFirstBalancer
                if (_balancer == null)
                {
                    _balancer = new PickFirstBalancer(this, _loggerFactory);
                }

                _resolverSubscription = _resolver.Subscribe(new ResolverObserver(this));
            }

            return Task.CompletedTask;
        }

        private class ResolverObserver : IObserver<AddressResolverResult>
        {
            private readonly ClientChannel _channel;

            public ResolverObserver(ClientChannel channel)
            {
                _channel = channel;
            }

            public void OnCompleted()
            {
            }

            public void OnError(Exception error)
            {
                _channel.OnResolverError(error);
            }

            public void OnNext(AddressResolverResult value)
            {
                _channel.OnResolverResult(value);
            }
        }

        public virtual void UpdateState(BalancerState state)
        {
            lock (_lock)
            {
                if (_state != state.ConnectivityState)
                {
                    Logger.LogInformation("Channel state updated: " + state.ConnectivityState);
                    _state = state.ConnectivityState;
                }

                if (!Equals(_picker, state.Picker))
                {
                    Logger.LogInformation("Updating picker");
                    _picker = state.Picker;
                    _nextPickerTcs.TrySetResult(state.Picker);
                }
            }
        }

        public async ValueTask<PickResult> PickAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var context = new PickContext(request);
            SubChannelPicker? previousPicker = null;

            PickResult result;

            // Wait for a valid picker. When the client state changes a new picker will be returned.
            // Cancellation will break out of the loop. Typically cancellation will come from a
            // deadline specified for a call being exceeded.
            while (true)
            {
                var currentPicker = await GetPickerAsync(previousPicker, cancellationToken).ConfigureAwait(false);

                result = currentPicker.Pick(context);

                if (result.SubChannel != null)
                {
                    break;
                }
                else
                {
                    Logger.LogInformation("Current picker doesn't have a ready sub-channel");
                    previousPicker = currentPicker;
                }
            }

            Logger.LogInformation("Successfully picked sub-channel: " + result.SubChannel);

            if (result.SubChannel.CurrentEndPoint == null)
            {
                // For some reason the returned sub-channel doesn't have a current endpoint.
                // Connect the sub-channel to get 
                await result.SubChannel.ConnectAsync(cancellationToken).ConfigureAwait(false);
            }

            return result;
        }

        private ValueTask<SubChannelPicker> GetPickerAsync(SubChannelPicker? currentPicker, CancellationToken cancellationToken)
        {
            lock (_lock)
            {
                if (_picker != null && _picker != currentPicker)
                {
                    return new ValueTask<SubChannelPicker>(_picker);
                }
                else
                {
                    return GetNextPickerAsync(cancellationToken);
                }
            }
        }

        private async ValueTask<SubChannelPicker> GetNextPickerAsync(CancellationToken cancellationToken)
        {
            Logger.LogInformation("Waiting for valid picker");

            Debug.Assert(Monitor.IsEntered(_lock));

            var nextPickerTcs = _nextPickerTcs;

            await _nextPickerLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var nextPicker = await nextPickerTcs.Task.ConfigureAwait(false);

                lock (_lock)
                {
                    _nextPickerTcs = new TaskCompletionSource<SubChannelPicker>(TaskCreationOptions.RunContinuationsAsynchronously);
                }

                return nextPicker;
            }
            finally
            {
                _nextPickerLock.Release();
            }
        }
    }

}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif