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
        private readonly ISubChannelTransportFactory _subChannelTransportFactory;
        private IDisposable? _resolverSubscription;
        private List<SubChannel> _subChannels;

        // Internal for testing
        internal LoadBalancer? _balancer;
        internal SubChannelPicker? _picker;

        private TaskCompletionSource<SubChannelPicker> _nextPickerTcs;
        private readonly SemaphoreSlim _nextPickerLock;
        private readonly object _lock;

        public ClientChannel(AddressResolver resolver, ILoggerFactory loggerFactory)
            : this(resolver, loggerFactory, DefaultSubChannelTransportFactory.Instance)
        {
        }

        internal ClientChannel(AddressResolver resolver, ILoggerFactory loggerFactory, ISubChannelTransportFactory subChannelTransportFactory)
        {
            _lock = new object();
            _nextPickerLock = new SemaphoreSlim(1);
            _nextPickerTcs = new TaskCompletionSource<SubChannelPicker>(TaskCreationOptions.RunContinuationsAsynchronously);

            Logger = loggerFactory.CreateLogger(GetType());

            _subChannels = new List<SubChannel>();
            _resolver = resolver;
            _loggerFactory = loggerFactory;
            _subChannelTransportFactory = subChannelTransportFactory;
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
            subChannel.Transport = _subChannelTransportFactory.Create(subChannel);

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

            subChannel.Dispose();
        }

        public Task ResolveNowAsync(CancellationToken cancellationToken)
        {
            return _resolver.RefreshAsync(cancellationToken);
        }

        public void UpdateAddresses(SubChannel subChannel, IReadOnlyList<DnsEndPoint> addresses)
        {
            subChannel.UpdateAddresses(addresses);
        }

        private void OnResolverError(Exception error)
        {
            throw new NotImplementedException();
        }

        private void OnResolverResult(AddressResolverResult value)
        {
            lock (_lock)
            {
                _balancer!.UpdateChannelState(new ChannelState(value, GrpcAttributes.Empty));
            }
        }

        public void Dispose()
        {
            _resolverSubscription?.Dispose();
            _balancer?.Dispose();
        }

        internal void OnSubChannelStateChange(SubChannel subChannel, ConnectivityState state)
        {
            Logger.LogInformation("Sub-channel state change: " + subChannel + " " + state);
            _balancer!.UpdateSubChannelState(subChannel, new SubChannelState { ConnectivityState = state });
        }

        public async Task ConnectAsync(CancellationToken cancellationToken)
        {
            if (_resolverSubscription == null)
            {
                if (_balancer == null)
                {
                    throw new InvalidOperationException($"Load balancer not configured.");
                }

                _resolverSubscription = _resolver.Subscribe(new ResolverObserver(this));
                await _resolver.RefreshAsync(cancellationToken).ConfigureAwait(false);
            }
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
                    Logger.LogInformation("Updating picker: " + state.Picker);
                    _picker = state.Picker;
                    if (_nextPickerTcs.Task.IsCompleted)
                    {
                        _nextPickerTcs = new TaskCompletionSource<SubChannelPicker>(TaskCreationOptions.RunContinuationsAsynchronously);
                    }
                    _nextPickerTcs.SetResult(state.Picker);
                }
            }
        }

        public async
#if !NETSTANDARD2_0
            ValueTask<PickResult>
#else
            Task<PickResult>
#endif
            PickAsync(PickContext context, CancellationToken cancellationToken)
        {
            SubChannelPicker? previousPicker = null;

            PickResult result;

            // Wait for a valid picker. When the client state changes a new picker will be returned.
            // Cancellation will break out of the loop. Typically cancellation will come from a
            // deadline specified for a call being exceeded.
            while (true)
            {
                var currentPicker = await GetPickerAsync(previousPicker, cancellationToken).ConfigureAwait(false);
                Logger.LogInformation("Evaluating picker");

                try
                {
                    result = currentPicker.Pick(context);

                    if (result.SubChannel != null)
                    {
                        Logger.LogInformation($"Current picker has sub-channel {result.SubChannel} with end point {result.EndPoint}.");
                        break;
                    }
                    else
                    {
                        Logger.LogInformation("Current picker doesn't have a ready sub-channel");
                        previousPicker = currentPicker;
                    }
                }
                catch (OperationCanceledException)
                {
                    // Always throw on cancellation. Could be from call deadline or client dispose.
                    throw;
                }
                catch (Exception ex)
                {
                    if (context.WaitForReady)
                    {
                        Logger.LogInformation(ex, "Error picking. Retry because of wait for ready.");
                        previousPicker = currentPicker;
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            Logger.LogInformation("Successfully picked sub-channel: " + result.SubChannel + " with endpoint " + result.EndPoint);

            return result;
        }

        private
#if !NETSTANDARD2_0
            ValueTask<SubChannelPicker>
#else
            Task<SubChannelPicker>
#endif
            GetPickerAsync(SubChannelPicker? currentPicker, CancellationToken cancellationToken)
        {
            lock (_lock)
            {
                if (_picker != null && _picker != currentPicker)
                {
#if !NETSTANDARD2_0
                    return new ValueTask<SubChannelPicker>(_picker);
#else
                    return Task.FromResult<SubChannelPicker>(_picker);
#endif
                }
                else
                {
                    return GetNextPickerAsync(cancellationToken);
                }
            }
        }

        private async
#if !NETSTANDARD2_0
            ValueTask<SubChannelPicker>
#else
            Task<SubChannelPicker>
#endif
            GetNextPickerAsync(CancellationToken cancellationToken)
        {
            Logger.LogInformation("Waiting for valid picker");

            Debug.Assert(Monitor.IsEntered(_lock));

            var nextPickerTcs = _nextPickerTcs;

            await _nextPickerLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                using (cancellationToken.Register(s => ((TaskCompletionSource<SubChannelPicker?>)s!).TrySetCanceled(), nextPickerTcs))
                {
                    var nextPicker = await nextPickerTcs.Task.ConfigureAwait(false);

                    lock (_lock)
                    {
                        _nextPickerTcs = new TaskCompletionSource<SubChannelPicker>(TaskCreationOptions.RunContinuationsAsynchronously);
                    }

                    return nextPicker;
                }
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