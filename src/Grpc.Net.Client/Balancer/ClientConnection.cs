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
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Balancer
{
    public abstract class ClientConnection
    {
        private ConnectivityState _state;

        // Internal for testing
        internal SubConnectionPicker? _picker;
        private TaskCompletionSource<SubConnectionPicker> _nextPickerTcs;
        private readonly SemaphoreSlim _nextPickerLock;
        private readonly object _lock;

        protected ClientConnection(ILoggerFactory loggerFactory)
        {
            _lock = new object();
            _nextPickerLock = new SemaphoreSlim(1);
            _nextPickerTcs = new TaskCompletionSource<SubConnectionPicker>(TaskCreationOptions.RunContinuationsAsynchronously);

            Logger = loggerFactory.CreateLogger(GetType());
        }

        public ConnectivityState State => _state;

        public ILogger Logger { get; }
        public abstract SubConnection CreateSubConnection(SubConnectionOptions options);
        public abstract void RemoveSubConnection(SubConnection subConnection);
        public abstract void UpdateAddresses(SubConnection subConnection, IReadOnlyList<DnsEndPoint> addresses);
        public abstract Task ResolveNowAsync(CancellationToken cancellationToken);
        public abstract Task ConnectAsync(CancellationToken cancellationToken);

        public virtual void UpdateState(BalancerState state)
        {
            lock (_lock)
            {
                if (_state != state.ConnectivityState)
                {
                    Logger.LogInformation("Connection state updated: " + state.ConnectivityState);
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
            SubConnectionPicker? previousPicker = null;

            PickResult result;

            // Wait for a valid picker. When the client state changes a new picker will be returned.
            // Cancellation will break out of the loop. Typically cancellation will come from a
            // deadline specified for a call being exceeded.
            while (true)
            {
                var currentPicker = await GetPickerAsync(previousPicker, cancellationToken).ConfigureAwait(false);

                result = currentPicker.Pick(context);

                if (result.SubConnection != null)
                {
                    break;
                }
                else
                {
                    Logger.LogInformation("Current picker doesn't have a ready connection");
                    previousPicker = currentPicker;
                }
            }

            Logger.LogInformation("Successfully picked sub-connection: " + result.SubConnection);

            if (result.SubConnection.CurrentEndPoint == null)
            {
                await result.SubConnection.ConnectAsync(cancellationToken).ConfigureAwait(false);
            }

            return result;
        }

        private ValueTask<SubConnectionPicker> GetPickerAsync(SubConnectionPicker? currentPicker, CancellationToken cancellationToken)
        {
            lock (_lock)
            {
                if (_picker != null && _picker != currentPicker)
                {
                    return ValueTask.FromResult(_picker);
                }
                else
                {
                    return GetNextPickerAsync(cancellationToken);
                }
            }
        }

        private async ValueTask<SubConnectionPicker> GetNextPickerAsync(CancellationToken cancellationToken)
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
                    _nextPickerTcs = new TaskCompletionSource<SubConnectionPicker>(TaskCreationOptions.RunContinuationsAsynchronously);
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