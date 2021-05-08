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
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client.Balancer.Internal;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Balancer
{
    public class SubChannel : IDisposable
    {
        internal readonly List<DnsEndPoint> _addresses;
        internal ILogger Logger => _channel.Logger;
        internal readonly object Lock;
        internal ISubChannelTransport Transport { get; set; } = default!;

        private readonly ClientChannel _channel;

        private ConnectivityState _state;
        private Task? _connectTask;

        public DnsEndPoint? CurrentEndPoint => Transport.CurrentEndPoint;
        public ConnectivityState State => _state;

        internal SubChannel(ClientChannel channel, IReadOnlyList<DnsEndPoint> addresses)
        {
            Lock = new object();
            _addresses = addresses.ToList();
            _channel = channel;
        }

        public void UpdateAddresses(IReadOnlyList<DnsEndPoint> addresses)
        {
            var connect = false;
            lock (Lock)
            {
                _addresses.Clear();
                _addresses.AddRange(addresses);

                connect = (CurrentEndPoint != null && !_addresses.Contains(CurrentEndPoint));
            }
            if (connect)
            {
                _ = ConnectAsync(CancellationToken.None);
            }
        }

        public Task ConnectAsync(CancellationToken cancellationToken)
        {
            lock (Lock)
            {
                if (!IsConnectInProgressUnsynchronized)
                {
                    _connectTask = ConnectCoreAsync(cancellationToken);
                }
                else
                {
                    Logger.LogInformation("Connect already in progress " + this);
                }

                return _connectTask;
            }
        }

        private Task ConnectCoreAsync(CancellationToken cancellationToken)
        {
            Debug.Assert(Monitor.IsEntered(Lock));

            if (_state == ConnectivityState.Shutdown)
            {
                throw new InvalidOperationException("Sub-channel has been shutdown.");
            }

            if (_state != ConnectivityState.Idle)
            {
                return Task.CompletedTask;
            }

            // Start connect in Task.Run so it isn't in the lock.
            return Task.Run(async () =>
            {
                UpdateConnectivityState(ConnectivityState.Connecting);

                await ResetTransportAsync(cancellationToken).ConfigureAwait(false);
            });
        }

        private async Task ResetTransportAsync(CancellationToken cancellationToken)
        {
            for (var attempt = 0; ; attempt++)
            {
                if (attempt > 0)
                {
                    await _channel.ResolveNowAsync(cancellationToken).ConfigureAwait(false);
                }

                if (_state == ConnectivityState.Shutdown)
                {
                    return;
                }

                if (await Transport.TryConnectAsync(cancellationToken).ConfigureAwait(false))
                {
                    return;
                }
            }
        }

        [MemberNotNullWhen(true, nameof(_connectTask))]
        private bool IsConnectInProgressUnsynchronized
        {
            get
            {
                Debug.Assert(Monitor.IsEntered(Lock));

                var connectTask = _connectTask;
                return connectTask != null && !connectTask.IsCompleted;
            }
        }

        internal void UpdateConnectivityState(ConnectivityState state)
        {
            lock (Lock)
            {
                // Don't update sub-channel state if the state is the same or the sub-channel has been shutdown.
                //
                // This could happen when:
                // 1. Start trying to connect with a sub-channel.
                // 2. Address resolver updates and sub-channel address is no longer there and sub-channel is shutdown.
                // 3. Connection attempt fails and tries to update sub-channel state.
                if (_state == state || _state == ConnectivityState.Shutdown)
                {
                    return;
                }
                _state = state;
            }
            
            // Notify channel outside of lock to avoid deadlocks.
            _channel.OnSubChannelStateChange(this, state);
        }

        public override string ToString()
        {
            return string.Join(", ", _addresses);
        }

        public IList<DnsEndPoint> GetAddresses()
        {
            return _addresses.ToArray();
        }

        public void Dispose()
        {
            UpdateConnectivityState(ConnectivityState.Shutdown);
            Transport.Dispose();
        }
    }
}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif