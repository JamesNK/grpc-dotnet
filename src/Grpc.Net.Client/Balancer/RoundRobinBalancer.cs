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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using Grpc.Net.Client.Balancer.Internal;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Balancer
{
    public class RoundRobinBalancer : LoadBalancer
    {
        private readonly ClientConnection _connection;
        private readonly List<(DnsEndPoint Address, SubConnection SubConnection)> _subConnections;
        private readonly Random _random;
        private ILogger _logger;

        public RoundRobinBalancer(ClientConnection connection)
        {
            _subConnections = new List<(DnsEndPoint Address, SubConnection SubConnection)>();
            _connection = connection;
            _random = new Random();
            _logger = _connection.LoggerFactory.CreateLogger<PickFirstBalancer>();
        }

        public override void Close()
        {
        }

        public override void ResolverError(Exception exception)
        {
            switch (_connection.State)
            {
                case ConnectivityState.Idle:
                case ConnectivityState.Connecting:
                case ConnectivityState.TransientFailure:
                    _connection.UpdateState(new BalancerState(ConnectivityState.TransientFailure, new FailurePicker(exception)));
                    break;
            }
        }

        private int? FindSubConnectionByAddress(List<(DnsEndPoint Address, SubConnection SubConnection)> subConnections, DnsEndPoint endPoint)
        {
            for (var i = 0; i < subConnections.Count; i++)
            {
                var s = subConnections[i];
                if (Equals(s.Address, endPoint))
                {
                    return i;
                }
            }

            return null;
        }

        private int? FindSubConnection(List<(DnsEndPoint Address, SubConnection SubConnection)> subConnections, SubConnection subConnection)
        {
            for (var i = 0; i < subConnections.Count; i++)
            {
                var s = subConnections[i];
                if (Equals(s.SubConnection, subConnection))
                {
                    return i;
                }
            }

            return null;
        }

        public override void UpdateConnectionState(ConnectionState state)
        {
            if (state.ResolverState.Addresses.Count == 0)
            {
                ResolverError(new InvalidOperationException("Resolver returned no addresses."));
            }

            var allUpdatedSubConnections = new List<(DnsEndPoint Address, SubConnection SubConnection)>();
            var newSubConnections = new List<SubConnection>();
            var currentSubConnections = _subConnections.ToList();

            foreach (var address in state.ResolverState.Addresses)
            {
                var i = FindSubConnectionByAddress(currentSubConnections, address);

                (DnsEndPoint, SubConnection) newSubConnection;
                if (i != null)
                {
                    newSubConnection = currentSubConnections[i.GetValueOrDefault()];
                    currentSubConnections.RemoveAt(i.GetValueOrDefault());
                }
                else
                {
                    var c = _connection.CreateSubConnection(new SubConnectionOptions(new[] { address }));

                    newSubConnections.Add(c);
                    newSubConnection = (address, c);
                }

                allUpdatedSubConnections.Add(newSubConnection);
            }

            foreach (var removedSubConnection in _subConnections)
            {
                _connection.RemoveSubConnection(removedSubConnection.SubConnection);
            }

            _subConnections.Clear();
            _subConnections.AddRange(allUpdatedSubConnections);

            // Start new connections after collection on balancer has been updated.
            foreach (var c in newSubConnections)
            {
                _ = c.ConnectAsync(CancellationToken.None);
            }

            UpdateBalancingState();
        }

        private void UpdateBalancingState()
        {
            var readySubConnections = _subConnections.Where(s => s.SubConnection.State == ConnectivityState.Ready).ToList();
            if (readySubConnections.Count == 0)
            {
                // No READY subchannels, determine aggregate state and error status
                var isConnecting = false;
                ConnectivityState? aggState = null;
                foreach (var subConnection in _subConnections)
                {
                    var state = subConnection.SubConnection.State;

                    if (state == ConnectivityState.Connecting || state == ConnectivityState.Idle)
                    {
                        isConnecting = true;
                    }
                    if (aggState == null || aggState == ConnectivityState.TransientFailure)
                    {
                        aggState = state;
                    }
                }

                if (isConnecting)
                {
                    _connection.UpdateState(new BalancerState(ConnectivityState.Connecting, new EmptyPicker()));
                }
                else
                {
                    _connection.UpdateState(new BalancerState(ConnectivityState.TransientFailure, new FailurePicker(new Exception())));
                }
            }
            else
            {
                // Initialize the Picker to a random start index to ensure that a high frequency of Picker
                // churn does not skew subchannel selection.
                int pickCount = _random.Next(0, readySubConnections.Count);
                _connection.UpdateState(new BalancerState(ConnectivityState.Ready, new RoundRobinPicker(readySubConnections, pickCount)));
            }
        }

        public override void UpdateSubConnectionState(SubConnection subConnection, SubConnectionState state)
        {
            _logger.LogInformation("Updating sub-connection state.");

            var index = FindSubConnection(_subConnections, subConnection);
            if (index == null)
            {
                _logger.LogInformation("Ignored state change because of unknown sub-connection.");
                return;
            }

            if (state.ConnectivityState == ConnectivityState.Idle)
            {
                _ = subConnection.ConnectAsync(CancellationToken.None);
            }

            UpdateBalancingState();
        }

        private class RoundRobinPicker : SubConnectionPicker
        {
            private readonly List<(DnsEndPoint Address, SubConnection SubConnection)> _subConnections;
            private long _pickCount;

            public RoundRobinPicker(List<(DnsEndPoint Address, SubConnection SubConnection)> subConnections, long pickCount)
            {
                _subConnections = subConnections;
                _pickCount = pickCount;
            }

            public override PickResult Pick(PickContext context)
            {
                var c = Interlocked.Increment(ref _pickCount);
                var index = c % _subConnections.Count;

                return new PickResult(_subConnections[(int)index].SubConnection, c => { });
            }
        }
    }

}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif