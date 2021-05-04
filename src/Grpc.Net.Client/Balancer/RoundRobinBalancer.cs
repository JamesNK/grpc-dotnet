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
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client.Balancer.Internal;
using Grpc.Net.Client.Configuration;
using Grpc.Net.Client.Internal;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Balancer
{
    public class RoundRobinBalancer : LoadBalancer
    {
        private readonly IChannelControlHelper _channel;
        private readonly IRandomGenerator _randomGenerator;
        private readonly List<(DnsEndPoint Address, SubChannel SubChannel)> _subChannels;
        private ILogger _logger;

        public RoundRobinBalancer(IChannelControlHelper channel, ILoggerFactory loggerFactory) : this(channel, loggerFactory, new RandomGenerator())
        {
        }

        internal RoundRobinBalancer(IChannelControlHelper channel, ILoggerFactory loggerFactory, IRandomGenerator randomGenerator)
        {
            _subChannels = new List<(DnsEndPoint Address, SubChannel SubChannel)>();
            _channel = channel;
            _randomGenerator = randomGenerator;
            _logger = loggerFactory.CreateLogger<PickFirstBalancer>();
        }

        public override void Close()
        {
        }

        public override void ResolverError(Exception exception)
        {
            switch (_channel.State)
            {
                case ConnectivityState.Idle:
                case ConnectivityState.Connecting:
                case ConnectivityState.TransientFailure:
                    _channel.UpdateState(new BalancerState(ConnectivityState.TransientFailure, new FailurePicker(exception)));
                    break;
            }
        }

        private int? FindSubChannelByAddress(List<(DnsEndPoint Address, SubChannel SubChannel)> subChannels, DnsEndPoint endPoint)
        {
            for (var i = 0; i < subChannels.Count; i++)
            {
                var s = subChannels[i];
                if (Equals(s.Address, endPoint))
                {
                    return i;
                }
            }

            return null;
        }

        private int? FindSubChannel(List<(DnsEndPoint Address, SubChannel SubChannel)> subChannels, SubChannel subChannel)
        {
            for (var i = 0; i < subChannels.Count; i++)
            {
                var s = subChannels[i];
                if (Equals(s.SubChannel, subChannel))
                {
                    return i;
                }
            }

            return null;
        }

        public override void UpdateChannelState(ChannelState state)
        {
            if (state.ResolverState.Addresses.Count == 0)
            {
                ResolverError(new InvalidOperationException("Resolver returned no addresses."));
            }

            var allUpdatedSubConnections = new List<(DnsEndPoint Address, SubChannel SubChannel)>();
            var newSubConnections = new List<SubChannel>();
            var currentSubConnections = _subChannels.ToList();

            foreach (var address in state.ResolverState.Addresses)
            {
                var i = FindSubChannelByAddress(currentSubConnections, address);

                (DnsEndPoint, SubChannel) newSubConnection;
                if (i != null)
                {
                    newSubConnection = currentSubConnections[i.GetValueOrDefault()];
                    currentSubConnections.RemoveAt(i.GetValueOrDefault());
                }
                else
                {
                    var c = _channel.CreateSubChannel(new SubChannelOptions(new[] { address }));

                    newSubConnections.Add(c);
                    newSubConnection = (address, c);
                }

                allUpdatedSubConnections.Add(newSubConnection);
            }

            // Any sub-connections still in this collection are no longer returned by the resolver.
            // This can all be removed.
            var removedSubConnections = currentSubConnections;

            if (removedSubConnections.Count == 0 && newSubConnections.Count == 0)
            {
                _logger.LogTrace("Connections unchanged.");
                return;
            }

            foreach (var removedSubConnection in removedSubConnections)
            {
                _channel.RemoveSubChannel(removedSubConnection.SubChannel);
            }

            _subChannels.Clear();
            _subChannels.AddRange(allUpdatedSubConnections);

            // Start new connections after collection on balancer has been updated.
            foreach (var c in newSubConnections)
            {
                _ = Task.Run(() => c.ConnectAsync(CancellationToken.None));
            }

            UpdateBalancingState();
        }

        private void UpdateBalancingState()
        {
            var readySubChannels = _subChannels.Where(s => s.SubChannel.State == ConnectivityState.Ready).ToList();
            if (readySubChannels.Count == 0)
            {
                // No READY subchannels, determine aggregate state and error status
                var isConnecting = false;
                ConnectivityState? aggState = null;
                foreach (var subChannel in _subChannels)
                {
                    var state = subChannel.SubChannel.State;

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
                    _channel.UpdateState(new BalancerState(ConnectivityState.Connecting, EmptyPicker.Instance));
                }
                else
                {
                    _channel.UpdateState(new BalancerState(ConnectivityState.TransientFailure, new FailurePicker(new Exception())));
                }
            }
            else
            {
                // Initialize the Picker to a random start index to ensure that a high frequency of Picker
                // churn does not skew subchannel selection.
                int pickCount = _randomGenerator.Next(0, readySubChannels.Count);
                _channel.UpdateState(new BalancerState(ConnectivityState.Ready, new RoundRobinPicker(readySubChannels, pickCount)));
            }
        }

        public override void UpdateSubChannelState(SubChannel subChannel, SubChannelState state)
        {
            _logger.LogInformation("Updating sub-channel state.");

            var index = FindSubChannel(_subChannels, subChannel);
            if (index == null)
            {
                _logger.LogInformation("Ignored state change because of unknown sub-channel.");
                return;
            }

            if (state.ConnectivityState == ConnectivityState.Idle)
            {
                _ = subChannel.ConnectAsync(CancellationToken.None);
            }

            UpdateBalancingState();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
        }
    }

    internal class RoundRobinPicker : SubChannelPicker, IEquatable<RoundRobinPicker>
    {
        // Internal for testing
        internal readonly List<(DnsEndPoint Address, SubChannel SubChannel)> _subChannels;
        private long _pickCount;

        public RoundRobinPicker(List<(DnsEndPoint Address, SubChannel SubChannel)> subChannels, long pickCount)
        {
            _subChannels = subChannels;
            _pickCount = pickCount;
        }

        public bool Equals(RoundRobinPicker? other)
        {
            if (other == null || _subChannels.Count != other._subChannels.Count)
            {
                return false;
            }

            for (var i = 0; i < _subChannels.Count; i++)
            {
                if (!Equals(_subChannels[i], other._subChannels[i]))
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as RoundRobinPicker);
        }

        public override int GetHashCode()
        {
            int hashCode = 0;
            for (var i = 0; i < _subChannels.Count; i++)
            {
                hashCode ^= _subChannels[i].GetHashCode();
            }
            return hashCode;
        }

        public override PickResult Pick(PickContext context)
        {
            var c = Interlocked.Increment(ref _pickCount);
            var index = (c - 1) % _subChannels.Count;

            return new PickResult(_subChannels[(int)index].SubChannel, c => { });
        }

        public override string ToString()
        {
            return string.Join(", ", _subChannels.Select(s => s.Address));
        }
    }

    public class RoundRobinBalancerFactory : LoadBalancerFactory
    {
        public override string Name { get; } = LoadBalancingConfig.RoundRobinPolicyName;

        public override LoadBalancer Create(IChannelControlHelper channelControlHelper, ILoggerFactory loggerFactory, IDictionary<string, object> options)
        {
            return new RoundRobinBalancer(channelControlHelper, loggerFactory);
        }
    }
}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif