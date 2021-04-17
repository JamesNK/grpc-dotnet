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
using System.Threading;
using Grpc.Net.Client.Balancer.Internal;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Balancer
{
    public class PickFirstBalancer : LoadBalancer
    {
        private readonly ClientConnection _connection;
        internal SubConnection? _subConnection;
        private ILogger _logger;

        public PickFirstBalancer(ClientConnection connection, ILoggerFactory loggerFactory)
        {
            _connection = connection;
            _logger = loggerFactory.CreateLogger<PickFirstBalancer>();
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

        public override void UpdateConnectionState(ConnectionState state)
        {
            if (state.ResolverState.Addresses.Count == 0)
            {
                ResolverError(new InvalidOperationException("Resolver returned no addresses."));
            }

            if (_subConnection == null)
            {
                try
                {
                    _subConnection = _connection.CreateSubConnection(new SubConnectionOptions(state.ResolverState.Addresses));
                }
                catch (Exception ex)
                {
                    _connection.UpdateState(new BalancerState(ConnectivityState.TransientFailure, new FailurePicker(ex)));
                    throw;
                }

                _connection.UpdateState(new BalancerState(ConnectivityState.Idle, new PickFirstPicker(_subConnection)));
            }
            else
            {
                _connection.UpdateAddresses(_subConnection, state.ResolverState.Addresses);
            }

            _ = _subConnection.ConnectAsync(CancellationToken.None);
        }

        public override void UpdateSubConnectionState(SubConnection subConnection, SubConnectionState state)
        {
            _logger.LogInformation("Updating sub-connection state.");

            if (_subConnection != subConnection)
            {
                _logger.LogInformation("Ignored state change because of unknown sub-connection.");
                return;
            }

            switch (_connection.State)
            {
                case ConnectivityState.Ready:
                case ConnectivityState.Idle:
                    _connection.UpdateState(new BalancerState(_connection.State, new PickFirstPicker(_subConnection)));
                    break;
                case ConnectivityState.Connecting:
                    _connection.UpdateState(new BalancerState(ConnectivityState.Connecting, EmptyPicker.Instance));
                    break;
                case ConnectivityState.TransientFailure:
                    _connection.UpdateState(new BalancerState(ConnectivityState.TransientFailure, new FailurePicker(state.ConnectionError!)));
                    break;
                case ConnectivityState.Shutdown:
                    _subConnection = null;
                    break;
            }
        }

        private class PickFirstPicker : SubConnectionPicker
        {
            private readonly SubConnection _subConnection;

            public PickFirstPicker(SubConnection subConnection)
            {
                _subConnection = subConnection;
            }

            public override PickResult Pick(PickContext context)
            {
                return new PickResult(_subConnection, c => { });
            }
        }
    }

}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif