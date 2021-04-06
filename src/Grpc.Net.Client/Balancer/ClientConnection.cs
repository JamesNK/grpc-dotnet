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
        private SubConnectionPicker? _picker;

        public ConnectivityState State => _state;

        public abstract ILoggerFactory LoggerFactory { get; }
        public abstract SubConnection CreateSubConnection(SubConnectionOptions options);
        public abstract void RemoveSubConnection(SubConnection subConnection);
        public abstract void UpdateAddresses(SubConnection subConnection, IReadOnlyList<DnsEndPoint> addresses);
        public abstract Task ResolveNowAsync();

        public virtual void UpdateState(BalancerState state)
        {
            _state = state.ConnectivityState;
            _picker = state.Picker;
        }

        public async ValueTask<PickResult> PickAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            await Task.Yield();

            PickResult? result;
            while (true)
            {
                result = _picker!.Pick(new PickContext(request));
                if (result.SubConnection == null)
                {

                }
            }

            if (result.SubConnection.CurrentEndPoint == null)
            {
                await result.SubConnection.ConnectAsync(cancellationToken).ConfigureAwait(false);
            }

            return result;
        }
    }

}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif