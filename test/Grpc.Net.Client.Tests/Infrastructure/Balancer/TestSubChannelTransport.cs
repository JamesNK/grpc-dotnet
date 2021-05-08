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

using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client.Balancer;
using Grpc.Net.Client.Balancer.Internal;

namespace Grpc.Net.Client.Tests.Infrastructure.Balancer
{
    internal class TestSubChannelTransport : ISubChannelTransport
    {
        private ConnectivityState _state = ConnectivityState.Ready;
        private TaskCompletionSource<object?> _connectTcs;

        public SubChannel SubChannel { get; }

        public DnsEndPoint? CurrentEndPoint { get; private set; }

        public Task TryConnectTask => _connectTcs.Task;

        public TestSubChannelTransport(SubChannel subChannel)
        {
            SubChannel = subChannel;
            _connectTcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public void UpdateState(ConnectivityState state)
        {
            _state = state;
            SubChannel.UpdateConnectivityState(_state);
        }

        public void Dispose()
        {
        }

        public ValueTask<Stream> GetStreamAsync(DnsEndPoint endPoint, CancellationToken cancellationToken)
        {
            return new ValueTask<Stream>(new MemoryStream());
        }

        public void OnRequestError(Exception ex)
        {
        }

        public void OnRequestSuccess()
        {
        }

        public ValueTask<bool> TryConnectAsync(CancellationToken cancellationToken)
        {
            CurrentEndPoint = SubChannel._addresses[0];
            SubChannel.UpdateConnectivityState(_state);

            _connectTcs.TrySetResult(null);
            return new ValueTask<bool>(true);
        }
    }
}

#endif
