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

using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Client
{
    public class UnixDomainSocketConnectionFactory : SocketsConnectionFactory
    {
        private readonly EndPoint _endPoint;

        public UnixDomainSocketConnectionFactory(EndPoint endPoint) : base(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified)
        {
            _endPoint = endPoint;
        }

        public ValueTask<Stream> ConnectAsync(SocketsHttpConnectionContext _, CancellationToken cancellationToken = default)
        {
            return ConnectAsync(_endPoint, cancellationToken);
        }
    }

    public class SocketsConnectionFactory
    {
        private readonly AddressFamily _addressFamily;
        private readonly SocketType _socketType;
        private readonly ProtocolType _protocolType;

        public SocketsConnectionFactory(
            AddressFamily addressFamily,
            SocketType socketType,
            ProtocolType protocolType)
        {
            _addressFamily = addressFamily;
            _socketType = socketType;
            _protocolType = protocolType;
        }

        protected async ValueTask<Stream> ConnectAsync(
            EndPoint endPoint,
            CancellationToken cancellationToken = default)
        {
            if (endPoint == null) throw new ArgumentNullException(nameof(endPoint));
            cancellationToken.ThrowIfCancellationRequested();

            Socket socket = new Socket(_addressFamily, _socketType, _protocolType);

            try
            {
                await socket.ConnectAsync(endPoint, cancellationToken).ConfigureAwait(false);
                return new NetworkStream(socket, true);
            }
            catch
            {
                socket.Dispose();
                throw;
            }
        }

        protected virtual Socket CreateSocket(
            AddressFamily addressFamily,
            SocketType socketType,
            ProtocolType protocolType,
            EndPoint? endPoint)
        {
            Socket socket = new Socket(addressFamily, socketType, protocolType);

            if (protocolType == ProtocolType.Tcp)
            {
                socket.NoDelay = true;
            }

            if (addressFamily == AddressFamily.InterNetworkV6)
            {
                socket.DualMode = true;
            }

            return socket;
        }
    }
}
