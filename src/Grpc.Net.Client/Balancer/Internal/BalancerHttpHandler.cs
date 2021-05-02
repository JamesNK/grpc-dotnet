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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Shared;

namespace Grpc.Net.Client.Balancer.Internal
{
    internal class BalancerHttpHandler : DelegatingHandler
    {
        private readonly ClientChannel _clientConnection;
        private static readonly HttpRequestOptionsKey<SubChannel> _requestOptionsSubConnectionKey = new HttpRequestOptionsKey<SubChannel>(nameof(SubChannel));

        public BalancerHttpHandler(HttpMessageHandler innerHandler, ClientChannel clientConnection)
            : base(innerHandler)
        {
            _clientConnection = clientConnection;

            var socketsHttpHandler = (SocketsHttpHandler?)HttpHandlerFactory.GetHttpHandlerType(innerHandler, "System.Net.Http.SocketsHttpHandler");
            if (socketsHttpHandler != null)
            {
                socketsHttpHandler.ConnectCallback = OnConnect;
            }
        }

        private async ValueTask<Stream> OnConnect(SocketsHttpConnectionContext context, CancellationToken cancellationToken)
        {
            if (!context.InitialRequestMessage.Options.TryGetValue(_requestOptionsSubConnectionKey, out var subConnection))
            {
                throw new InvalidOperationException();
            }

            return await subConnection.GetStreamAsync(context.DnsEndPoint, cancellationToken).ConfigureAwait(false);
        }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            await _clientConnection.ConnectAsync(cancellationToken).ConfigureAwait(false);
            var result = await _clientConnection.PickAsync(request, cancellationToken).ConfigureAwait(false);

            // Update request host.
            var uriBuilder = new UriBuilder(request.RequestUri!);
            uriBuilder.Host = result.SubChannel!.CurrentEndPoint!.Host;
            uriBuilder.Port = result.SubChannel!.CurrentEndPoint!.Port;
            request.RequestUri = uriBuilder.Uri;

            // Set sub-connection onto request.
            // Will be used to get a stream in SocketsHttpHandler.ConnectCallback.
            request.Options.Set(_requestOptionsSubConnectionKey, result.SubChannel);

            return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
        }
    }
}

#endif