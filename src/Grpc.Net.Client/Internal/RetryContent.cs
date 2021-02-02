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
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;

namespace Grpc.Net.Client.Internal
{
    internal class RetryContent<TRequest, TResponse> : HttpContent
        where TRequest : class
        where TResponse : class
    {
        private readonly List<ReadOnlyMemory<byte>> _content;
        private readonly GrpcCall<TRequest, TResponse> _call;

        public RetryContent(List<ReadOnlyMemory<byte>> content, GrpcCall<TRequest, TResponse> call)
        {
            _content = content;
            _call = call;
            Headers.ContentType = GrpcProtocolConstants.GrpcContentTypeHeaderValue;
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
        {
            foreach (var item in _content)
            {
                await _call.WriteMessageAsync(stream, item, _call.Options).ConfigureAwait(false);
                GrpcEventSource.Log.MessageSent();
            }
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;
            return false;
        }
    }
}