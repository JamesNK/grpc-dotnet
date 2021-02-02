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
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace Grpc.Net.Client.Internal
{
    internal class PushStreamContent<TRequest, TResponse> : HttpContent
        where TRequest : class
        where TResponse : class
    {
        private readonly HttpContentClientStreamWriter<TRequest, TResponse> _streamWriter;
        private readonly List<ReadOnlyMemory<byte>>? _data;
        private readonly bool _completed;

        public PushStreamContent(HttpContentClientStreamWriter<TRequest, TResponse> streamWriter)
        {
            Headers.ContentType = GrpcProtocolConstants.GrpcContentTypeHeaderValue;
            _streamWriter = streamWriter;
        }

        public PushStreamContent(
            HttpContentClientStreamWriter<TRequest, TResponse> streamWriter,
            List<ReadOnlyMemory<byte>> data,
            bool completed) : this(streamWriter)
        {
            _data = data;
            _completed = completed;
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
        {
            // Immediately flush request stream to send headers
            // https://github.com/dotnet/corefx/issues/39586#issuecomment-516210081
            await stream.FlushAsync().ConfigureAwait(false);

            if (_data != null)
            {
                foreach (var item in _data)
                {
                    await _streamWriter.WriteAsyncCore(item, stream).ConfigureAwait(false);
                }

                if (_completed)
                {
                    await _streamWriter.CompleteAsync().ConfigureAwait(false);
                }
            }

            if (_streamWriter.Call.StreamWrapper != null)
            {
                stream = _streamWriter.Call.StreamWrapper(stream);
            }

            // Pass request stream to writer
            _streamWriter.WriteStreamTcs.TrySetResult(stream);

            // Wait for the writer to report it is complete
            await _streamWriter.CompleteTcs.Task.ConfigureAwait(false);
        }

        protected override bool TryComputeLength(out long length)
        {
            // We can't know the length of the content being pushed to the output stream.
            length = -1;
            return false;
        }

        // Hacky. ReadAsStreamAsync does not complete until SerializeToStreamAsync finishes
        internal Task PushComplete => ReadAsStreamAsync();
    }
}