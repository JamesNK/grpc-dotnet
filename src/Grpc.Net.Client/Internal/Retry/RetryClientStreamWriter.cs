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

using System.Threading.Tasks;
using Grpc.Core;

namespace Grpc.Net.Client.Internal.Retry
{
    internal class RetryClientStreamWriter<TRequest, TResponse> : IClientStreamWriter<TRequest>
        where TRequest : class
        where TResponse : class
    {
        private readonly RetryCall<TRequest, TResponse> _retryCall;

        public RetryClientStreamWriter(RetryCall<TRequest, TResponse> retryCall)
        {
            _retryCall = retryCall;
            WriteOptions = retryCall.ActiveCall.Options.WriteOptions;
        }

        public WriteOptions WriteOptions { get; set; }

        public async Task CompleteAsync()
        {
            while (true)
            {
                GrpcCall<TRequest, TResponse> call = _retryCall.ActiveCall;
                try
                {
                    await call.ClientStreamWriter!.CompleteAsync().ConfigureAwait(false);
                    return;
                }
                catch
                {
                    if (!await _retryCall.ResolveRetryTask(call).ConfigureAwait(false))
                    {
                        throw;
                    }
                }
            }
        }

        public async Task WriteAsync(TRequest message)
        {
            while (true)
            {
                GrpcCall<TRequest, TResponse> call = _retryCall.ActiveCall;
                try
                {
                    var writer = call.ClientStreamWriter!;
                    writer.WriteOptions = WriteOptions;

                    await writer.WriteAsync(_retryCall.WriteNewMessage, message).ConfigureAwait(false);
                    _retryCall.BufferedCurrentMessage = false;

                    if (writer.CompleteCalled)
                    {
                        await writer.CompleteAsync().ConfigureAwait(false);
                    }
                    return;
                }
                catch
                {
                    if (!await _retryCall.ResolveRetryTask(call).ConfigureAwait(false))
                    {
                        throw;
                    }
                }
            }
        }
    }
}
