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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Internal.Retry
{
    internal abstract partial class RetryCallBase<TRequest, TResponse> : IGrpcCall<TRequest, TResponse>
        where TRequest : class
        where TResponse : class
    {
        private RetryCallBaseClientStreamReader<TRequest, TResponse>? _retryBaseClientStreamReader;
        private RetryCallBaseClientStreamWriter<TRequest, TResponse>? _retryBaseClientStreamWriter;

        protected ILogger Logger { get; }
        protected GrpcChannel Channel { get; }
        protected Method<TRequest, TResponse> Method { get; }
        protected CallOptions Options { get; }
        protected TaskCompletionSource<GrpcCall<TRequest, TResponse>> FinalizedCallTcs { get; }

        public Task<GrpcCall<TRequest, TResponse>> FinalizedCallTask => FinalizedCallTcs.Task;
        public IAsyncStreamReader<TResponse>? ClientStreamReader => _retryBaseClientStreamReader ??= new RetryCallBaseClientStreamReader<TRequest, TResponse>(this);
        public IClientStreamWriter<TRequest>? ClientStreamWriter => _retryBaseClientStreamWriter ??= new RetryCallBaseClientStreamWriter<TRequest, TResponse>(this);
        public WriteOptions? ClientStreamWriteOptions { get; internal set; }

        protected bool ClientStreamComplete { get; set; }

        protected readonly List<ReadOnlyMemory<byte>> WrittenMessages;
        protected bool BufferedCurrentMessage { get; set; }

        protected object Lock { get; } = new object();

        protected RetryCallBase(GrpcChannel channel, Method<TRequest, TResponse> method, CallOptions options, string loggerName)
        {
            Logger = channel.LoggerFactory.CreateLogger(loggerName);
            Channel = channel;
            Method = method;
            Options = options;
            FinalizedCallTcs = new TaskCompletionSource<GrpcCall<TRequest, TResponse>>(TaskCreationOptions.RunContinuationsAsynchronously);
            WrittenMessages = new List<ReadOnlyMemory<byte>>();
        }

        public async Task<TResponse> GetResponseAsync()
        {
            var call = await FinalizedCallTcs.Task.ConfigureAwait(false);
            return await call.GetResponseAsync().ConfigureAwait(false);
        }

        public async Task<Metadata> GetResponseHeadersAsync()
        {
            var call = await FinalizedCallTcs.Task.ConfigureAwait(false);
            return await call.GetResponseHeadersAsync().ConfigureAwait(false);
        }

        public Status GetStatus()
        {
            if (FinalizedCallTcs.Task.IsCompletedSuccessfully)
            {
                return FinalizedCallTcs.Task.Result.GetStatus();
            }

            throw new InvalidOperationException("Unable to get the status because the call is not complete.");
        }

        public Metadata GetTrailers()
        {
            if (FinalizedCallTcs.Task.IsCompletedSuccessfully)
            {
                return FinalizedCallTcs.Task.Result.GetTrailers();
            }

            throw new InvalidOperationException("Can't get the call trailers because the call has not completed successfully.");
        }

        public abstract void Dispose();

        public void StartUnary(TRequest request)
        {
            StartCore(call =>
            {
                call.StartUnaryCore(new PushUnaryContent<TRequest, TResponse>(stream => WriteNewMessage(call, stream, call.Options, request)));
            });
        }

        public void StartClientStreaming()
        {
            StartCore(call =>
            {
                var clientStreamWriter = new HttpContentClientStreamWriter<TRequest, TResponse>(call);
                var content = new PushStreamContent<TRequest, TResponse>(clientStreamWriter, async requestStream =>
                {
                    //Log.SendingBufferedMessages(_logger, _writtenMessages.Count);
                    Console.WriteLine("PushStreamStart");
                    await WriteBufferedMessages(call, requestStream, call.CancellationToken).ConfigureAwait(false);

                    if (ClientStreamComplete)
                    {
                        await call.ClientStreamWriter!.CompleteAsync().ConfigureAwait(false);
                    }
                });
                call.StartClientStreamingCore(clientStreamWriter, content);
            });
        }

        public void StartServerStreaming(TRequest request)
        {
            StartCore(call =>
            {
                call.StartServerStreamingCore(new PushUnaryContent<TRequest, TResponse>(stream => WriteNewMessage(call, stream, call.Options, request)));
            });
        }

        public void StartDuplexStreaming()
        {
            StartCore(call =>
            {
                var clientStreamWriter = new HttpContentClientStreamWriter<TRequest, TResponse>(call);
                var content = new PushStreamContent<TRequest, TResponse>(clientStreamWriter, async requestStream =>
                {
                    //Log.SendingBufferedMessages(_logger, _writtenMessages.Count);
                    Console.WriteLine("PushStreamStart");
                    await WriteBufferedMessages(call, requestStream, call.CancellationToken).ConfigureAwait(false);

                    if (ClientStreamComplete)
                    {
                        await call.ClientStreamWriter!.CompleteAsync().ConfigureAwait(false);
                    }
                });
                call.StartDuplexStreamingCore(clientStreamWriter, content);
            });
        }

        protected abstract void StartCore(Action<GrpcCall<TRequest, TResponse>> startCallFunc);

        public abstract Task ClientStreamCompleteAsync();

        public abstract Task ClientStreamWriteAsync(TRequest message);

        protected abstract ValueTask WriteBufferedMessages(GrpcCall<TRequest, TResponse> call, Stream writeStream, CancellationToken cancellationToken);

        protected int? GetRetryPushback(GrpcCall<TRequest, TResponse> call)
        {
            // https://github.com/grpc/proposal/blob/master/A6-client-retries.md#pushback
            if (call.HttpResponse != null)
            {
                if (call.HttpResponse.Headers.TryGetValues(GrpcProtocolConstants.RetryPushbackHeader, out var values))
                {
                    var headerValue = values.Single();
                    Log.RetryPushbackReceived(Logger, headerValue);

                    // A non-integer value means the server wants retries to stop.
                    // Resolve non-integer value to a negative integer which also means stop.
                    return int.TryParse(headerValue, out var value) ? value : -1;
                }
            }

            return null;
        }

        protected byte[] SerializePayload(GrpcCall<TRequest, TResponse> call, CallOptions callOptions, TRequest request)
        {
            var serializationContext = call.SerializationContext;
            serializationContext.CallOptions = callOptions;
            serializationContext.Initialize();

            try
            {
                call.Method.RequestMarshaller.ContextualSerializer(request, serializationContext);

                if (!serializationContext.TryGetPayload(out var payload))
                {
                    throw new Exception();
                }
                return payload.ToArray();
            }
            finally
            {
                serializationContext.Reset();
            }
        }

        protected async ValueTask WriteNewMessage(GrpcCall<TRequest, TResponse> call, Stream writeStream, CallOptions callOptions, TRequest message)
        {
            // Serialize current message and add to the buffer.
            if (!BufferedCurrentMessage)
            {
                lock (Lock)
                {
                    if (!BufferedCurrentMessage)
                    {
                        var payload = SerializePayload(call, callOptions, message);
                        WrittenMessages.Add(payload);
                        BufferedCurrentMessage = true;

                        Console.WriteLine($"Serialized and buffered message. New buffer count {WrittenMessages.Count}");
                    }
                }
            }

            await call.WriteMessageAsync(writeStream, WrittenMessages[WrittenMessages.Count - 1], callOptions.CancellationToken).ConfigureAwait(false);
            //await WriteBufferedMessages(call, writeStream, callOptions.CancellationToken).ConfigureAwait(false);
        }

    }
}
