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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Shared;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Internal.Retry
{
    internal abstract partial class RetryCallBase<TRequest, TResponse> : IGrpcCall<TRequest, TResponse>
        where TRequest : class
        where TResponse : class
    {
        private static StatusGrpcCall<TRequest, TResponse>? _deadlineCall;
        protected static StatusGrpcCall<TRequest, TResponse> DeadlineCall => _deadlineCall ??= new StatusGrpcCall<TRequest, TResponse>(new Status(StatusCode.DeadlineExceeded, string.Empty));
        private static StatusGrpcCall<TRequest, TResponse>? _throttledCall;
        protected static StatusGrpcCall<TRequest, TResponse> ThrottledCall => _throttledCall ??= new StatusGrpcCall<TRequest, TResponse>(new Status(StatusCode.Cancelled, "Retries stopped because retry throttling is active."));

        private RetryCallBaseClientStreamReader<TRequest, TResponse>? _retryBaseClientStreamReader;
        private RetryCallBaseClientStreamWriter<TRequest, TResponse>? _retryBaseClientStreamWriter;

        protected object Lock { get; } = new object();
        protected ILogger Logger { get; }
        protected GrpcChannel Channel { get; }
        protected Method<TRequest, TResponse> Method { get; }
        protected CallOptions Options { get; }
        protected TaskCompletionSource<IGrpcCall<TRequest, TResponse>> FinalizedCallTcs { get; }
        protected CancellationTokenSource CancellationTokenSource { get; }

        public Task<IGrpcCall<TRequest, TResponse>> FinalizedCallTask => FinalizedCallTcs.Task;
        public IAsyncStreamReader<TResponse>? ClientStreamReader => _retryBaseClientStreamReader ??= new RetryCallBaseClientStreamReader<TRequest, TResponse>(this);
        public IClientStreamWriter<TRequest>? ClientStreamWriter => _retryBaseClientStreamWriter ??= new RetryCallBaseClientStreamWriter<TRequest, TResponse>(this);
        public WriteOptions? ClientStreamWriteOptions { get; internal set; }

        protected bool ClientStreamComplete { get; set; }

        protected List<ReadOnlyMemory<byte>> BufferedMessages { get; }
        protected bool BufferedCurrentMessage { get; set; }

        protected RetryCallBase(GrpcChannel channel, Method<TRequest, TResponse> method, CallOptions options, string loggerName)
        {
            Logger = channel.LoggerFactory.CreateLogger(loggerName);
            Channel = channel;
            Method = method;
            Options = options;
            FinalizedCallTcs = new TaskCompletionSource<IGrpcCall<TRequest, TResponse>>(TaskCreationOptions.RunContinuationsAsynchronously);
            BufferedMessages = new List<ReadOnlyMemory<byte>>();

            CancellationTokenSource = new CancellationTokenSource();

            // TODO(JamesNK) - Check that large deadlines are supported. Might need to use a Timer here instead.
            var deadline = Options.Deadline.GetValueOrDefault(DateTime.MaxValue);
            if (deadline != DateTime.MaxValue)
            {
                var timeout = CommonGrpcProtocolHelpers.GetTimerDueTime(deadline - Channel.Clock.UtcNow, Channel.MaxTimerDueTime);
                CancellationTokenSource.CancelAfter(TimeSpan.FromMilliseconds(timeout));
            }
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
            StartCore(call => call.StartUnaryCore(CreatePushUnaryContent(request, call)));
        }

        public void StartClientStreaming()
        {
            StartCore(call =>
            {
                var clientStreamWriter = new HttpContentClientStreamWriter<TRequest, TResponse>(call);
                var content = CreatePushStreamContent(call, clientStreamWriter);
                call.StartClientStreamingCore(clientStreamWriter, content);
            });
        }

        public void StartServerStreaming(TRequest request)
        {
            StartCore(call => call.StartServerStreamingCore(CreatePushUnaryContent(request, call)));
        }

        public void StartDuplexStreaming()
        {
            StartCore(call =>
            {
                var clientStreamWriter = new HttpContentClientStreamWriter<TRequest, TResponse>(call);
                var content = CreatePushStreamContent(call, clientStreamWriter);
                call.StartDuplexStreamingCore(clientStreamWriter, content);
            });
        }

        private PushUnaryContent<TRequest, TResponse> CreatePushUnaryContent(TRequest request, GrpcCall<TRequest, TResponse> call)
        {
            return new PushUnaryContent<TRequest, TResponse>(stream => WriteNewMessage(call, stream, call.Options, request));
        }

        private PushStreamContent<TRequest, TResponse> CreatePushStreamContent(GrpcCall<TRequest, TResponse> call, HttpContentClientStreamWriter<TRequest, TResponse> clientStreamWriter)
        {
            return new PushStreamContent<TRequest, TResponse>(clientStreamWriter, async requestStream =>
            {
                ValueTask writeTask;
                lock (Lock)
                {
                    Log.SendingBufferedMessages(Logger, BufferedMessages.Count);

                    if (BufferedMessages.Count == 0)
                    {
                        writeTask = default;
                    }
                    else if (BufferedMessages.Count == 1)
                    {
                        writeTask = call.WriteMessageAsync(requestStream, BufferedMessages[0], call.CancellationToken);
                    }
                    else
                    {
                        // Copy messages to a new collection in lock for thread-safety.
                        var bufferedMessageCopy = BufferedMessages.ToArray();
                        writeTask = WriteBufferedMessages(call, requestStream, bufferedMessageCopy);
                    }
                }

                await writeTask.ConfigureAwait(false);

                if (ClientStreamComplete)
                {
                    await call.ClientStreamWriter!.CompleteAsync().ConfigureAwait(false);
                }
            });

            static async ValueTask WriteBufferedMessages(GrpcCall<TRequest, TResponse> call, Stream requestStream, ReadOnlyMemory<byte>[] bufferedMessages)
            {
                foreach (var writtenMessage in bufferedMessages)
                {
                    await call.WriteMessageAsync(requestStream, writtenMessage, call.CancellationToken).ConfigureAwait(false);
                }
            }
        }

        protected abstract void StartCore(Action<GrpcCall<TRequest, TResponse>> startCallFunc);

        public abstract Task ClientStreamCompleteAsync();

        public abstract Task ClientStreamWriteAsync(TRequest message);

        protected bool IsDeadlineExceeded()
        {
            return Options.Deadline != null && Options.Deadline <= Channel.Clock.UtcNow;
        }

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

                // Need to take a copy because the serialization context will returned a rented buffer.
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
            ReadOnlyMemory<byte> messageData;

            lock (Lock)
            {
                if (!BufferedCurrentMessage)
                {
                    messageData = SerializePayload(call, callOptions, message);
                    BufferedMessages.Add(messageData);
                    BufferedCurrentMessage = true;

                    Log.MessageAddedToBuffer(Logger, messageData.Length);
                }
                else
                {
                    // There is a race between:
                    // 1. A client stream starting for a new call. It will write all buffered messages, and
                    // 2. Writing a new message here. The message may already have been buffered when the client
                    //    stream started so we don't want to write it again.
                    //
                    // Check the client stream write count against he buffer message count to ensure all buffered
                    // messages haven't already been written.
                    if (call.MessagesWritten == BufferedMessages.Count)
                    {
                        return;
                    }

                    messageData = BufferedMessages[BufferedMessages.Count - 1];
                }
            }

            await call.WriteMessageAsync(writeStream, messageData, callOptions.CancellationToken).ConfigureAwait(false);
        }

        protected bool HasClientStream()
        {
            return Method.Type == MethodType.ClientStreaming || Method.Type == MethodType.DuplexStreaming;
        }

        Task IGrpcCall<TRequest, TResponse>.WriteClientStreamAsync<TState>(Func<GrpcCall<TRequest, TResponse>, Stream, CallOptions, TState, ValueTask> writeFunc, TState state)
        {
            throw new NotSupportedException();
        }
    }
}
