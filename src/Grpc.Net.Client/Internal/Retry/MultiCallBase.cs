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
using Grpc.Net.Client.Configuration;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Internal.Retry
{
    internal abstract class MultiCallBase<TRequest, TResponse> : IGrpcCall<TRequest, TResponse>
        where TRequest : class
        where TResponse : class
    {
        private readonly ILogger _logger;
        private readonly GrpcChannel _channel;
        private readonly Method<TRequest, TResponse> _method;
        private readonly CallOptions _options;
        private readonly List<ReadOnlyMemory<byte>> _writtenMessages;
        private readonly Random _random;
        private readonly object _lock = new object();
        private readonly CancellationTokenSource _retryCts = new CancellationTokenSource();

        private int _bufferedMessagesIndex;
        //private int _attemptCount;
        //private TaskCompletionSource<bool> _canRetryTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool BufferedCurrentMessage { get; set; }
        public abstract IClientStreamWriter<TRequest>? ClientStreamWriter { get; }
        public IAsyncStreamReader<TResponse>? ClientStreamReader { get; }

        public async ValueTask WriteNewMessage(GrpcCall<TRequest, TResponse> call, Stream writeStream, CallOptions callOptions, TRequest message)
        {
            // Serialize current message and add to the buffer.
            if (!BufferedCurrentMessage)
            {
                var payload = SerializePayload(call, callOptions, message);
                _writtenMessages.Add(payload);
                BufferedCurrentMessage = true;
            }

            await WriteBufferedMessages(writeStream, callOptions.CancellationToken).ConfigureAwait(false);
        }

        private byte[] SerializePayload(GrpcCall<TRequest, TResponse> call, CallOptions callOptions, TRequest request)
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

        internal async ValueTask WriteBufferedMessages(Stream writeStream, CancellationToken cancellationToken)
        {
            while (_bufferedMessagesIndex < _writtenMessages.Count)
            {
                var writtenMessage = _writtenMessages[_bufferedMessagesIndex];

                await Task.Yield();
                //await ActiveCall.WriteMessageAsync(writeStream, writtenMessage, cancellationToken).ConfigureAwait(false);
                _bufferedMessagesIndex++;
            }
        }

        public MultiCallBase(RetryPolicy retryPolicy, GrpcChannel channel, Method<TRequest, TResponse> method, CallOptions options)
        {
            _logger = channel.LoggerFactory.CreateLogger(string.Empty); // TODO
            _channel = channel;
            _method = method;
            _options = options;
            _writtenMessages = new List<ReadOnlyMemory<byte>>();
            _random = new Random();
            //_attemptCount = 1;
        }

        private void ValidatePolicy(RetryPolicy retryPolicy)
        {
            if (retryPolicy.MaxAttempts == null)
            {
                throw CreateException(_method, RetryPolicy.MaxAttemptsPropertyName);
            }
            if (retryPolicy.InitialBackoff == null)
            {
                throw CreateException(_method, RetryPolicy.InitialBackoffPropertyName);
            }
            if (retryPolicy.MaxBackoff == null)
            {
                throw CreateException(_method, RetryPolicy.MaxBackoffPropertyName);
            }
            if (retryPolicy.BackoffMultiplier == null)
            {
                throw CreateException(_method, RetryPolicy.BackoffMultiplierPropertyName);
            }
            if (retryPolicy.RetryableStatusCodes.Count == 0)
            {
                throw new InvalidOperationException($"Retry policy for '{_method.FullName}' must have property '{RetryPolicy.RetryableStatusCodesPropertyName}' and must be non-empty.");
            }

            static InvalidOperationException CreateException(IMethod method, string propertyName)
            {
                return new InvalidOperationException($"Retry policy for '{method.FullName}' is missing required property '{propertyName}'.");
            }
        }

        private GrpcCall<TRequest, TResponse> CreateRetryCall(bool clientStreamCompleted, int previousAttempts)
        {
            var call = HttpClientCallInvoker.CreateGrpcCall<TRequest, TResponse>(_channel, _method, _options, previousAttempts);
            call.StartRetry(clientStreamCompleted, async requestStream =>
            {
                //Log.SendingBufferedMessages(_logger, _writtenMessages.Count);

                //await WriteBufferedMessages(requestStream, ActiveCall.CancellationToken).ConfigureAwait(false);
                await Task.Yield();

                if (clientStreamCompleted)
                {
                    await call.ClientStreamWriter!.CompleteAsync().ConfigureAwait(false);
                }
            });

            return call;
        }

        public abstract Task<TResponse> GetResponseAsync();

        //public async Task<bool> ResolveRetryTask(GrpcCall<TRequest, TResponse> call)
        //{
        //    Task<bool> canRetryTask;
        //    lock (_lock)
        //    {
        //        // New call has already been made
        //        if (call != ActiveCall)
        //        {
        //            return true;
        //        }

        //        // Wait to see whether new call will be made
        //        canRetryTask = _canRetryTcs.Task;
        //    }

        //    var canRetry = await canRetryTask.ConfigureAwait(false);
        //    if (canRetry)
        //    {
        //        // Verify a new call has been made
        //        Debug.Assert(call != ActiveCall);
        //        return true;
        //    }

        //    return false;
        //}


        protected abstract Task StartRetry();

        public abstract Task<Metadata> GetResponseHeadersAsync();

        public abstract Status GetStatus();

        public abstract Metadata GetTrailers();

        public void Dispose()
        {
            lock (_lock)
            {
                _retryCts.Cancel();
                //ActiveCall.Dispose();
            }
        }

        public void StartUnary(TRequest request)
        {
            //ActiveCall.StartUnaryCore(new PushUnaryContent<TRequest, TResponse>(stream => WriteNewMessage(ActiveCall, stream, ActiveCall.Options, request)));
            _ = StartRetry();
        }

        public void StartClientStreaming()
        {
            //ActiveCall.StartClientStreaming();
            _ = StartRetry();
        }

        public void StartServerStreaming(TRequest request)
        {
            //ActiveCall.StartServerStreamingCore(new PushUnaryContent<TRequest, TResponse>(stream => WriteNewMessage(ActiveCall, stream, ActiveCall.Options, request)));
            _ = StartRetry();
        }

        public void StartDuplexStreaming()
        {
            //ActiveCall.StartDuplexStreaming();
            _ = StartRetry();
        }
    }
}
