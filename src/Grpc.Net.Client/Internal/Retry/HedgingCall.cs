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

#if false

namespace Grpc.Net.Client.Internal.Retry
{
    internal partial class HedgingCall<TRequest, TResponse> : IGrpcCall<TRequest, TResponse>
        where TRequest : class
        where TResponse : class
    {
        // Getting logger name from generic type is slow. Cached copy.
        private const string LoggerName = "Grpc.Net.Client.Internal.HedgingCall";

        private readonly ILogger _logger;
        private readonly HedgingPolicy _hedgingPolicy;

        private readonly GrpcChannel _channel;
        private readonly Method<TRequest, TResponse> _method;
        private readonly CallOptions _options;
        private readonly List<ReadOnlyMemory<byte>> _writtenMessages;
        private readonly Random _random;
        private readonly object _lock = new object();
        private readonly CancellationTokenSource _retryCts = new CancellationTokenSource();

        private int _bufferedMessagesIndex;
        private int _attemptCount;
        private int _nextRetryDelayMilliseconds;
        private RetryClientStreamReader<TRequest, TResponse>? _retryClientStreamReader;
        private HedgingClientStreamWriter<TRequest, TResponse>? _retryClientStreamWriter;

        public bool BufferedCurrentMessage { get; set; }
        public List<GrpcCall<TRequest, TResponse>> ActiveCalls { get; }
        public IClientStreamWriter<TRequest>? ClientStreamWriter => _retryClientStreamWriter ??= new HedgingClientStreamWriter<TRequest, TResponse>(this);
        public IAsyncStreamReader<TResponse>? ClientStreamReader => _retryClientStreamReader ??= new RetryClientStreamReader<TRequest, TResponse>(this);

        public WriteOptions? ClientStreamWriteOptions { get; internal set; }

        public async ValueTask WriteNewMessage(GrpcCall<TRequest, TResponse> call, Stream writeStream, CallOptions callOptions, TRequest message)
        {
            // Serialize current message and add to the buffer.
            if (!BufferedCurrentMessage)
            {
                lock (_lock)
                {
                    if (!BufferedCurrentMessage)
                    {
                        var payload = SerializePayload(call, callOptions, message);
                        _writtenMessages.Add(payload);
                        BufferedCurrentMessage = true;
                    }
                }
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

                await ActiveCall.WriteMessageAsync(writeStream, writtenMessage, cancellationToken).ConfigureAwait(false);
                _bufferedMessagesIndex++;
            }
        }

        public HedgingCall(HedgingPolicy hedgingPolicy, GrpcChannel channel, Method<TRequest, TResponse> method, CallOptions options)
        {
            _logger = channel.LoggerFactory.CreateLogger(LoggerName);
            _hedgingPolicy = hedgingPolicy;
            _channel = channel;
            _method = method;
            _options = options;
            _writtenMessages = new List<ReadOnlyMemory<byte>>();
            _random = new Random();
            _attemptCount = 1;
            ActiveCalls = new List<GrpcCall<TRequest, TResponse>>();

            ValidatePolicy(hedgingPolicy);

            _nextRetryDelayMilliseconds = Convert.ToInt32(hedgingPolicy.HedgingDelay.GetValueOrDefault().TotalMilliseconds);
        }

        private void ValidatePolicy(HedgingPolicy hedgingPolicy)
        {
            //if (retryThrottlingPolicy.MaxAttempts == null)
            //{
            //    throw CreateException(_method, RetryPolicy.MaxAttemptsPropertyName);
            //}
            //if (retryThrottlingPolicy.InitialBackoff == null)
            //{
            //    throw CreateException(_method, RetryPolicy.InitialBackoffPropertyName);
            //}
            //if (retryThrottlingPolicy.MaxBackoff == null)
            //{
            //    throw CreateException(_method, RetryPolicy.MaxBackoffPropertyName);
            //}
            //if (retryThrottlingPolicy.BackoffMultiplier == null)
            //{
            //    throw CreateException(_method, RetryPolicy.BackoffMultiplierPropertyName);
            //}
            //if (retryThrottlingPolicy.RetryableStatusCodes.Count == 0)
            //{
            //    throw new InvalidOperationException($"Retry policy for '{_method.FullName}' must have property '{RetryPolicy.RetryableStatusCodesPropertyName}' and must be non-empty.");
            //}

            //static InvalidOperationException CreateException(IMethod method, string propertyName)
            //{
            //    return new InvalidOperationException($"Retry policy for '{method.FullName}' is missing required property '{propertyName}'.");
            //}
        }

        private readonly TaskCompletionSource<GrpcCall<TRequest, TResponse>> _finalizedCall = new TaskCompletionSource<GrpcCall<TRequest, TResponse>>(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<TResponse> GetResponseAsync()
        {
            var call = await _finalizedCall.Task.ConfigureAwait(false);
            return await call.GetResponseAsync().ConfigureAwait(false);
        }

        private static bool HasResponseHeaderStatus(GrpcCall<TRequest, TResponse> call)
        {
            return call.HttpResponse != null &&
                GrpcProtocolHelpers.GetHeaderValue(call.HttpResponse.Headers, GrpcProtocolConstants.StatusTrailer) != null;
        }

        private Timer? _createCallTimer;

        private void CreateCallCallback(object state)
        {
            // TODO: What happen is the timer is really small and callback is already in flight when disposing?
            _ = StartCall();

            if (_callsAttempted >= _hedgingPolicy.MaxAttempts.GetValueOrDefault())
            {
                _createCallTimer!.Dispose();
            }
        }

        private async Task StartCall()
        {
            GrpcCall<TRequest, TResponse> call = null!;
            lock (_lock)
            {
                ActiveCalls.Add(call);
                _callsAttempted++;
            }

            var status = await call.CallTask.ConfigureAwait(false);
            if (status.StatusCode == StatusCode.OK)
            {
                // Success. Exit retry loop.
                _channel.RetryThrottling?.CallSuccess();
                return;
            }

            var retryPushbackMS = GetRetryPushback(call);

            if (_hedgingPolicy.NonFatalStatusCodes.Contains(status.StatusCode) ||
                retryPushbackMS < 0)
            {
                _channel.RetryThrottling?.CallFailure();
            }
            else
            {
                _finalizedCall.SetResult(call);
            }
        }

        private int _callsAttempted;

        private void StartCalls()
        {
            if (_hedgingPolicy.HedgingDelay.GetValueOrDefault() == TimeSpan.Zero)
            {
                while (_callsAttempted <= _hedgingPolicy.MaxAttempts.GetValueOrDefault())
                {
                    _ = StartCall();
                }
            }
            else
            {
                _createCallTimer = new Timer(CreateCallCallback, null, 100, 100);
            }
        }

        private int? GetRetryPushback(GrpcCall<TRequest, TResponse> call)
        {
            // https://github.com/grpc/proposal/blob/master/A6-client-retries.md#pushback
            if (call.HttpResponse != null)
            {
                if (call.HttpResponse.Headers.TryGetValues(GrpcProtocolConstants.RetryPushbackHeader, out var values))
                {
                    var headerValue = values.Single();
                    //Log.RetryPushbackReceived(_logger, headerValue);

                    // A non-integer value means the server wants retries to stop.
                    // Resolve non-integer value to a negative integer which also means stop.
                    return int.TryParse(headerValue, out var value) ? value : -1;
                }
            }

            return null;
        }

        public async Task<Metadata> GetResponseHeadersAsync()
        {
            var call = await _finalizedCall.Task.ConfigureAwait(false);
            call.GetStatus();
            return await call.GetResponseHeadersAsync().ConfigureAwait(false);
        }

        public Status GetStatus()
        {
            if (_finalizedCall.Task.IsCompletedSuccessfully)
            {
                return _finalizedCall.Task.Result.GetStatus();
            }

            throw new InvalidOperationException("Unable to get the status because the call is not complete.");
        }

        public Metadata GetTrailers()
        {
            if (_finalizedCall.Task.IsCompletedSuccessfully)
            {
                return _finalizedCall.Task.Result.GetTrailers();
            }

            throw new InvalidOperationException("Can't get the call trailers because the call has not completed successfully.");
        }

        public void Dispose()
        {
            lock (_lock)
            {
                _createCallTimer?.Dispose();

                for (var i = 0; i < ActiveCalls.Count; i++)
                {
                    ActiveCalls[i].Dispose();
                }
            }
        }

        private GrpcCall<TRequest, TResponse> CreateRetryCall(bool clientStreamCompleted, int previousAttempts)
        {
            var call = HttpClientCallInvoker.CreateGrpcCall<TRequest, TResponse>(_channel, _method, _options, previousAttempts);
            call.StartRetry(clientStreamCompleted, async requestStream =>
            {
                //Log.SendingBufferedMessages(_logger, _writtenMessages.Count);

                await WriteBufferedMessages(requestStream, call.CancellationToken).ConfigureAwait(false);

                if (clientStreamCompleted)
                {
                    await call.ClientStreamWriter!.CompleteAsync().ConfigureAwait(false);
                }
            });

            return call;
        }

        public void StartUnary(TRequest request)
        {
            //ActiveCall.StartUnaryCore(new PushUnaryContent<TRequest, TResponse>(stream => WriteNewMessage(ActiveCall, stream, ActiveCall.Options, request)));
            StartCalls();
        }

        public void StartClientStreaming()
        {
            //ActiveCall.StartClientStreaming();
            StartCalls();
        }

        public void StartServerStreaming(TRequest request)
        {
            //ActiveCall.StartServerStreamingCore(new PushUnaryContent<TRequest, TResponse>(stream => WriteNewMessage(ActiveCall, stream, ActiveCall.Options, request)));
            StartCalls();
        }

        public void StartDuplexStreaming()
        {
            //ActiveCall.StartDuplexStreaming();
            StartCalls();
        }

        internal Task ClientStreamCompleteAsync()
        {
            lock (_lock)
            {
                var completeTasks = new Task[ActiveCalls.Count];
                for (var i = 0; i < ActiveCalls.Count; i++)
                {
                    completeTasks[i] = ActiveCalls[i].ClientStreamWriter!.CompleteAsync();
                }

                return Task.WhenAll(completeTasks);
            }
        }

        internal async Task ClientStreamWriteAsync(TRequest message)
        {
            // TODO(JamesNK) - Not safe for multi-threading
            var writeTasks = new Task[ActiveCalls.Count];
            lock (_lock)
            {
                for (var i = 0; i < ActiveCalls.Count; i++)
                {
                    writeTasks[i] = ActiveCalls[i].ClientStreamWriter!.WriteAsync(WriteNewMessage, message);
                }
            }

            await Task.WhenAll(writeTasks).ConfigureAwait(false);
            BufferedCurrentMessage = false;
        }
    }
}
#endif