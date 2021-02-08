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

#if true

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

        private readonly object _lock = new object();
        //private readonly CancellationTokenSource _retryCts = new CancellationTokenSource();
        private readonly TaskCompletionSource<GrpcCall<TRequest, TResponse>> _finalizedCallTcs;
        internal Timer? _createCallTimer;

        //private int _bufferedMessagesIndex;
        //private int _attemptCount;
        private HedgingClientStreamReader<TRequest, TResponse>? _retryClientStreamReader;
        private HedgingClientStreamWriter<TRequest, TResponse>? _retryClientStreamWriter;

        public Task<GrpcCall<TRequest, TResponse>> FinalizedCallTask => _finalizedCallTcs.Task;
        public bool BufferedCurrentMessage { get; set; }
        public List<GrpcCall<TRequest, TResponse>> ActiveCalls { get; }
        public IClientStreamWriter<TRequest>? ClientStreamWriter => _retryClientStreamWriter ??= new HedgingClientStreamWriter<TRequest, TResponse>(this);
        public IAsyncStreamReader<TResponse>? ClientStreamReader => _retryClientStreamReader ??= new HedgingClientStreamReader<TRequest, TResponse>(this);

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

            await WriteBufferedMessages(call, writeStream, callOptions.CancellationToken).ConfigureAwait(false);
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

        public HedgingCall(HedgingPolicy hedgingPolicy, GrpcChannel channel, Method<TRequest, TResponse> method, CallOptions options)
        {
            _logger = channel.LoggerFactory.CreateLogger(LoggerName);
            _hedgingPolicy = hedgingPolicy;
            _channel = channel;
            _method = method;
            _options = options;
            _writtenMessages = new List<ReadOnlyMemory<byte>>();
            //_attemptCount = 1;
            ActiveCalls = new List<GrpcCall<TRequest, TResponse>>();
            _finalizedCallTcs = new TaskCompletionSource<GrpcCall<TRequest, TResponse>>(TaskCreationOptions.RunContinuationsAsynchronously);

            ValidatePolicy(hedgingPolicy);
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

        private async Task StartCall(Action<GrpcCall<TRequest, TResponse>> startCallFunc)
        {
            GrpcCall<TRequest, TResponse> call;
            lock (_lock)
            {
                call = HttpClientCallInvoker.CreateGrpcCall<TRequest, TResponse>(_channel, _method, _options, _callsAttempted);
                ActiveCalls.Add(call);
                _callsAttempted++;
            }

            startCallFunc(call);

            Status? responseStatus;

            try
            {
                call.CancellationToken.ThrowIfCancellationRequested();

                Debug.Assert(call._httpResponseTask != null, "Request should have be made if call is not preemptively cancelled.");
                var httpResponse = await call._httpResponseTask.ConfigureAwait(false);

                responseStatus = GrpcCall.ValidateHeaders(httpResponse, out _);
            }
            catch (Exception ex)
            {
                call.ResolveException(GrpcCall<TRequest, TResponse>.ErrorStartingCallMessage, ex, out responseStatus, out _);
            }

            // Check to see the response returned from the server makes the call commited
            // Null status code indicates the headers were valid and a "Response-Headers" response
            // was received from the server.
            // https://github.com/grpc/proposal/blob/master/A6-client-retries.md#when-retries-are-valid
            if (responseStatus == null)
            {
                // Headers were returned. We're commited.
                FinalizeCall(call);

                responseStatus = await call.CallTask.ConfigureAwait(false);
                if (responseStatus.GetValueOrDefault().StatusCode == StatusCode.OK)
                {
                    // Success. Exit retry loop.
                    _channel.RetryThrottling?.CallSuccess();
                }
            }
            else
            {
                Status status = responseStatus.Value;

                var retryPushbackMS = GetRetryPushback(call);

                if (retryPushbackMS < 0)
                {
                    _channel.RetryThrottling?.CallFailure();
                }
                else if (_hedgingPolicy.NonFatalStatusCodes.Contains(status.StatusCode))
                {
                    if (retryPushbackMS >= 0)
                    {
                        lock (_lock)
                        {
                            _createCallTimer?.Change(
                                TimeSpan.FromMilliseconds(retryPushbackMS.GetValueOrDefault()),
                                _hedgingPolicy.HedgingDelay.GetValueOrDefault());
                        }
                    }
                    _channel.RetryThrottling?.CallFailure();
                }
                else
                {
                    FinalizeCall(call);
                }
            }

            lock (_lock)
            {
                // This is the last active call and no more will be made.
                if (ActiveCalls.Count == 1 && _callsAttempted >= _hedgingPolicy.MaxAttempts.GetValueOrDefault())
                {
                    FinalizeCall(call);
                }
                else
                {
                    var removed = ActiveCalls.Remove(call);
                    Debug.Assert(removed);

                    call.Dispose();
                }
            }
        }

        private void FinalizeCall(GrpcCall<TRequest, TResponse> call)
        {
            lock (_lock)
            {
                var removed = ActiveCalls.Remove(call);
                Debug.Assert(removed);

                _finalizedCallTcs.SetResult(call);

                CleanUp();
            }
        }

        private void CleanUp()
        {
            // Stop creating calls and cancel all others in progress.
            _createCallTimer?.Dispose();
            _createCallTimer = null;
            while (ActiveCalls.Count > 0)
            {
                ActiveCalls[ActiveCalls.Count - 1].Dispose();
                ActiveCalls.RemoveAt(ActiveCalls.Count - 1);
            }
        }

        private int _callsAttempted;

        private void StartCalls(Action<GrpcCall<TRequest, TResponse>> startCallFunc)
        {
            var hedgingDelay = _hedgingPolicy.HedgingDelay.GetValueOrDefault();
            if (hedgingDelay == TimeSpan.Zero)
            {
                // If there is no delay then start all call immediately
                while (_callsAttempted < _hedgingPolicy.MaxAttempts.GetValueOrDefault())
                {
                    _ = StartCall(startCallFunc);
                }
            }
            else
            {
                // Create timer and set to field before setting time.
                // Ensures there is no weird situation where the timer triggers
                // before the field is set. 
                _createCallTimer = new Timer(CreateCallCallback, startCallFunc, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                _createCallTimer.Change(TimeSpan.Zero, hedgingDelay);
            }
        }

        private void CreateCallCallback(object? state)
        {
            lock (_lock)
            {
                if (_callsAttempted < _hedgingPolicy.MaxAttempts.GetValueOrDefault())
                {
                    _ = StartCall((Action<GrpcCall<TRequest, TResponse>>)state!);
                }
                else
                {
                    _createCallTimer?.Dispose();
                    _createCallTimer = null;
                }
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

        public async Task<TResponse> GetResponseAsync()
        {
            var call = await _finalizedCallTcs.Task.ConfigureAwait(false);
            return await call.GetResponseAsync().ConfigureAwait(false);
        }

        public async Task<Metadata> GetResponseHeadersAsync()
        {
            var call = await _finalizedCallTcs.Task.ConfigureAwait(false);
            return await call.GetResponseHeadersAsync().ConfigureAwait(false);
        }

        public Status GetStatus()
        {
            if (_finalizedCallTcs.Task.IsCompletedSuccessfully)
            {
                return _finalizedCallTcs.Task.Result.GetStatus();
            }

            throw new InvalidOperationException("Unable to get the status because the call is not complete.");
        }

        public Metadata GetTrailers()
        {
            if (_finalizedCallTcs.Task.IsCompletedSuccessfully)
            {
                return _finalizedCallTcs.Task.Result.GetTrailers();
            }

            throw new InvalidOperationException("Can't get the call trailers because the call has not completed successfully.");
        }

        public void Dispose()
        {
            lock (_lock)
            {
                CleanUp();
            }
        }

        private GrpcCall<TRequest, TResponse> CreateRetryCall(bool clientStreamCompleted, int previousAttempts)
        {
            var call = HttpClientCallInvoker.CreateGrpcCall<TRequest, TResponse>(_channel, _method, _options, previousAttempts);
            call.StartRetry(clientStreamCompleted, async requestStream =>
            {
                //Log.SendingBufferedMessages(_logger, _writtenMessages.Count);

                await WriteBufferedMessages(call, requestStream, call.CancellationToken).ConfigureAwait(false);

                if (clientStreamCompleted)
                {
                    await call.ClientStreamWriter!.CompleteAsync().ConfigureAwait(false);
                }
            });

            return call;
        }

        public void StartUnary(TRequest request)
        {
            StartCalls(call =>
            {
                call.StartUnaryCore(new PushUnaryContent<TRequest, TResponse>(stream => WriteNewMessage(call, stream, call.Options, request)));
            });
        }

        public void StartClientStreaming()
        {
            //ActiveCall.StartClientStreaming();
            //StartCalls();
        }

        public void StartServerStreaming(TRequest request)
        {
            StartCalls(call =>
            {
                call.StartServerStreamingCore(new PushUnaryContent<TRequest, TResponse>(stream => WriteNewMessage(call, stream, call.Options, request)));
            });
        }

        public void StartDuplexStreaming()
        {
            //ActiveCall.StartDuplexStreaming();
            //StartCalls();
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

        internal async ValueTask WriteBufferedMessages(GrpcCall<TRequest, TResponse> call, Stream writeStream, CancellationToken cancellationToken)
        {
            foreach (var writtenMessage in _writtenMessages)
            {
                await call.WriteMessageAsync(writeStream, writtenMessage, cancellationToken).ConfigureAwait(false);
            }
            //while (_bufferedMessagesIndex < _writtenMessages.Count)
            //{
            //    var writtenMessage = _writtenMessages[_bufferedMessagesIndex];

            //    await call.WriteMessageAsync(writeStream, writtenMessage, cancellationToken).ConfigureAwait(false);
            //    _bufferedMessagesIndex++;
            //}
        }
    }
}
#endif