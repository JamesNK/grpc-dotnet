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
    internal partial class RetryCall<TRequest, TResponse> : IGrpcCall<TRequest, TResponse>
        where TRequest : class
        where TResponse : class
    {
        // Getting logger name from generic type is slow. Cached copy.
        private const string LoggerName = "Grpc.Net.Client.Internal.RetryCall";

        private readonly ILogger _logger;
        private readonly RetryPolicy _retryPolicy;

        private readonly GrpcChannel _channel;
        private readonly Method<TRequest, TResponse> _method;
        private readonly CallOptions _options;
        private readonly List<ReadOnlyMemory<byte>> _writtenMessages;
        private readonly Random _random;

        private readonly object _lock = new object();
        private readonly CancellationTokenSource _retryCts = new CancellationTokenSource();
        private readonly TaskCompletionSource<GrpcCall<TRequest, TResponse>> _finalizedCallTcs;

        private int _bufferedMessagesIndex;
        private int _attemptCount;
        private int _nextRetryDelayMilliseconds;
        private RetryClientStreamReader<TRequest, TResponse>? _retryClientStreamReader;
        private RetryClientStreamWriter<TRequest, TResponse>? _retryClientStreamWriter;
        private TaskCompletionSource<bool> _canRetryTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<GrpcCall<TRequest, TResponse>> FinalizedCallTask => _finalizedCallTcs.Task;
        public bool BufferedCurrentMessage { get; set; }
        public GrpcCall<TRequest, TResponse> ActiveCall { get; private set; }
        public IClientStreamWriter<TRequest>? ClientStreamWriter => _retryClientStreamWriter ??= new RetryClientStreamWriter<TRequest, TResponse>(this);
        public IAsyncStreamReader<TResponse>? ClientStreamReader => _retryClientStreamReader ??= new RetryClientStreamReader<TRequest, TResponse>(this);

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

                await ActiveCall.WriteMessageAsync(writeStream, writtenMessage, cancellationToken).ConfigureAwait(false);
                _bufferedMessagesIndex++;
            }
        }

        public RetryCall(RetryPolicy retryPolicy, GrpcChannel channel, Method<TRequest, TResponse> method, CallOptions options)
        {
            _logger = channel.LoggerFactory.CreateLogger(LoggerName);
            _retryPolicy = retryPolicy;
            _channel = channel;
            _method = method;
            _options = options;
            _writtenMessages = new List<ReadOnlyMemory<byte>>();
            _random = new Random();
            _attemptCount = 1;
            ActiveCall = HttpClientCallInvoker.CreateGrpcCall<TRequest, TResponse>(_channel, _method, _options, previousAttempts: 0);
            _finalizedCallTcs = new TaskCompletionSource<GrpcCall<TRequest, TResponse>>(TaskCreationOptions.RunContinuationsAsynchronously);

            ValidatePolicy(retryPolicy);

            _nextRetryDelayMilliseconds = Convert.ToInt32(retryPolicy.InitialBackoff.GetValueOrDefault().TotalMilliseconds);
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
                Log.SendingBufferedMessages(_logger, _writtenMessages.Count);

                await WriteBufferedMessages(requestStream, call.CancellationToken).ConfigureAwait(false);

                if (clientStreamCompleted)
                {
                    await call.ClientStreamWriter!.CompleteAsync().ConfigureAwait(false);
                }
            });

            return call;
        }

        public async Task<bool> ResolveRetryTask(GrpcCall<TRequest, TResponse> call)
        {
            Task<bool> canRetryTask;
            lock (_lock)
            {
                // New call has already been made
                if (call != ActiveCall)
                {
                    return true;
                }

                // Wait to see whether new call will be made
                canRetryTask = _canRetryTcs.Task;
            }

            var canRetry = await canRetryTask.ConfigureAwait(false);
            if (canRetry)
            {
                // Verify a new call has been made
                Debug.Assert(call != ActiveCall);
                return true;
            }

            return false;
        }

        private int CalculateNextRetryDelay()
        {
            var nextMilliseconds = _nextRetryDelayMilliseconds * _retryPolicy.BackoffMultiplier.GetValueOrDefault();
            nextMilliseconds = Math.Min(nextMilliseconds, _retryPolicy.MaxBackoff.GetValueOrDefault().TotalMilliseconds);

            return Convert.ToInt32(nextMilliseconds);
        }

        private RetryResult EvaluateRetry(Status status, int? retryPushbackMilliseconds)
        {
            if (_options.Deadline != null && _options.Deadline <= _channel.Clock.UtcNow)
            {
                return RetryResult.DeadlineExceeded;
            }

            if (_channel.RetryThrottling?.IsRetryThrottlingActive() ?? false)
            {
                return RetryResult.Throttled;
            }

            if (_attemptCount >= _retryPolicy.MaxAttempts.GetValueOrDefault())
            {
                return RetryResult.ExceededAttemptCount;
            }

            if (retryPushbackMilliseconds != null)
            {
                if (retryPushbackMilliseconds >= 0)
                {
                    return RetryResult.Retry;
                }
                else
                {
                    return RetryResult.PushbackStop;
                }
            }

            if (!HasResponseHeaderStatus(ActiveCall))
            {
                // If a HttpResponse has been received and it's not a "trailers only" response (contains status in header)
                // then headers were returned before failure. The call is commited and can't be retried.
                return RetryResult.CallCommited;
            }

            if (!_retryPolicy.RetryableStatusCodes.Contains(status.StatusCode))
            {
                return RetryResult.NotRetryableStatusCode;
            }

            return RetryResult.Retry;
        }

        private enum RetryResult
        {
            Retry,
            ExceededAttemptCount,
            CallCommited,
            NotRetryableStatusCode,
            PushbackStop,
            Throttled,
            DeadlineExceeded
        }

        private static bool HasResponseHeaderStatus(GrpcCall<TRequest, TResponse> call)
        {
            return call.HttpResponse != null &&
                GrpcProtocolHelpers.GetHeaderValue(call.HttpResponse.Headers, GrpcProtocolConstants.StatusTrailer) != null;
        }

        private async Task StartRetry()
        {
            // This is the main retry loop. It will:
            // 1. Check the result of the active call was successful.
            // 2. If it was unsuccessful then evaluate if the call can be retried.
            // 3. If it can be retried then start a new active call and begin again.
            while (true)
            {
                Status? responseStatus;

                try
                {
                    ActiveCall.CancellationToken.ThrowIfCancellationRequested();

                    Debug.Assert(ActiveCall._httpResponseTask != null, "Request should have be made if call is not preemptively cancelled.");
                    var httpResponse = await ActiveCall._httpResponseTask.ConfigureAwait(false);

                    responseStatus = GrpcCall.ValidateHeaders(httpResponse, out _);
                }
                catch (Exception ex)
                {
                    ActiveCall.ResolveException(GrpcCall<TRequest, TResponse>.ErrorStartingCallMessage, ex, out responseStatus, out _);
                }

                // Check to see the response returned from the server makes the call commited
                // Null status code indicates the headers were valid and a "Response-Headers" response
                // was received from the server.
                // https://github.com/grpc/proposal/blob/master/A6-client-retries.md#when-retries-are-valid
                if (responseStatus == null)
                {
                    // Headers were returned. We're commited.
                    _finalizedCallTcs.SetResult(ActiveCall);

                    responseStatus = await ActiveCall.CallTask.ConfigureAwait(false);
                    if (responseStatus.GetValueOrDefault().StatusCode == StatusCode.OK)
                    {
                        // Success. Exit retry loop.
                        _channel.RetryThrottling?.CallSuccess();
                    }
                    return;
                }

                Status status = responseStatus.Value;

                try
                {
                    var retryPushbackMS = GetRetryPushback();

                    // Failures only could towards retry throttling if they have a known, retriable status.
                    // This stops non-transient statuses, e.g. INVALID_ARGUMENT, from triggering throttling.
                    if (_retryPolicy.RetryableStatusCodes.Contains(status.StatusCode) ||
                        retryPushbackMS < 0)
                    {
                        _channel.RetryThrottling?.CallFailure();
                    }

                    var result = EvaluateRetry(status, retryPushbackMS);
                    Log.RetryEvaluated(_logger, status.StatusCode, _attemptCount, result);

                    if (result == RetryResult.Retry)
                    {
                        TimeSpan delayDuration;
                        if (retryPushbackMS != null)
                        {
                            delayDuration = TimeSpan.FromMilliseconds(retryPushbackMS.GetValueOrDefault());
                            _nextRetryDelayMilliseconds = retryPushbackMS.GetValueOrDefault();
                        }
                        else
                        {
                            delayDuration = TimeSpan.FromMilliseconds(_random.Next(0, Convert.ToInt32(_nextRetryDelayMilliseconds)));
                        }

                        Log.StartingRetryDelay(_logger, delayDuration);
                        await Task.Delay(delayDuration, _retryCts.Token).ConfigureAwait(false);

                        _nextRetryDelayMilliseconds = CalculateNextRetryDelay();

                        lock (_lock)
                        {
                            // Check if dispose was called on call.
                            _retryCts.Token.ThrowIfCancellationRequested();

                            // Clean up the failed call.
                            ActiveCall.Dispose();

                            // Start new call.
                            _bufferedMessagesIndex = 0;
                            ActiveCall = CreateRetryCall(ActiveCall.ClientStreamWriter?.CompleteCalled ?? false, _attemptCount);
                            _attemptCount++;

                            // Signal any calls to public APIs (e.g. ResponseAsync, MoveNext, WriteAsync)
                            // that have thrown and are caught in catch blocks and are waiting for the call to retry.
                            _canRetryTcs.TrySetResult(true);

                            // Create a new TCS for future failures.
                            _canRetryTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                        }
                    }
                    else
                    {
                        // Can't retry.
                        // Signal public API exceptions that they should finish throwing and then exit the retry loop.
                        _canRetryTcs.TrySetResult(false);
                        _finalizedCallTcs.TrySetResult(ActiveCall);
                        return;
                    }
                }
                catch (Exception ex)
                {
                    // Cancellation token triggered by dispose could throw here. Only log unexpected errors.
                    if (ex is not OperationCanceledException || !_retryCts.IsCancellationRequested)
                    {
                        Log.ErrorRetryingCall(_logger, ex);
                    }

                    _canRetryTcs.TrySetResult(false);
                    _finalizedCallTcs.TrySetResult(ActiveCall);
                    return;
                }
            }
        }

        private int? GetRetryPushback()
        {
            // https://github.com/grpc/proposal/blob/master/A6-client-retries.md#pushback
            if (ActiveCall.HttpResponse != null)
            {
                if (ActiveCall.HttpResponse.Headers.TryGetValues(GrpcProtocolConstants.RetryPushbackHeader, out var values))
                {
                    var headerValue = values.Single();
                    Log.RetryPushbackReceived(_logger, headerValue);

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
                _retryCts.Cancel();
                ActiveCall.Dispose();
            }
        }

        public void StartUnary(TRequest request)
        {
            ActiveCall.StartUnaryCore(new PushUnaryContent<TRequest, TResponse>(stream => WriteNewMessage(ActiveCall, stream, ActiveCall.Options, request)));
            _ = StartRetry();
        }

        public void StartClientStreaming()
        {
            ActiveCall.StartClientStreaming();
            _ = StartRetry();
        }

        public void StartServerStreaming(TRequest request)
        {
            ActiveCall.StartServerStreamingCore(new PushUnaryContent<TRequest, TResponse>(stream => WriteNewMessage(ActiveCall, stream, ActiveCall.Options, request)));
            _ = StartRetry();
        }

        public void StartDuplexStreaming()
        {
            ActiveCall.StartDuplexStreaming();
            _ = StartRetry();
        }

        //internal Task ClientStreamCompleteAsync()
        //{
        //    throw new NotImplementedException();
        //}

        //internal Task ClientStreamWriteAsync<TRequest>(TRequest message) where TRequest : class
        //{
        //    throw new NotImplementedException();
        //}
    }
}
