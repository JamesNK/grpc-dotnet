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
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client.Configuration;

namespace Grpc.Net.Client.Internal.Retry
{
    internal sealed class RetryCall<TRequest, TResponse> : RetryCallBase<TRequest, TResponse>
        where TRequest : class
        where TResponse : class
    {
        // Getting logger name from generic type is slow. Cached copy.
        private const string LoggerName = "Grpc.Net.Client.Internal.RetryCall";

        private readonly RetryPolicy _retryPolicy;

        private readonly Random _random;

        private readonly CancellationTokenSource _retryCts = new CancellationTokenSource();

        private int _bufferedMessagesIndex;
        private int _attemptCount;
        private int _nextRetryDelayMilliseconds;
        private TaskCompletionSource<bool> _canRetryTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        private GrpcCall<TRequest, TResponse> _activeCall;

        protected override async ValueTask WriteBufferedMessages(GrpcCall<TRequest, TResponse> call, Stream writeStream, CancellationToken cancellationToken)
        {
            while (_bufferedMessagesIndex < WrittenMessages.Count)
            {
                var writtenMessage = WrittenMessages[_bufferedMessagesIndex];

                await call.WriteMessageAsync(writeStream, writtenMessage, cancellationToken).ConfigureAwait(false);
                _bufferedMessagesIndex++;
            }
        }

        public RetryCall(RetryPolicy retryPolicy, GrpcChannel channel, Method<TRequest, TResponse> method, CallOptions options)
            : base(channel, method, options, LoggerName)
        {
            _retryPolicy = retryPolicy;

            _random = new Random();
            _attemptCount = 1;
            _activeCall = HttpClientCallInvoker.CreateGrpcCall<TRequest, TResponse>(Channel, Method, Options, previousAttempts: 0);

            ValidatePolicy(retryPolicy);

            _nextRetryDelayMilliseconds = Convert.ToInt32(retryPolicy.InitialBackoff.GetValueOrDefault().TotalMilliseconds);
        }

        private void ValidatePolicy(RetryPolicy retryPolicy)
        {
            if (retryPolicy.MaxAttempts == null)
            {
                throw CreateException(Method, RetryPolicy.MaxAttemptsPropertyName);
            }
            if (retryPolicy.InitialBackoff == null)
            {
                throw CreateException(Method, RetryPolicy.InitialBackoffPropertyName);
            }
            if (retryPolicy.MaxBackoff == null)
            {
                throw CreateException(Method, RetryPolicy.MaxBackoffPropertyName);
            }
            if (retryPolicy.BackoffMultiplier == null)
            {
                throw CreateException(Method, RetryPolicy.BackoffMultiplierPropertyName);
            }
            if (retryPolicy.RetryableStatusCodes.Count == 0)
            {
                throw new InvalidOperationException($"Retry policy for '{Method.FullName}' must have property '{RetryPolicy.RetryableStatusCodesPropertyName}' and must be non-empty.");
            }

            static InvalidOperationException CreateException(IMethod method, string propertyName)
            {
                return new InvalidOperationException($"Retry policy for '{method.FullName}' is missing required property '{propertyName}'.");
            }
        }

        public async Task<bool> ResolveRetryTask(GrpcCall<TRequest, TResponse> call)
        {
            Task<bool> canRetryTask;
            lock (Lock)
            {
                // New call has already been made
                if (call != _activeCall)
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
                Debug.Assert(call != _activeCall);
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
            if (Options.Deadline != null && Options.Deadline <= Channel.Clock.UtcNow)
            {
                return RetryResult.DeadlineExceeded;
            }

            if (Channel.RetryThrottling?.IsRetryThrottlingActive() ?? false)
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

            if (!HasResponseHeaderStatus(_activeCall))
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

        private static bool HasResponseHeaderStatus(GrpcCall<TRequest, TResponse> call)
        {
            return call.HttpResponse != null &&
                GrpcProtocolHelpers.GetHeaderValue(call.HttpResponse.Headers, GrpcProtocolConstants.StatusTrailer) != null;
        }

        private async Task StartRetry(Action<GrpcCall<TRequest, TResponse>> startCallFunc)
        {
            startCallFunc(_activeCall);

            // This is the main retry loop. It will:
            // 1. Check the result of the active call was successful.
            // 2. If it was unsuccessful then evaluate if the call can be retried.
            // 3. If it can be retried then start a new active call and begin again.
            while (true)
            {
                Status? responseStatus;

                try
                {
                    _activeCall.CancellationToken.ThrowIfCancellationRequested();

                    Debug.Assert(_activeCall._httpResponseTask != null, "Request should have be made if call is not preemptively cancelled.");
                    var httpResponse = await _activeCall._httpResponseTask.ConfigureAwait(false);

                    responseStatus = GrpcCall.ValidateHeaders(httpResponse, out _);
                }
                catch (Exception ex)
                {
                    _activeCall.ResolveException(GrpcCall<TRequest, TResponse>.ErrorStartingCallMessage, ex, out responseStatus, out _);
                }

                // Check to see the response returned from the server makes the call commited
                // Null status code indicates the headers were valid and a "Response-Headers" response
                // was received from the server.
                // https://github.com/grpc/proposal/blob/master/A6-client-retries.md#when-retries-are-valid
                if (responseStatus == null)
                {
                    // Headers were returned. We're commited.
                    FinalizedCallTcs.SetResult(_activeCall);

                    responseStatus = await _activeCall.CallTask.ConfigureAwait(false);
                    if (responseStatus.GetValueOrDefault().StatusCode == StatusCode.OK)
                    {
                        // Success. Exit retry loop.
                        Channel.RetryThrottling?.CallSuccess();
                    }
                    return;
                }

                Status status = responseStatus.Value;

                try
                {
                    var retryPushbackMS = GetRetryPushback(_activeCall);

                    // Failures only could towards retry throttling if they have a known, retriable status.
                    // This stops non-transient statuses, e.g. INVALID_ARGUMENT, from triggering throttling.
                    if (_retryPolicy.RetryableStatusCodes.Contains(status.StatusCode) ||
                        retryPushbackMS < 0)
                    {
                        Channel.RetryThrottling?.CallFailure();
                    }

                    var result = EvaluateRetry(status, retryPushbackMS);
                    Log.RetryEvaluated(Logger, status.StatusCode, _attemptCount, result);

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

                        Log.StartingRetryDelay(Logger, delayDuration);
                        await Task.Delay(delayDuration, _retryCts.Token).ConfigureAwait(false);

                        _nextRetryDelayMilliseconds = CalculateNextRetryDelay();

                        lock (Lock)
                        {
                            // Check if dispose was called on call.
                            _retryCts.Token.ThrowIfCancellationRequested();

                            // Clean up the failed call.
                            _activeCall.Dispose();

                            // Start new call.
                            _bufferedMessagesIndex = 0;
                            _activeCall = HttpClientCallInvoker.CreateGrpcCall<TRequest, TResponse>(Channel, Method, Options, _attemptCount);
                            startCallFunc(_activeCall);

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
                        FinalizedCallTcs.TrySetResult(_activeCall);
                        return;
                    }
                }
                catch (Exception ex)
                {
                    // Cancellation token triggered by dispose could throw here. Only log unexpected errors.
                    if (ex is not OperationCanceledException || !_retryCts.IsCancellationRequested)
                    {
                        Log.ErrorRetryingCall(Logger, ex);
                    }

                    _canRetryTcs.TrySetResult(false);
                    FinalizedCallTcs.TrySetResult(_activeCall);
                    return;
                }
            }
        }

        public override void Dispose()
        {
            lock (Lock)
            {
                _retryCts.Cancel();
                _activeCall.Dispose();
            }
        }

        protected override void StartCore(Action<GrpcCall<TRequest, TResponse>> startCallFunc)
        {
            _ = StartRetry(startCallFunc);
        }

        public override async Task ClientStreamCompleteAsync()
        {
            ClientStreamComplete = true;
            while (true)
            {
                GrpcCall<TRequest, TResponse> call = _activeCall;
                try
                {
                    await call.ClientStreamWriter!.CompleteAsync().ConfigureAwait(false);
                    return;
                }
                catch
                {
                    if (!await ResolveRetryTask(call).ConfigureAwait(false))
                    {
                        throw;
                    }
                }
            }
        }

        public override async Task ClientStreamWriteAsync(TRequest message)
        {
            while (true)
            {
                GrpcCall<TRequest, TResponse> call = _activeCall;
                try
                {
                    var writer = call.ClientStreamWriter!;
                    if (ClientStreamWriteOptions != null)
                    {
                        writer.WriteOptions = ClientStreamWriteOptions;
                    }

                    await writer.WriteAsync(WriteNewMessage, message).ConfigureAwait(false);
                    BufferedCurrentMessage = false;

                    if (ClientStreamComplete)
                    {
                        await writer.CompleteAsync().ConfigureAwait(false);
                    }
                    return;
                }
                catch
                {
                    if (!await ResolveRetryTask(call).ConfigureAwait(false))
                    {
                        throw;
                    }
                }
            }
        }
    }
}
