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
using Grpc.Shared;

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

        private int _attemptCount;
        private int _nextRetryDelayMilliseconds;

        private GrpcCall<TRequest, TResponse>? _activeCall;

        public RetryCall(RetryPolicy retryPolicy, GrpcChannel channel, Method<TRequest, TResponse> method, CallOptions options)
            : base(channel, method, options, LoggerName, retryPolicy.MaxAttempts.GetValueOrDefault())
        {
            _retryPolicy = retryPolicy;

            _random = new Random();

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

        private int CalculateNextRetryDelay()
        {
            var nextMilliseconds = _nextRetryDelayMilliseconds * _retryPolicy.BackoffMultiplier.GetValueOrDefault();
            nextMilliseconds = Math.Min(nextMilliseconds, _retryPolicy.MaxBackoff.GetValueOrDefault().TotalMilliseconds);

            return Convert.ToInt32(nextMilliseconds);
        }

        private CommitReason? EvaluateRetry(Status status, int? retryPushbackMilliseconds)
        {
            if (IsDeadlineExceeded())
            {
                return CommitReason.DeadlineExceeded;
            }

            if (Channel.RetryThrottling?.IsRetryThrottlingActive() ?? false)
            {
                return CommitReason.Throttled;
            }

            if (_attemptCount >= MaxRetryAttempts)
            {
                return CommitReason.ExceededAttemptCount;
            }

            if (retryPushbackMilliseconds != null)
            {
                if (retryPushbackMilliseconds >= 0)
                {
                    return null;
                }
                else
                {
                    return CommitReason.PushbackStop;
                }
            }

            if (!_retryPolicy.RetryableStatusCodes.Contains(status.StatusCode))
            {
                return CommitReason.FatalStatusCode;
            }

            return null;
        }

        private async Task StartRetry(Action<GrpcCall<TRequest, TResponse>> startCallFunc)
        {
            // This is the main retry loop. It will:
            // 1. Check the result of the active call was successful.
            // 2. If it was unsuccessful then evaluate if the call can be retried.
            // 3. If it can be retried then start a new active call and begin again.
            while (true)
            {
                GrpcCall<TRequest, TResponse> currentCall;
                lock (Lock)
                {
                    // Start new call.
                    currentCall = _activeCall = HttpClientCallInvoker.CreateGrpcCall<TRequest, TResponse>(Channel, Method, Options, _attemptCount);
                    startCallFunc(currentCall);

                    _attemptCount++;

                    SetNewActiveCall(currentCall);
                }

                Status? responseStatus;

                try
                {
                    currentCall.CancellationToken.ThrowIfCancellationRequested();

                    Debug.Assert(currentCall._httpResponseTask != null, "Request should have be made if call is not preemptively cancelled.");
                    var httpResponse = await currentCall._httpResponseTask.ConfigureAwait(false);

                    responseStatus = GrpcCall.ValidateHeaders(httpResponse, out _);
                }
                catch (Exception ex)
                {
                    currentCall.ResolveException(GrpcCall<TRequest, TResponse>.ErrorStartingCallMessage, ex, out responseStatus, out _);
                }

                // Check to see the response returned from the server makes the call commited
                // Null status code indicates the headers were valid and a "Response-Headers" response
                // was received from the server.
                // https://github.com/grpc/proposal/blob/master/A6-client-retries.md#when-retries-are-valid
                if (responseStatus == null)
                {
                    // Headers were returned. We're commited.
                    CommitCall(currentCall, CommitReason.ResponseHeadersReceived);

                    responseStatus = await currentCall.CallTask.ConfigureAwait(false);
                    if (responseStatus.GetValueOrDefault().StatusCode == StatusCode.OK)
                    {
                        // Success. Exit retry loop.
                        Channel.RetryThrottling?.CallSuccess();
                    }
                    return;
                }

                if (FinalizedCallTask.IsCompletedSuccessfully)
                {
                    // Call has already been commited. This could happen if written messages exceed
                    // buffer limits, which causes the call to immediately become commited and to clear buffers.
                    return;
                }

                Status status = responseStatus.Value;

                try
                {
                    var retryPushbackMS = GetRetryPushback(currentCall);

                    // Failures only could towards retry throttling if they have a known, retriable status.
                    // This stops non-transient statuses, e.g. INVALID_ARGUMENT, from triggering throttling.
                    if (_retryPolicy.RetryableStatusCodes.Contains(status.StatusCode) ||
                        retryPushbackMS < 0)
                    {
                        Channel.RetryThrottling?.CallFailure();
                    }

                    var result = EvaluateRetry(status, retryPushbackMS);
                    Log.RetryEvaluated(Logger, status.StatusCode, _attemptCount, result == null);

                    if (result == null)
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
                        await Task.Delay(delayDuration, CancellationTokenSource.Token).ConfigureAwait(false);

                        _nextRetryDelayMilliseconds = CalculateNextRetryDelay();

                        // Check if dispose was called on call.
                        CancellationTokenSource.Token.ThrowIfCancellationRequested();

                        // Clean up the failed call.
                        currentCall.Dispose();
                    }
                    else
                    {
                        // Handle the situation where the call failed with a non-deadline status, but retry
                        // didn't happen because of deadline exceeded.
                        IGrpcCall<TRequest, TResponse> resolvedCall = (IsDeadlineExceeded() && !(currentCall.CallTask.IsCompletedSuccessfully && currentCall.CallTask.Result.StatusCode == StatusCode.DeadlineExceeded))
                            ? CreateStatusCall(GrpcProtocolConstants.DeadlineExceededStatus)
                            : currentCall;

                        // Can't retry.
                        // Signal public API exceptions that they should finish throwing and then exit the retry loop.
                        CommitCall(resolvedCall, result.GetValueOrDefault());
                        return;
                    }
                }
                catch (Exception ex)
                {
                    HandleUnexpectedError(ex);
                    return;
                }
            }
        }

        protected override void OnCommitCall(IGrpcCall<TRequest, TResponse> call)
        {
            _activeCall = null;
        }

        protected override void Dispose(bool disposing)
        {
            lock (Lock)
            {
                base.Dispose(disposing);

                _activeCall?.Dispose();
            }
        }

        protected override void StartCore(Action<GrpcCall<TRequest, TResponse>> startCallFunc)
        {
            _ = StartRetry(startCallFunc);
        }

        public override Task ClientStreamCompleteAsync()
        {
            ClientStreamComplete = true;

            return DoClientStreamActionAsync(async call =>
            {
                await call.ClientStreamWriter!.CompleteAsync().ConfigureAwait(false);
            });
        }

        public override Task ClientStreamWriteAsync(TRequest message)
        {
            return DoClientStreamActionAsync(async call =>
            {
                Debug.Assert(call.ClientStreamWriter != null);

                if (ClientStreamWriteOptions != null)
                {
                    call.ClientStreamWriter.WriteOptions = ClientStreamWriteOptions;
                }

                await call.WriteClientStreamAsync(WriteNewMessage, message).ConfigureAwait(false);
                BufferedCurrentMessage = false;

                if (ClientStreamComplete)
                {
                    await call.ClientStreamWriter.CompleteAsync().ConfigureAwait(false);
                }
            });
        }

        private async Task DoClientStreamActionAsync(Func<IGrpcCall<TRequest, TResponse>, Task> action)
        {
            var call = await GetActiveCallAsync(previousCall: null).ConfigureAwait(false);
            while (true)
            {
                try
                {
                    await action(call!).ConfigureAwait(false);
                    return;
                }
                catch
                {
                    call = await GetActiveCallAsync(previousCall: call).ConfigureAwait(false);
                    if (call == null)
                    {
                        throw;
                    }
                }
            }
        }

        private Task<IGrpcCall<TRequest, TResponse>?> GetActiveCallAsync(IGrpcCall<TRequest, TResponse>? previousCall)
        {
            Debug.Assert(NewActiveCallTcs != null);

            lock (Lock)
            {
                // Return currently active call if there is one, and its not the previous call.
                if (_activeCall != null && previousCall != _activeCall)
                {
                    return Task.FromResult<IGrpcCall<TRequest, TResponse>?>(_activeCall);
                }

                // Wait to see whether new call will be made
                return GetActiveCallUnsynchronizedAsync(previousCall);
            }
        }
    }
}
