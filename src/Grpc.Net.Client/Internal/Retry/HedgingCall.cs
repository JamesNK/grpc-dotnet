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
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client.Configuration;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Internal.Retry
{
    internal sealed partial class HedgingCall<TRequest, TResponse> : RetryCallBase<TRequest, TResponse>
        where TRequest : class
        where TResponse : class
    {
        // Getting logger name from generic type is slow. Cached copy.
        private const string LoggerName = "Grpc.Net.Client.Internal.HedgingCall";

        private readonly HedgingPolicy _hedgingPolicy;

        private int _callsAttempted;

        private TaskCompletionSource<object?>? _pushbackReceivedTcs;
        private TimeSpan? _pushbackDelay;

        // Internal for testing
        internal List<IGrpcCall<TRequest, TResponse>> _activeCalls { get; }
        internal Task? CreateHedgingCallsTask { get; set; }

        public HedgingCall(HedgingPolicy hedgingPolicy, GrpcChannel channel, Method<TRequest, TResponse> method, CallOptions options)
            : base(channel, method, options, LoggerName, hedgingPolicy.MaxAttempts.GetValueOrDefault())
        {
            _hedgingPolicy = hedgingPolicy;
            _activeCalls = new List<IGrpcCall<TRequest, TResponse>>();

            if (_hedgingPolicy.HedgingDelay > TimeSpan.Zero)
            {
                _pushbackReceivedTcs = new TaskCompletionSource<object?>(TaskCreationOptions.None);
            }

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
            lock (Lock)
            {
                call = HttpClientCallInvoker.CreateGrpcCall<TRequest, TResponse>(Channel, Method, Options, _callsAttempted);
                _activeCalls.Add(call);
                _callsAttempted++;

                startCallFunc(call);

                SetNewActiveCall(call);
            }

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
                CommitCall(call, CommitReason.ResponseHeadersReceived);

                // Wait until the call has finished and then check its status code
                // to update retry throttling tokens.
                var status = await call.CallTask.ConfigureAwait(false);
                if (status.StatusCode == StatusCode.OK)
                {
                    // Success. Exit retry loop.
                    Channel.RetryThrottling?.CallSuccess();
                }
            }
            else
            {
                var status = responseStatus.Value;

                var retryPushbackMS = GetRetryPushback(call);

                if (retryPushbackMS < 0)
                {
                    Channel.RetryThrottling?.CallFailure();
                }
                else if (_hedgingPolicy.NonFatalStatusCodes.Contains(status.StatusCode))
                {
                    // Pushback doesn't do anything if we started with no delay and all calls
                    // have already been made when hedging starting.
                    if (retryPushbackMS >= 0 && _pushbackReceivedTcs != null)
                    {
                        lock (Lock)
                        {
                            _pushbackDelay = TimeSpan.FromMilliseconds(retryPushbackMS.GetValueOrDefault());
                            _pushbackReceivedTcs.TrySetResult(null);
                        }
                    }
                    Channel.RetryThrottling?.CallFailure();
                }
                else
                {
                    CommitCall(call, CommitReason.FatalStatusCode);
                }
            }

            lock (Lock)
            {
                if (IsDeadlineExceeded())
                {
                    // Deadline has been exceeded so immediately commit call.
                    CommitCall(call, CommitReason.DeadlineExceeded);
                }
                else if (_activeCalls.Count == 1 && _callsAttempted >= MaxRetryAttempts)
                {
                    // This is the last active call and no more will be made.
                    CommitCall(call, CommitReason.ExceededAttemptCount);
                }
                else if (_activeCalls.Count == 1 && (Channel.RetryThrottling?.IsRetryThrottlingActive() ?? false))
                {
                    // This is the last active call and throttling is active.
                    CommitCall(call, CommitReason.Throttled);
                }
                else
                {
                    // Call isn't used and can be cancelled.
                    // Note that the call could have already been removed and disposed if the
                    // hedging call has been finalized or disposed.
                    if (_activeCalls.Remove(call))
                    {
                        call.Dispose();
                    }
                }
            }
        }

        protected override void OnCommitCall(IGrpcCall<TRequest, TResponse> call)
        {
            _activeCalls.Remove(call);

            CleanUpUnsynchronized();
        }

        private void CleanUpUnsynchronized()
        {
            CancellationTokenSource.Cancel();

            while (_activeCalls.Count > 0)
            {
                _activeCalls[_activeCalls.Count - 1].Dispose();
                _activeCalls.RemoveAt(_activeCalls.Count - 1);
            }
        }

        protected override void StartCore(Action<GrpcCall<TRequest, TResponse>> startCallFunc)
        {
            var hedgingDelay = _hedgingPolicy.HedgingDelay.GetValueOrDefault();
            if (hedgingDelay == TimeSpan.Zero)
            {
                // If there is no delay then start all call immediately
                while (_callsAttempted < MaxRetryAttempts)
                {
                    _ = StartCall(startCallFunc);

                    // Don't send additional calls if retry throttling is active.
                    if (Channel.RetryThrottling?.IsRetryThrottlingActive() ?? false)
                    {
                        // TODO(JamesNK) - Log
                        break;
                    }
                }
            }
            else
            {
                CreateHedgingCallsTask = CreateHedgingCalls(startCallFunc);
            }
        }

        private async Task CreateHedgingCalls(Action<GrpcCall<TRequest, TResponse>> startCallFunc)
        {
            var hedgingDelay = _hedgingPolicy.HedgingDelay.GetValueOrDefault();

            Log.StartingHedgingCallTimer(Logger, hedgingDelay);

            try
            {
                while (_callsAttempted < MaxRetryAttempts)
                {
                    _ = StartCall(startCallFunc);

                    await HedgingDelayAsync(hedgingDelay).ConfigureAwait(false);

                    if (IsDeadlineExceeded())
                    {
                        CommitCall(new StatusGrpcCall<TRequest, TResponse>(new Status(StatusCode.DeadlineExceeded, string.Empty)), CommitReason.DeadlineExceeded);
                        break;
                    }
                    else
                    {
                        lock (Lock)
                        {
                            if (Channel.RetryThrottling?.IsRetryThrottlingActive() ?? false && _activeCalls.Count == 0)
                            {
                                CommitCall(ThrottledCall, CommitReason.Throttled);
                                break;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Cancellation token triggered by dispose could throw here.
                if (ex is OperationCanceledException && CancellationTokenSource.IsCancellationRequested)
                {
                    // Cancellation could have been caused by an exceeded deadline.
                    if (IsDeadlineExceeded())
                    {
                        // An exceeded deadline inbetween calls means there is no active call.
                        // Create a fake call that returns exceeded deadline status to the app.
                        CommitCall(DeadlineCall, CommitReason.DeadlineExceeded);
                    }
                }
                else
                {
                    // Only log unexpected errors.
                    Log.ErrorRetryingCall(Logger, ex);
                }
            }
            finally
            {
                Log.StoppingHedgingCallTimer(Logger);
            }
        }

        private async Task HedgingDelayAsync(TimeSpan hedgingDelay)
        {
            while (true)
            {
                var tcs = _pushbackReceivedTcs;
                if (tcs != null)
                {
                    var completedTask = await Task.WhenAny(Task.Delay(hedgingDelay, CancellationTokenSource.Token), tcs.Task).ConfigureAwait(false);
                    if (completedTask != tcs.Task)
                    {
                        // Task.Delay won. Check CTS to see if it won because of cancellation.
                        CancellationTokenSource.Token.ThrowIfCancellationRequested();
                        return;
                    }

                    lock (Lock)
                    {
                        Debug.Assert(_pushbackDelay != null);

                        // Use pushback value and delay again
                        hedgingDelay = _pushbackDelay.GetValueOrDefault();

                        _pushbackDelay = null;
                        _pushbackReceivedTcs = new TaskCompletionSource<object?>(TaskCreationOptions.None);
                    }
                }
                else
                {
                    await Task.Delay(hedgingDelay).ConfigureAwait(false);
                    return;
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            lock (Lock)
            {
                base.Dispose(disposing);

                CleanUpUnsynchronized();
            }
        }

        public override Task ClientStreamCompleteAsync()
        {
            ClientStreamComplete = true;

            return DoClientStreamActionAsync(calls =>
            {
                var completeTasks = new Task[calls.Count];
                for (var i = 0; i < calls.Count; i++)
                {
                    completeTasks[i] = calls[i].ClientStreamWriter!.CompleteAsync();
                }

                //Console.WriteLine($"Completing");
                return Task.WhenAll(completeTasks);
            });
        }

        public override async Task ClientStreamWriteAsync(TRequest message)
        {
            // TODO(JamesNK) - Not safe for multi-threading
            await DoClientStreamActionAsync(calls =>
            {
                var writeTasks = new Task[calls.Count];
                for (var i = 0; i < calls.Count; i++)
                {
                    writeTasks[i] = calls[i].WriteClientStreamAsync(WriteNewMessage, message);
                }

                //Console.WriteLine($"Writing client stream message to {writeTasks.Length} calls.");
                return Task.WhenAll(writeTasks);
            }).ConfigureAwait(false);
            BufferedCurrentMessage = false;
        }

        private Task DoClientStreamActionAsync(Func<IList<IGrpcCall<TRequest, TResponse>>, Task> action)
        {
            lock (Lock)
            {
                if (_activeCalls.Count > 0)
                {
                    return action(_activeCalls);
                }
                else
                {
                    return WaitForCallUnsynchronizedAsync(action);
                }
            }

            async Task WaitForCallUnsynchronizedAsync(Func<IList<IGrpcCall<TRequest, TResponse>>, Task> action)
            {
                var call = await GetActiveCallUnsynchronizedAsync(previousCall: null).ConfigureAwait(false);
                await action(new[] { call! }).ConfigureAwait(false);
            }
        }
    }
}
