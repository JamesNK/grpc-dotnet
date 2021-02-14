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

namespace Grpc.Net.Client.Internal.Retry
{
    internal sealed partial class HedgingCall<TRequest, TResponse> : RetryCallBase<TRequest, TResponse>
        where TRequest : class
        where TResponse : class
    {
        // Getting logger name from generic type is slow. Cached copy.
        private const string LoggerName = "Grpc.Net.Client.Internal.HedgingCall";

        private readonly HedgingPolicy _hedgingPolicy;
        private TaskCompletionSource<GrpcCall<TRequest, TResponse>>? _newActiveCallTcs;
        private int _callsAttempted;

        // Internal for testing
        internal Timer? _createCallTimer;
        internal List<IGrpcCall<TRequest, TResponse>> _activeCalls { get; }

        public HedgingCall(HedgingPolicy hedgingPolicy, GrpcChannel channel, Method<TRequest, TResponse> method, CallOptions options)
            : base(channel, method, options, LoggerName)
        {
            _hedgingPolicy = hedgingPolicy;
            _activeCalls = new List<IGrpcCall<TRequest, TResponse>>();

            // Active call TCS is only used if there is a client stream, and a hedging delay.
            // It is awaited when attempting to write to the client stream and there are no active calls.
            if (_hedgingPolicy.HedgingDelay > TimeSpan.Zero && HasClientStream())
            {
                _newActiveCallTcs = new TaskCompletionSource<GrpcCall<TRequest, TResponse>>(TaskCreationOptions.None);
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

                if (_newActiveCallTcs != null)
                {
                    // Run continuation synchronously so awaiters execute inside the lock
                    _newActiveCallTcs.SetResult(call);
                    _newActiveCallTcs = new TaskCompletionSource<GrpcCall<TRequest, TResponse>>(TaskCreationOptions.None);
                }
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
                    if (retryPushbackMS >= 0)
                    {
                        lock (Lock)
                        {
                            _createCallTimer?.Change(
                                TimeSpan.FromMilliseconds(retryPushbackMS.GetValueOrDefault()),
                                _hedgingPolicy.HedgingDelay.GetValueOrDefault());
                        }
                    }
                    Channel.RetryThrottling?.CallFailure();
                }
                else
                {
                    CommitCall(call, CommitReason.FatelStatusCode);
                }
            }

            lock (Lock)
            {
                if (IsDeadlineExceeded())
                {
                    // Deadline has been exceeded so immediately commit call.
                    CommitCall(call, CommitReason.DeadlineExceeded);
                }
                else if (_activeCalls.Count == 1 && _callsAttempted >= _hedgingPolicy.MaxAttempts.GetValueOrDefault())
                {
                    // This is the last active call and no more will be made.
                    CommitCall(call, CommitReason.FinalCall);
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

        private void CommitCall(GrpcCall<TRequest, TResponse> call, CommitReason commitReason)
        {
            lock (Lock)
            {
                if (!FinalizedCallTask.IsCompletedSuccessfully)
                {
                    var removed = _activeCalls.Remove(call);
                    Debug.Assert(removed);

                    CleanUpUnsynchronized();

                    // Log before committing for unit tests.
                    Log.CallCommited(Logger, commitReason);

                    FinalizedCallTcs.SetResult(call);
                }
            }
        }

        private void CleanUpUnsynchronized()
        {
            // Stop creating calls and cancel all others in progress.
            _createCallTimer?.Dispose();
            _createCallTimer = null;
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
                while (_callsAttempted < _hedgingPolicy.MaxAttempts.GetValueOrDefault())
                {
                    _ = StartCall(startCallFunc);
                }
            }
            else
            {
                Log.StartingHedgingCallTimer(Logger, hedgingDelay);

                // Create timer and set to field before setting time.
                // Ensures there is no weird situation where the timer triggers
                // before the field is set. 
                _createCallTimer = new Timer(CreateCallCallback, startCallFunc, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                _createCallTimer.Change(hedgingDelay, hedgingDelay);
                _ = StartCall(startCallFunc);
            }
        }

        private void CreateCallCallback(object? state)
        {
            lock (Lock)
            {
                // Note that a new call could be started after the deadline has been exceeded.
                // This is fine because the call will immediately have a deadline exceeded status
                // and when its status is evaluated the timer will be stopped.
                if (_callsAttempted < _hedgingPolicy.MaxAttempts.GetValueOrDefault())
                {
                    _ = StartCall((Action<GrpcCall<TRequest, TResponse>>)state!);
                    return;
                }

                // Reached maximum allowed attempts.
                // No more calls will be made so there is no point keeping the timer.
                Log.StoppingHedgingCallTimer(Logger);
                _createCallTimer?.Dispose();
                _createCallTimer = null;
            }
        }

        public override void Dispose()
        {
            lock (Lock)
            {
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

        private async Task WaitForCallAsync(Func<IList<IGrpcCall<TRequest, TResponse>>, Task> action)
        {
            Console.WriteLine($"No activie calls. Wait for next call.");

            IGrpcCall<TRequest, TResponse>? call = null;

            // If there is a hedge delay then wait for next call.
            if (_newActiveCallTcs != null)
            {
                call = await _newActiveCallTcs.Task.ConfigureAwait(false);
            }
            // If there isn't a delay, or there are no more active calls then use the finalized call.
            if (call == null)
            {
                call = await FinalizedCallTask.ConfigureAwait(false);
            }
            await action(new[] { call }).ConfigureAwait(false);
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
                    return WaitForCallAsync(action);
                }
            }
        }
    }
}
