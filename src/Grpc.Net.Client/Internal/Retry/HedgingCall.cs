﻿#region Copyright notice and license

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
        private TaskCompletionSource<GrpcCall<TRequest, TResponse>>? _newActiveCall;
        private int _callsAttempted;

        // Internal for testing
        internal Timer? _createCallTimer;
        internal List<GrpcCall<TRequest, TResponse>> _activeCalls { get; }

        public HedgingCall(HedgingPolicy hedgingPolicy, GrpcChannel channel, Method<TRequest, TResponse> method, CallOptions options)
            : base(channel, method, options, LoggerName)
        {
            _hedgingPolicy = hedgingPolicy;
            //_attemptCount = 1;
            _activeCalls = new List<GrpcCall<TRequest, TResponse>>();
            if (HasDelayAndClientStream())
            {
                _newActiveCall = new TaskCompletionSource<GrpcCall<TRequest, TResponse>>(TaskCreationOptions.None);
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
                Console.WriteLine("Starting call.");
                call = HttpClientCallInvoker.CreateGrpcCall<TRequest, TResponse>(Channel, Method, Options, _callsAttempted);
                _activeCalls.Add(call);
                _callsAttempted++;

                startCallFunc(call);

                if (_newActiveCall != null)
                {
                    // Run continuation synchronously so awaiters execute inside the lock
                    _newActiveCall.SetResult(call);
                    _newActiveCall = new TaskCompletionSource<GrpcCall<TRequest, TResponse>>(TaskCreationOptions.None);
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
                FinalizeCall(call);

                responseStatus = await call.CallTask.ConfigureAwait(false);
                if (responseStatus.GetValueOrDefault().StatusCode == StatusCode.OK)
                {
                    // Success. Exit retry loop.
                    Channel.RetryThrottling?.CallSuccess();
                }
            }
            else
            {
                Status status = responseStatus.Value;

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
                    FinalizeCall(call);
                }
            }

            lock (Lock)
            {
                // This is the last active call and no more will be made.
                if (_activeCalls.Count == 1 && _callsAttempted >= _hedgingPolicy.MaxAttempts.GetValueOrDefault())
                {
                    FinalizeCall(call);
                }
                else
                {
                    var removed = _activeCalls.Remove(call);
                    Debug.Assert(removed);

                    call.Dispose();
                }
            }
        }

        private bool HasDelayAndClientStream()
        {
            return _hedgingPolicy.HedgingDelay > TimeSpan.Zero &&
                (Method.Type == MethodType.ClientStreaming || Method.Type == MethodType.DuplexStreaming);
        }

        private void FinalizeCall(GrpcCall<TRequest, TResponse> call)
        {
            lock (Lock)
            {
                var removed = _activeCalls.Remove(call);
                Debug.Assert(removed);

                CleanUpUnsynchronized();
            }

            FinalizedCallTcs.SetResult(call);
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
                if (_callsAttempted < _hedgingPolicy.MaxAttempts.GetValueOrDefault())
                {
                    _ = StartCall((Action<GrpcCall<TRequest, TResponse>>)state!);
                }
                else
                {
                    // Reached maximum allowed attempts. No point keeping timer.
                    _createCallTimer?.Dispose();
                    _createCallTimer = null;
                }
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
            lock (Lock)
            {
                ClientStreamComplete = true;

                var completeTasks = new Task[_activeCalls.Count];
                for (var i = 0; i < _activeCalls.Count; i++)
                {
                    completeTasks[i] = _activeCalls[i].ClientStreamWriter!.CompleteAsync();
                }

                Console.WriteLine($"Completing");
                return Task.WhenAll(completeTasks);
            }
        }

        public override async Task ClientStreamWriteAsync(TRequest message)
        {
            // TODO(JamesNK) - Not safe for multi-threading
            Task writeTask;
            lock (Lock)
            {
                if (_activeCalls.Count > 0)
                {
                    var writeTasks = new Task[_activeCalls.Count];
                    for (var i = 0; i < _activeCalls.Count; i++)
                    {
                        var writer = _activeCalls[i].ClientStreamWriter;
                        Debug.Assert(writer != null);

                        writeTasks[i] = writer.WriteAsync(WriteNewMessage, message);
                    }
                    Console.WriteLine($"Writing client stream message to {writeTasks.Length} calls.");
                    writeTask = Task.WhenAll(writeTasks);
                }
                else
                {
                    writeTask = WaitForCall(message);
                }
            }

            await Task.WhenAll(writeTask).ConfigureAwait(false);
            BufferedCurrentMessage = false;
        }

        private async Task WaitForCall(TRequest message)
        {
            Console.WriteLine($"No activie calls. Wait for next call.");

            // Should resume synchronously inside lock
            await _newActiveCall!.Task.ConfigureAwait(false);
            Console.WriteLine($"New call! Active count: {_activeCalls.Count}");

            await ClientStreamWriteAsync(message).ConfigureAwait(false);
        }

        protected override async ValueTask WriteBufferedMessages(GrpcCall<TRequest, TResponse> call, Stream writeStream, CancellationToken cancellationToken)
        {
            Console.WriteLine($"Writing {WrittenMessages.Count} buffered messages.");

            foreach (var writtenMessage in WrittenMessages)
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
