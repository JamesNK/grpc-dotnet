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

namespace Grpc.Net.Client.Internal.Retry
{
    internal sealed partial class HedgingCall<TRequest, TResponse> : RetryCallBase<TRequest, TResponse>
        where TRequest : class
        where TResponse : class
    {
        // Getting logger name from generic type is slow. Cached copy.
        private const string LoggerName = "Grpc.Net.Client.Internal.HedgingCall";

        private readonly HedgingPolicy _hedgingPolicy;

        private readonly List<ReadOnlyMemory<byte>> _writtenMessages;

        private readonly object _lock = new object();
        internal Timer? _createCallTimer;
        private TaskCompletionSource<GrpcCall<TRequest, TResponse>>? _newActiveCall;

        private int _callsAttempted;

        public bool BufferedCurrentMessage { get; set; }
        public List<GrpcCall<TRequest, TResponse>> ActiveCalls { get; }

        public async ValueTask WriteNewMessage(GrpcCall<TRequest, TResponse> call, Stream writeStream, CallOptions callOptions, TRequest message)
        {
            // Serialize current message and add to the buffer.
            BufferCurrentMessage(call, callOptions, message);

            await call.WriteMessageAsync(writeStream, _writtenMessages[_writtenMessages.Count - 1], callOptions.CancellationToken).ConfigureAwait(false);
            //await WriteBufferedMessages(call, writeStream, callOptions.CancellationToken).ConfigureAwait(false);
        }

        private void BufferCurrentMessage(GrpcCall<TRequest, TResponse> call, CallOptions callOptions, TRequest message)
        {
            if (!BufferedCurrentMessage)
            {
                lock (_lock)
                {
                    if (!BufferedCurrentMessage)
                    {
                        var payload = SerializePayload(call, callOptions, message);
                        _writtenMessages.Add(payload);
                        BufferedCurrentMessage = true;

                        Console.WriteLine($"Serialized and buffered message. New buffer count {_writtenMessages.Count}");
                    }
                }
            }
        }

        public HedgingCall(HedgingPolicy hedgingPolicy, GrpcChannel channel, Method<TRequest, TResponse> method, CallOptions options)
            : base(channel, method, options, LoggerName)
        {
            _hedgingPolicy = hedgingPolicy;
            _writtenMessages = new List<ReadOnlyMemory<byte>>();
            //_attemptCount = 1;
            ActiveCalls = new List<GrpcCall<TRequest, TResponse>>();
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
            lock (_lock)
            {
                Console.WriteLine("Starting call.");
                call = HttpClientCallInvoker.CreateGrpcCall<TRequest, TResponse>(Channel, Method, Options, _callsAttempted);
                ActiveCalls.Add(call);
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
                        lock (_lock)
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

        private bool HasDelayAndClientStream()
        {
            return _hedgingPolicy.HedgingDelay > TimeSpan.Zero &&
                (Method.Type == MethodType.ClientStreaming || Method.Type == MethodType.DuplexStreaming);
        }

        private void FinalizeCall(GrpcCall<TRequest, TResponse> call)
        {
            lock (_lock)
            {
                var removed = ActiveCalls.Remove(call);
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
            while (ActiveCalls.Count > 0)
            {
                ActiveCalls[ActiveCalls.Count - 1].Dispose();
                ActiveCalls.RemoveAt(ActiveCalls.Count - 1);
            }
        }

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
                _createCallTimer.Change(hedgingDelay, hedgingDelay);
                _ = StartCall(startCallFunc);
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
                    // Reached maximum allowed attempts. No point keeping timer.
                    _createCallTimer?.Dispose();
                    _createCallTimer = null;
                }
            }
        }

        public override void Dispose()
        {
            lock (_lock)
            {
                CleanUpUnsynchronized();
            }
        }

        public override void StartUnary(TRequest request)
        {
            StartCalls(call =>
            {
                call.StartUnaryCore(new PushUnaryContent<TRequest, TResponse>(stream => WriteNewMessage(call, stream, call.Options, request)));
            });
        }

        public override void StartClientStreaming()
        {
            StartCalls(call =>
            {
                var clientStreamWriter = new HttpContentClientStreamWriter<TRequest, TResponse>(call);
                var content = new PushStreamContent<TRequest, TResponse>(clientStreamWriter, async requestStream =>
                {
                    //Log.SendingBufferedMessages(_logger, _writtenMessages.Count);
                    Console.WriteLine("PushStreamStart");
                    await WriteBufferedMessages(call, requestStream, call.CancellationToken).ConfigureAwait(false);

                    if (ClientStreamComplete)
                    {
                        await call.ClientStreamWriter!.CompleteAsync().ConfigureAwait(false);
                    }
                });
                call.StartClientStreamingCore(clientStreamWriter, content);
            });

            //ActiveCall.StartClientStreaming();
            //StartCalls();
        }

        public override void StartServerStreaming(TRequest request)
        {
            StartCalls(call =>
            {
                call.StartServerStreamingCore(new PushUnaryContent<TRequest, TResponse>(stream => WriteNewMessage(call, stream, call.Options, request)));
            });
        }

        public override void StartDuplexStreaming()
        {
            //ActiveCall.StartDuplexStreaming();
            //StartCalls();
        }

        public override Task ClientStreamCompleteAsync()
        {
            lock (_lock)
            {
                ClientStreamComplete = true;

                var completeTasks = new Task[ActiveCalls.Count];
                for (var i = 0; i < ActiveCalls.Count; i++)
                {
                    completeTasks[i] = ActiveCalls[i].ClientStreamWriter!.CompleteAsync();
                }

                Console.WriteLine($"Completing");
                return Task.WhenAll(completeTasks);
            }
        }

        public override async Task ClientStreamWriteAsync(TRequest message)
        {
            // TODO(JamesNK) - Not safe for multi-threading
            Task writeTask;
            lock (_lock)
            {
                if (ActiveCalls.Count > 0)
                {
                    var writeTasks = new Task[ActiveCalls.Count];
                    for (var i = 0; i < ActiveCalls.Count; i++)
                    {
                        var writer = ActiveCalls[i].ClientStreamWriter;
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
            Console.WriteLine($"New call! Active count: {ActiveCalls.Count}");

            await ClientStreamWriteAsync(message).ConfigureAwait(false);
        }

        internal async ValueTask WriteBufferedMessages(GrpcCall<TRequest, TResponse> call, Stream writeStream, CancellationToken cancellationToken)
        {
            Console.WriteLine($"Writing {_writtenMessages.Count} buffered messages.");

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
