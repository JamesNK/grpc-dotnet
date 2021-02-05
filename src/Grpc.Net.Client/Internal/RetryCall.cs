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
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Internal
{
    internal partial class RetryCall<TRequest, TResponse> : IGrpcCall<TRequest, TResponse>
        where TRequest : class
        where TResponse : class
    {
        // Getting logger name from generic type is slow. Cached copy.
        private const string LoggerName = "Grpc.Net.Client.Internal.RetryCall";

        private readonly ILogger _logger;
        private readonly RetryThrottlingPolicy _retryThrottlingPolicy;
        private readonly GrpcChannel _channel;
        private readonly Method<TRequest, TResponse> _method;
        private readonly CallOptions _options;
        private readonly List<ReadOnlyMemory<byte>> _writtenMessages;
        private readonly Random _random;
        private readonly object _lock = new object();
        private readonly CancellationTokenSource _retryCts = new CancellationTokenSource();

        private int _attemptCount;
        private int _nextRetryDelayMilliseconds;
        private GrpcCall<TRequest, TResponse> _call;
        private RetryClientStreamReader? _retryClientStreamReader;
        private RetryClientStreamWriter? _retryClientStreamWriter;
        private TaskCompletionSource<bool> _canRetryTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        public IClientStreamWriter<TRequest>? ClientStreamWriter
        {
            get
            {
                if (_retryClientStreamWriter == null)
                {
                    _retryClientStreamWriter = new RetryClientStreamWriter(this);
                }

                return _retryClientStreamWriter;
            }
        }

        public IAsyncStreamReader<TResponse>? ClientStreamReader
        {
            get
            {
                if (_retryClientStreamReader == null)
                {
                    _retryClientStreamReader = new RetryClientStreamReader(this);
                }

                return _retryClientStreamReader;
            }
        }

        private class RetryClientStreamWriter : IClientStreamWriter<TRequest>
        {
            private readonly RetryCall<TRequest, TResponse> _retryCall;

            public RetryClientStreamWriter(RetryCall<TRequest, TResponse> retryCall)
            {
                _retryCall = retryCall;
                WriteOptions = retryCall._call.Options.WriteOptions;
            }

            public WriteOptions WriteOptions { get; set; }

            public async Task CompleteAsync()
            {
                while (true)
                {
                    GrpcCall<TRequest, TResponse> call = _retryCall._call;
                    try
                    {
                        await call.ClientStreamWriter!.CompleteAsync().ConfigureAwait(false);
                        return;
                    }
                    catch
                    {
                        if (!await _retryCall.ResolveRetryTask(call).ConfigureAwait(false))
                        {
                            throw;
                        }
                    }
                }
            }

            public async Task WriteAsync(TRequest message)
            {
                while (true)
                {
                    GrpcCall<TRequest, TResponse> call = _retryCall._call;
                    try
                    {
                        var writer = call.ClientStreamWriter!;
                        writer.WriteOptions = WriteOptions;

                        await writer.WriteAsync(message).ConfigureAwait(false);
                        return;
                    }
                    catch
                    {
                        if (!await _retryCall.ResolveRetryTask(call).ConfigureAwait(false))
                        {
                            throw;
                        }
                    }
                }
            }
        }

        private class RetryClientStreamReader : IAsyncStreamReader<TResponse>
        {
            private readonly RetryCall<TRequest, TResponse> _retryCall;

            public RetryClientStreamReader(RetryCall<TRequest, TResponse> retryCall)
            {
                _retryCall = retryCall;
            }

            // Suppress warning when overriding interface definition
#pragma warning disable CS8613, CS8766 // Nullability of reference types in return type doesn't match implicitly implemented member.
            public TResponse? Current => InnerReader.Current;
#pragma warning restore CS8613, CS8766 // Nullability of reference types in return type doesn't match implicitly implemented member.

            private HttpContentClientStreamReader<TRequest, TResponse> InnerReader => _retryCall._call.ClientStreamReader!;

            public async Task<bool> MoveNext(CancellationToken cancellationToken)
            {
                while (true)
                {
                    GrpcCall<TRequest, TResponse> call = _retryCall._call;
                    try
                    {
                        return await call.ClientStreamReader!.MoveNext(cancellationToken).ConfigureAwait(false);
                    }
                    catch
                    {
                        if (!await _retryCall.ResolveRetryTask(call).ConfigureAwait(false))
                        {
                            throw;
                        }
                    }
                }
            }
        }

        public RetryCall(RetryThrottlingPolicy retryThrottlingPolicy, GrpcChannel channel, Method<TRequest, TResponse> method, CallOptions options)
        {
            _logger = channel.LoggerFactory.CreateLogger(LoggerName);
            _retryThrottlingPolicy = retryThrottlingPolicy;
            _channel = channel;
            _method = method;
            _options = options;
            _writtenMessages = new List<ReadOnlyMemory<byte>>();
            _random = new Random();
            _attemptCount = 1;
            _call = HttpClientCallInvoker.CreateGrpcCall<TRequest, TResponse>(_channel, _method, _options);
            _call.StreamWrapper = output => new RetryCaptureStream(output, _writtenMessages);

            ValidatePolicy(retryThrottlingPolicy);

            _nextRetryDelayMilliseconds = Convert.ToInt32(retryThrottlingPolicy.InitialBackoff.GetValueOrDefault().TotalMilliseconds);
        }

        private void ValidatePolicy(RetryThrottlingPolicy retryThrottlingPolicy)
        {
            if (retryThrottlingPolicy.MaxAttempts == null)
            {
                throw CreateException(_method, RetryThrottlingPolicy.MaxAttemptsPropertyName);
            }
            if (retryThrottlingPolicy.InitialBackoff == null)
            {
                throw CreateException(_method, RetryThrottlingPolicy.InitialBackoffPropertyName);
            }
            if (retryThrottlingPolicy.MaxBackoff == null)
            {
                throw CreateException(_method, RetryThrottlingPolicy.MaxBackoffPropertyName);
            }
            if (retryThrottlingPolicy.BackoffMultiplier == null)
            {
                throw CreateException(_method, RetryThrottlingPolicy.BackoffMultiplierPropertyName);
            }
            if (retryThrottlingPolicy.RetryableStatusCodes.Count == 0)
            {
                throw new InvalidOperationException($"Retry policy for '{_method.FullName}' must have property '{RetryThrottlingPolicy.RetryableStatusCodesPropertyName}' and must be non-empty.");
            }

            static InvalidOperationException CreateException(IMethod method, string propertyName)
            {
                return new InvalidOperationException($"Retry policy for '{method.FullName}' is missing required property '{propertyName}'.");
            }
        }

        private GrpcCall<TRequest, TResponse> CreateRetryCall(bool clientStreamCompleted)
        {
            var call = HttpClientCallInvoker.CreateGrpcCall<TRequest, TResponse>(_channel, _method, _options);
            call.StartRetry(_writtenMessages, clientStreamCompleted);

            return call;
        }

        public async Task<TResponse> GetResponseAsync()
        {
            while (true)
            {
                GrpcCall<TRequest, TResponse> call = _call;
                try
                {
                    return await call.GetResponseAsync().ConfigureAwait(false);
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

        private async Task<bool> ResolveRetryTask(GrpcCall<TRequest, TResponse> call)
        {
            Task<bool> canRetryTask;
            lock (_lock)
            {
                // New call has already been made
                if (call != _call)
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
                Debug.Assert(call != _call);
                return true;
            }

            return false;
        }

        private int CalculateNextRetryDelay()
        {
            var nextMilliseconds = _nextRetryDelayMilliseconds * _retryThrottlingPolicy.BackoffMultiplier.GetValueOrDefault();
            nextMilliseconds = Math.Min(nextMilliseconds, _retryThrottlingPolicy.MaxBackoff.GetValueOrDefault().TotalMilliseconds);

            return Convert.ToInt32(nextMilliseconds);
        }

        private RetryResult EvaluateRetry(Status status, int? retryPushbackMilliseconds)
        {
            if (_attemptCount >= _retryThrottlingPolicy.MaxAttempts.GetValueOrDefault())
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

            if (!HasResponseHeaderStatus(_call))
            {
                // If a HttpResponse has been received and it's not a "trailers only" response (contains status in header)
                // then headers were returned before failure. The call is commited and can't be retried.
                return RetryResult.CallCommited;
            }

            if (!_retryThrottlingPolicy.RetryableStatusCodes.Contains(status.StatusCode))
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
            PushbackStop
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
                var status = await _call.CallTask.ConfigureAwait(false);
                if (status.StatusCode == StatusCode.OK)
                {
                    // Success. Exit retry loop.
                    return;
                }

                try
                {
                    var retryPushbackMS = GetRetryPushback();

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
                            _call.Dispose();

                            // Start new call.
                            _attemptCount++;
                            _call = CreateRetryCall(_call.ClientStreamWriter?.CompleteTcs.Task.IsCompletedSuccessfully ?? false);

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
                    return;
                }
            }
        }

        private int? GetRetryPushback()
        {
            // https://github.com/grpc/proposal/blob/master/A6-client-retries.md#pushback
            if (_call.HttpResponse != null)
            {
                if (_call.HttpResponse.Headers.TryGetValues(GrpcProtocolConstants.RetryPushbackHeader, out var values))
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

        public async Task<Metadata> GetResponseHeadersAsync()
        {
            while (true)
            {
                GrpcCall<TRequest, TResponse> call = _call;
                try
                {
                    var headers = await call.GetResponseHeadersAsync().ConfigureAwait(false);

                    // GetResponseHeadersAsync won't throw if there is a trailers only error (i.e. grpc-status returned with headers).
                    // Check whether a status was returned with the response headers and retry if it was.
                    if (HasResponseHeaderStatus(call))
                    {
                        if (!await ResolveRetryTask(call).ConfigureAwait(false))
                        {
                            return headers;
                        }
                    }
                    else
                    {
                        return headers;
                    }
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

        public Status GetStatus()
        {
            return _call.GetStatus();
        }

        public Metadata GetTrailers()
        {
            return _call.GetTrailers();
        }

        public void Dispose()
        {
            lock (_lock)
            {
                _retryCts.Cancel();
                _call.Dispose();
            }
        }

        public void StartUnary(TRequest request)
        {
            _call.StartUnary(request);
            _ = StartRetry();
        }

        public void StartClientStreaming()
        {
            _call.StartClientStreaming();
            _ = StartRetry();
        }

        public void StartServerStreaming(TRequest request)
        {
            _call.StartServerStreaming(request);
            _ = StartRetry();
        }

        public void StartDuplexStreaming()
        {
            _call.StartDuplexStreaming();
            _ = StartRetry();
        }

        private class RetryCaptureStream : Stream
        {
            private readonly Stream _inner;
            private readonly List<ReadOnlyMemory<byte>> _writtenMessages;

            public RetryCaptureStream(Stream inner, List<ReadOnlyMemory<byte>> writtenMessages)
            {
                _inner = inner;
                _writtenMessages = writtenMessages;
            }

            public override bool CanRead => _inner.CanRead;
            public override bool CanSeek => _inner.CanSeek;
            public override bool CanWrite => _inner.CanWrite;
            public override long Length => _inner.Length;
            public override long Position
            {
                get => _inner.Position;
                set => _inner.Position = value;
            }

            public override void Flush() => _inner.Flush();
            public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
            public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
            public override void SetLength(long value) => _inner.SetLength(value);
            public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);

            public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);
            public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken) => _inner.CopyToAsync(destination, bufferSize, cancellationToken);
            public override void Write(ReadOnlySpan<byte> buffer) => throw new NotSupportedException();
            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                _writtenMessages.Add(buffer);
                return _inner.WriteAsync(buffer, offset, count, cancellationToken);
            }
            public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            {
                _writtenMessages.Add(buffer);
                return _inner.WriteAsync(buffer, cancellationToken);
            }
            protected override void Dispose(bool disposing)
            {
                base.Dispose(disposing);
                if (disposing)
                {
                    _inner.Dispose();
                }
            }
            public override async ValueTask DisposeAsync()
            {
                await base.DisposeAsync().ConfigureAwait(false);
                await _inner.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}
