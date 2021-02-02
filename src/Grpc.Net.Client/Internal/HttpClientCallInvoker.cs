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
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client.Internal;

namespace Grpc.Net.Client.Internal
{
    /// <summary>
    /// A client-side RPC invocation using HttpClient.
    /// </summary>
    internal sealed class HttpClientCallInvoker : CallInvoker
    {
        internal GrpcChannel Channel { get; }

        public HttpClientCallInvoker(GrpcChannel channel)
        {
            Channel = channel;
        }

        /// <summary>
        /// Invokes a client streaming call asynchronously.
        /// In client streaming scenario, client sends a stream of requests and server responds with a single response.
        /// </summary>
        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options)
        {
            var call = CreateRootGrpcCall<TRequest, TResponse>(Channel, method, options);
            call.StartClientStreaming();

            return new AsyncClientStreamingCall<TRequest, TResponse>(
                requestStream: call.ClientStreamWriter,
                responseAsync: call.GetResponseAsync(),
                responseHeadersAsync: Callbacks<TRequest, TResponse>.GetResponseHeadersAsync,
                getStatusFunc: Callbacks<TRequest, TResponse>.GetStatus,
                getTrailersFunc: Callbacks<TRequest, TResponse>.GetTrailers,
                disposeAction: Callbacks<TRequest, TResponse>.Dispose,
                call);
        }

        /// <summary>
        /// Invokes a duplex streaming call asynchronously.
        /// In duplex streaming scenario, client sends a stream of requests and server responds with a stream of responses.
        /// The response stream is completely independent and both side can be sending messages at the same time.
        /// </summary>
        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options)
        {
            var call = CreateRootGrpcCall<TRequest, TResponse>(Channel, method, options);
            call.StartDuplexStreaming();

            return new AsyncDuplexStreamingCall<TRequest, TResponse>(
                requestStream: call.ClientStreamWriter,
                responseStream: call.ClientStreamReader,
                responseHeadersAsync: Callbacks<TRequest, TResponse>.GetResponseHeadersAsync,
                getStatusFunc: Callbacks<TRequest, TResponse>.GetStatus,
                getTrailersFunc: Callbacks<TRequest, TResponse>.GetTrailers,
                disposeAction: Callbacks<TRequest, TResponse>.Dispose,
                call);
        }

        /// <summary>
        /// Invokes a server streaming call asynchronously.
        /// In server streaming scenario, client sends on request and server responds with a stream of responses.
        /// </summary>
        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options, TRequest request)
        {
            var call = CreateRootGrpcCall<TRequest, TResponse>(Channel, method, options);
            call.StartServerStreaming(request);

            return new AsyncServerStreamingCall<TResponse>(
                responseStream: call.ClientStreamReader,
                responseHeadersAsync: Callbacks<TRequest, TResponse>.GetResponseHeadersAsync,
                getStatusFunc: Callbacks<TRequest, TResponse>.GetStatus,
                getTrailersFunc: Callbacks<TRequest, TResponse>.GetTrailers,
                disposeAction: Callbacks<TRequest, TResponse>.Dispose,
                call);
        }

        /// <summary>
        /// Invokes a simple remote call asynchronously.
        /// </summary>
        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options, TRequest request)
        {
            var call = CreateRootGrpcCall<TRequest, TResponse>(Channel, method, options);
            call.StartUnary(request);

            return new AsyncUnaryCall<TResponse>(
                responseAsync: call.GetResponseAsync(),
                responseHeadersAsync: Callbacks<TRequest, TResponse>.GetResponseHeadersAsync,
                getStatusFunc: Callbacks<TRequest, TResponse>.GetStatus,
                getTrailersFunc: Callbacks<TRequest, TResponse>.GetTrailers,
                disposeAction: Callbacks<TRequest, TResponse>.Dispose,
                call);
        }

        private class Retrier<TRequest, TResponse> : IGrpcCall<TRequest, TResponse>
            where TRequest : class
            where TResponse : class
        {
            private readonly RetryThrottlingPolicy _retryThrottlingPolicy;
            private readonly GrpcChannel _channel;
            private readonly Method<TRequest, TResponse> _method;
            private readonly CallOptions _options;
            private readonly List<ReadOnlyMemory<byte>> _writtenMessages;
            private readonly object _lock = new object();

            private GrpcCall<TRequest, TResponse> _call;
            private TaskCompletionSource<GrpcCall<TRequest, TResponse>?> _callTcs = new TaskCompletionSource<GrpcCall<TRequest, TResponse>?>(TaskCreationOptions.RunContinuationsAsynchronously);

            public IClientStreamWriter<TRequest>? ClientStreamWriter => _call.ClientStreamWriter;
            public IAsyncStreamReader<TResponse>? ClientStreamReader => _call.ClientStreamReader;

            public Retrier(RetryThrottlingPolicy retryThrottlingPolicy, GrpcChannel channel, Method<TRequest, TResponse> method, CallOptions options)
            {
                _retryThrottlingPolicy = retryThrottlingPolicy;
                _channel = channel;
                _method = method;
                _options = options;
                _writtenMessages = new List<ReadOnlyMemory<byte>>();

                _call = CreateGrpcCall<TRequest, TResponse>(_channel, _method, _options);
                _call.StreamWrapper = WrapStream;
            }

            private Stream WrapStream(Stream arg) => new RetryCaptureStream(arg, _writtenMessages);

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
            }

            private GrpcCall<TRequest, TResponse> RetryCall()
            {
                var call = CreateGrpcCall<TRequest, TResponse>(_channel, _method, _options);
                call.StartRetry(_writtenMessages);

                return call;
            }

            public async Task<TResponse> GetResponseAsync()
            {
                while (true)
                {
                    try
                    {
                        return await _call.GetResponseAsync().ConfigureAwait(false);
                    }
                    catch (RpcException ex)
                    {
                        if (CanRetry(ex))
                        {
                            await Task.Delay(_retryThrottlingPolicy.InitialBackoff.GetValueOrDefault()).ConfigureAwait(false);

                            lock (_lock)
                            {
                                _call = RetryCall();
                                _callTcs.TrySetResult(_call);
                                _callTcs = new TaskCompletionSource<GrpcCall<TRequest, TResponse>?>(TaskCreationOptions.RunContinuationsAsynchronously);
                            }
                        }
                        else
                        {
                            _callTcs.TrySetResult(null);
                        }
                    }
                }
            }

            private bool CanRetry(RpcException ex)
            {
                // TODO(JamesNK): Also check count

                if (_call.HttpResponse != null &&
                    GrpcProtocolHelpers.GetHeaderValue(_call.HttpResponse.Headers, GrpcProtocolConstants.StatusTrailer) != null)
                {
                    // If a HttpResponse has been received and it's not a "trailers only" response (contains status in header)
                    // then headers were returned before failure. The call can't be retried.
                    return false;
                }

                return _retryThrottlingPolicy.RetryableStatusCodes.Contains(ex.StatusCode);
            }

            public async Task<Metadata> GetResponseHeadersAsync()
            {
                while (true)
                {
                    try
                    {
                        return await _call.GetResponseHeadersAsync().ConfigureAwait(false);
                    }
                    catch (RpcException ex) when (CanRetry(ex))
                    {
                        TaskCompletionSource<GrpcCall<TRequest, TResponse>?>? tcs;
                        lock (_callTcs)
                        {
                            tcs = _callTcs;
                        }

                        var newCall = await tcs.Task.ConfigureAwait(false);
                        if (newCall == null)
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
                _call.Dispose();
            }

            public void StartUnary(TRequest request)
            {
                _call.StartUnary(request);
            }

            public void StartClientStreaming()
            {
                _call.StartClientStreaming();
            }

            public void StartServerStreaming(TRequest request)
            {
                _call.StartServerStreaming(request);
            }

            public void StartDuplexStreaming()
            {
                _call.StartDuplexStreaming();
            }
        }

        /// <summary>
        /// Invokes a simple remote call in a blocking fashion.
        /// </summary>
        public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options, TRequest request)
        {
            var call = AsyncUnaryCall(method, host, options, request);
            return call.ResponseAsync.GetAwaiter().GetResult();
        }

        private static IGrpcCall<TRequest, TResponse> CreateRootGrpcCall<TRequest, TResponse>(
            GrpcChannel channel,
            Method<TRequest, TResponse> method,
            CallOptions options)
            where TRequest : class
            where TResponse : class
        {
            var methodInfo = channel.GetCachedGrpcMethodInfo(method);
            var retryPolicy = methodInfo.MethodConfig?.RetryPolicy;
            if (retryPolicy == null)
            {
                // No retry/hedge policy configured. Fast path!
                return CreateGrpcCall<TRequest, TResponse>(channel, method, options);
            }
            else
            {
                return new Retrier<TRequest, TResponse>(retryPolicy, channel, method, options);
            }
        }

        private static GrpcCall<TRequest, TResponse> CreateGrpcCall<TRequest, TResponse>(
            GrpcChannel channel,
            Method<TRequest, TResponse> method,
            CallOptions options)
            where TRequest : class
            where TResponse : class
        {
            if (channel.Disposed)
            {
                throw new ObjectDisposedException(nameof(GrpcChannel));
            }

            var methodInfo = channel.GetCachedGrpcMethodInfo(method);
            var call = new GrpcCall<TRequest, TResponse>(method, methodInfo, options, channel);

            return call;
        }

        // Store static callbacks so delegates are allocated once
        private static class Callbacks<TRequest, TResponse>
            where TRequest : class
            where TResponse : class
        {
            internal static readonly Func<object, Task<Metadata>> GetResponseHeadersAsync = state => ((IGrpcCall<TRequest, TResponse>)state).GetResponseHeadersAsync();
            internal static readonly Func<object, Status> GetStatus = state => ((IGrpcCall<TRequest, TResponse>)state).GetStatus();
            internal static readonly Func<object, Metadata> GetTrailers = state => ((IGrpcCall<TRequest, TResponse>)state).GetTrailers();
            internal static readonly Action<object> Dispose = state => ((IGrpcCall<TRequest, TResponse>)state).Dispose();
        }
    }
}
