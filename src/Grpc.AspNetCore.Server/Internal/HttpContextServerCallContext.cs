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
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Grpc.AspNetCore.Server.Features;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.Logging;

namespace Grpc.AspNetCore.Server.Internal
{
    internal sealed partial class HttpContextServerCallContext : ServerCallContext, IAsyncDisposable, IServerCallContextFeature
    {
        private static readonly AuthContext UnauthenticatedContext = new AuthContext(null, new Dictionary<string, List<AuthProperty>>());
        private readonly ILogger _logger;

        // Override the current time for unit testing
        internal ISystemClock Clock = SystemClock.Instance;

        private string? _peer;
        private Metadata? _requestHeaders;
        private Metadata? _responseTrailers;
        private DateTime _deadline;
        private CancellationTokenSource? _deadlineCts;
        private SemaphoreSlim? _deadlineLock;
        private Status _status;
        private AuthContext? _authContext;
        private CancellationTokenRegistration _requestAbortedRegistration;
        // Internal for tests
        internal bool _disposed;

        internal HttpContextServerCallContext(HttpContext httpContext, GrpcServiceOptions serviceOptions, ILogger logger)
        {
            HttpContext = httpContext;
            ServiceOptions = serviceOptions;
            _logger = logger;
        }

        internal HttpContext HttpContext { get; }
        internal GrpcServiceOptions ServiceOptions { get; }
        internal string? ResponseGrpcEncoding { get; private set; }

        internal bool HasResponseTrailers => _responseTrailers != null;

        protected override string? MethodCore => HttpContext.Request.Path.Value;

        protected override string? HostCore => HttpContext.Request.Host.Value;

        protected override string? PeerCore
        {
            get
            {
                if (_peer == null)
                {
                    var connection = HttpContext.Connection;
                    if (connection.RemoteIpAddress != null)
                    {
                        _peer = (connection.RemoteIpAddress.AddressFamily == AddressFamily.InterNetwork ? "ipv4:" : "ipv6:") + connection.RemoteIpAddress + ":" + connection.RemotePort;
                    }
                }

                return _peer;
            }
        }

        protected override DateTime DeadlineCore => _deadline;

        protected override Metadata RequestHeadersCore
        {
            get
            {
                if (_requestHeaders == null)
                {
                    _requestHeaders = new Metadata();

                    foreach (var header in HttpContext.Request.Headers)
                    {
                        // gRPC metadata contains a subset of the request headers
                        // Filter out pseudo headers (start with :) and other known headers
                        if (header.Key.StartsWith(':') || GrpcProtocolConstants.FilteredHeaders.Contains(header.Key))
                        {
                            continue;
                        }
                        else if (header.Key.EndsWith(Metadata.BinaryHeaderSuffix, StringComparison.OrdinalIgnoreCase))
                        {
                            _requestHeaders.Add(header.Key, GrpcProtocolHelpers.ParseBinaryHeader(header.Value));
                        }
                        else
                        {
                            _requestHeaders.Add(header.Key, header.Value);
                        }
                    }
                }

                return _requestHeaders;
            }
        }

        internal void ProcessHandlerError(Exception ex, string method)
        {
            if (ex is RpcException rpcException)
            {
                Log.RpcConnectionError(_logger, rpcException.StatusCode, ex);

                // There are two sources of metadata entries on the server-side:
                // 1. serverCallContext.ResponseTrailers
                // 2. trailers in RpcException thrown by user code in server side handler.
                // As metadata allows duplicate keys, the logical thing to do is
                // to just merge trailers from RpcException into serverCallContext.ResponseTrailers.
                foreach (var entry in rpcException.Trailers)
                {
                    ResponseTrailers.Add(entry);
                }

                _status = rpcException.Status;
            }
            else
            {
                Log.ErrorExecutingServiceMethod(_logger, method, ex);

                var message = ErrorMessageHelper.BuildErrorMessage("Exception was thrown by handler.", ex, ServiceOptions.EnableDetailedErrors);
                _status = new Status(StatusCode.Unknown, message);
            }

            HttpContext.Response.ConsolidateTrailers(this);
        }

        protected override CancellationToken CancellationTokenCore => _deadlineCts?.Token ?? HttpContext.RequestAborted;

        protected override Metadata ResponseTrailersCore
        {
            get
            {
                if (_responseTrailers == null)
                {
                    _responseTrailers = new Metadata();
                }

                return _responseTrailers;
            }
        }

        protected override Status StatusCore
        {
            get => _status;
            set => _status = value;
        }

        internal Task EndCallAsync()
        {
            if (_deadlineLock == null)
            {
                EndCallCore();
                return Task.CompletedTask;
            }

            var lockTask = _deadlineLock.WaitAsync();
            if (lockTask.IsCompletedSuccessfully)
            {
                try
                {
                    EndCallCore();
                }
                finally
                {
                    _deadlineLock.Release();
                }

                return Task.CompletedTask;
            }
            else
            {
                return EndCallAsyncCore(lockTask);
            }

            async Task EndCallAsyncCore(Task lockTask)
            {
                await lockTask;

                try
                {
                    EndCallCore();
                }
                finally
                {
                    _deadlineLock!.Release();
                }
            }
        }

        private void EndCallCore()
        {
            // Don't set trailers if deadline exceeded or request aborted
            if (CancellationToken.IsCancellationRequested)
            {
                return;
            }

            HttpContext.Response.ConsolidateTrailers(this);
        }

        protected override WriteOptions? WriteOptionsCore { get; set; }

        protected override AuthContext AuthContextCore
        {
            get
            {
                if (_authContext == null)
                {
                    var clientCertificate = HttpContext.Connection.ClientCertificate;
                    if (clientCertificate == null)
                    {
                        _authContext = UnauthenticatedContext;
                    }
                    else
                    {
                        _authContext = GrpcProtocolHelpers.CreateAuthContext(clientCertificate);
                    }
                }

                return _authContext;
            }
        }

        public ServerCallContext ServerCallContext => this;

        protected override IDictionary<object, object> UserStateCore => HttpContext.Items;

        internal bool HasBufferedMessage { get; set; }

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions options)
        {
            // TODO(JunTaoLuo, JamesNK): Currently blocked on ContextPropagationToken implementation in Grpc.Core.Api
            // https://github.com/grpc/grpc-dotnet/issues/40
            throw new NotImplementedException("CreatePropagationToken will be implemented in a future version.");
        }

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders)
        {
            // Headers can only be written once. Throw on subsequent call to write response header instead of silent no-op.
            if (HttpContext.Response.HasStarted)
            {
                throw new InvalidOperationException("Response headers can only be sent once per call.");
            }

            if (responseHeaders != null)
            {
                foreach (var entry in responseHeaders)
                {
                    if (entry.IsBinary)
                    {
                        HttpContext.Response.Headers[entry.Key] = Convert.ToBase64String(entry.ValueBytes);
                    }
                    else
                    {
                        HttpContext.Response.Headers[entry.Key] = entry.Value;
                    }
                }
            }

            return HttpContext.Response.Body.FlushAsync();
        }

        public void Initialize()
        {
            var timeout = GetTimeout();

            if (timeout != TimeSpan.Zero)
            {
                _deadline = Clock.UtcNow.Add(timeout);

                // Create lock before setting up deadline event
                _deadlineLock = new SemaphoreSlim(1, 1);

                _deadlineCts = new CancellationTokenSource(timeout);
                _deadlineCts.Token.Register(DeadlineExceeded);

                _requestAbortedRegistration = HttpContext.RequestAborted.Register(RequestAborted);
            }
            else
            {
                _deadline = DateTime.MaxValue;
            }

            var serviceDefaultCompression = ServiceOptions.ResponseCompressionAlgorithm;
            if (serviceDefaultCompression != null &&
                !string.Equals(serviceDefaultCompression, GrpcProtocolConstants.IdentityGrpcEncoding, StringComparison.Ordinal) &&
                IsEncodingInRequestAcceptEncoding(serviceDefaultCompression))
            {
                ResponseGrpcEncoding = serviceDefaultCompression;
            }
            else
            {
                ResponseGrpcEncoding = GrpcProtocolConstants.IdentityGrpcEncoding;
            }

            HttpContext.Response.Headers.Append(GrpcProtocolConstants.MessageEncodingHeader, ResponseGrpcEncoding);
        }

        private void RequestAborted()
        {
            _deadlineCts?.Cancel();
        }

        private TimeSpan GetTimeout()
        {
            if (HttpContext.Request.Headers.TryGetValue(GrpcProtocolConstants.TimeoutHeader, out var values))
            {
                // CancellationTokenSource does not support greater than int.MaxValue milliseconds
                if (GrpcProtocolHelpers.TryDecodeTimeout(values, out var timeout) &&
                    timeout > TimeSpan.Zero &&
                    timeout.TotalMilliseconds <= int.MaxValue)
                {
                    return timeout;
                }

                Log.InvalidTimeoutIgnored(_logger, values);
            }

            return TimeSpan.Zero;
        }

        // Internal for tests
        internal async void DeadlineExceeded()
        {
            // Deadline could be raised after context has been disposed
            if (_disposed)
            {
                return;
            }

            // Request abort uses the same cancellation token
            // Don't run deadline logic if the request has already been aborted
            if (HttpContext.RequestAborted.IsCancellationRequested)
            {
                return;
            }

            Debug.Assert(_deadlineLock != null, "Lock has not been created.");

            await _deadlineLock.WaitAsync();

            try
            {
                if (_disposed)
                {
                    return;
                }

                Log.DeadlineExceeded(_logger, GetTimeout());

                var status = new Status(StatusCode.DeadlineExceeded, "Deadline Exceeded");

                var trailersDestination = GrpcProtocolHelpers.GetTrailersDestination(HttpContext.Response);
                GrpcProtocolHelpers.SetStatus(trailersDestination, status);

                _status = status;

                // Immediately send remaining response content and trailers
                // If feature is null then abort will still end request, but response won't have trailers
                var completionFeature = HttpContext.Features.Get<IHttpResponseCompletionFeature>();
                if (completionFeature != null)
                {
                    await completionFeature.CompleteAsync();
                }

                // TODO(JamesNK): I believe this sends a RST_STREAM with INTERNAL_ERROR. Grpc.Core sends NO_ERROR
                HttpContext.Abort();
            }
            catch (Exception ex)
            {
                Log.DeadlineCancellationError(_logger, ex);
            }
            finally
            {
                _deadlineLock.Release();
            }
        }

        public ValueTask DisposeAsync()
        {
            if (_deadlineLock == null)
            {
                DisposeCore();
                return default;
            }

            var lockTask = _deadlineLock.WaitAsync();
            if (lockTask.IsCompletedSuccessfully)
            {
                try
                {
                    DisposeCore();
                }
                finally
                {
                    _deadlineLock.Release();
                }

                return default;
            }
            else
            {
                return DisposeAsyncCore(lockTask);
            }

            async ValueTask DisposeAsyncCore(Task lockTask)
            {
                await lockTask;

                try
                {
                    DisposeCore();
                }
                finally
                {
                    _deadlineLock!.Release();
                }
            }
        }

        private void DisposeCore()
        {
            _disposed = true;
            _deadlineCts?.Dispose();
            _requestAbortedRegistration.Dispose();
        }

        internal string? GetRequestGrpcEncoding()
        {
            if (HttpContext.Request.Headers.TryGetValue(GrpcProtocolConstants.MessageEncodingHeader, out var values))
            {
                return values;
            }

            return null;
        }

        internal bool IsEncodingInRequestAcceptEncoding(string encoding)
        {
            if (HttpContext.Request.Headers.TryGetValue(GrpcProtocolConstants.MessageAcceptEncodingHeader, out var values))
            {
                var acceptEncoding = values.ToString().AsSpan();

                while (true)
                {
                    var separatorIndex = acceptEncoding.IndexOf(',');
                    if (separatorIndex == -1)
                    {
                        break;
                    }

                    var segment = acceptEncoding.Slice(0, separatorIndex);
                    acceptEncoding = acceptEncoding.Slice(separatorIndex);

                    // Check segment
                    if (segment.SequenceEqual(encoding))
                    {
                        return true;
                    }
                }

                // Check remainder
                if (acceptEncoding.SequenceEqual(encoding))
                {
                    return true;
                }
            }

            return false;
        }

        internal void ValidateAcceptEncodingContainsResponseEncoding()
        {
            Debug.Assert(ResponseGrpcEncoding != null);

            if (!IsEncodingInRequestAcceptEncoding(ResponseGrpcEncoding))
            {
                Log.EncodingNotInAcceptEncoding(_logger, ResponseGrpcEncoding);
            }
        }

        internal bool CanWriteCompressed()
        {
            var canCompress = ((WriteOptions?.Flags ?? default) & WriteFlags.NoCompress) != WriteFlags.NoCompress;

            return canCompress;
        }
    }
}
