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
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace Grpc.NetCore.HttpClient.Internal
{
    internal class GrpcCall<TRequest, TResponse>
    {
        private readonly CancellationTokenSource _callCts;
        private readonly ISystemClock _clock;

        private HttpResponseMessage _httpResponse;
        private Metadata _trailers;

        public bool Disposed { get; private set; }
        public bool ResponseFinished { get; set; }
        public CallOptions Options { get; }
        public Method<TRequest, TResponse> Method { get; }
        public Task<HttpResponseMessage> SendTask { get; private set; }
        public HttpContentClientStreamWriter<TRequest, TResponse> ClientStreamWriter { get; private set; }

        public HttpContextClientStreamReader<TRequest, TResponse> StreamReader { get; private set; }

        public GrpcCall(Method<TRequest, TResponse> method, CallOptions options, ISystemClock clock)
        {
            _callCts = new CancellationTokenSource();
            Method = method;
            Options = options;
            _clock = clock;
        }

        public CancellationToken CancellationToken
        {
            get { return _callCts.Token; }
        }

        public void SendUnary(System.Net.Http.HttpClient client, TRequest request)
        {
            HttpRequestMessage message = CreateHttpRequestMessage();

            message.Content = new PushStreamContent(
                (stream) =>
                {
                    return SerialiationHelpers.WriteMessage<TRequest>(stream, request, Method.RequestMarshaller.Serializer, Options.CancellationToken);
                },
                GrpcProtocolConstants.GrpcContentTypeHeaderValue);

            SendCore(client, message);
        }

        public void SendClientStreaming(System.Net.Http.HttpClient client)
        {
            var message = CreateHttpRequestMessage();
            ClientStreamWriter = CreateWriter(message);

            SendCore(client, message);
        }

        public void SendServerStreaming(System.Net.Http.HttpClient client)
        {
            HttpRequestMessage message = CreateHttpRequestMessage();

            SendCore(client, message);

            StreamReader = new HttpContextClientStreamReader<TRequest, TResponse>(this);
        }

        public void SendDuplexStreaming(System.Net.Http.HttpClient client)
        {
            var message = CreateHttpRequestMessage();
            ClientStreamWriter = CreateWriter(message);

            SendCore(client, message);

            StreamReader = new HttpContextClientStreamReader<TRequest, TResponse>(this);
        }

        public void Dispose()
        {
            if (!Disposed)
            {
                Disposed = true;

                _callCts.Cancel();
                _callCts.Dispose();
                _httpResponse?.Dispose();
                StreamReader?.Dispose();
                ClientStreamWriter?.Dispose();
            }
        }

        private void SendCore(System.Net.Http.HttpClient client, HttpRequestMessage message)
        {
            SendTask = client.SendAsync(message, HttpCompletionOption.ResponseHeadersRead, _callCts.Token);
        }

        private HttpContentClientStreamWriter<TRequest, TResponse> CreateWriter(HttpRequestMessage message)
        {
            TaskCompletionSource<Stream> writeStreamTcs = new TaskCompletionSource<Stream>(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource<bool> completeTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            message.Content = new PushStreamContent(
                (stream) =>
                {
                    writeStreamTcs.SetResult(stream);
                    return completeTcs.Task;
                },
                GrpcProtocolConstants.GrpcContentTypeHeaderValue);

            var writer = new HttpContentClientStreamWriter<TRequest, TResponse>(this, writeStreamTcs.Task, completeTcs);
            return writer;
        }

        private HttpRequestMessage CreateHttpRequestMessage()
        {
            var message = new HttpRequestMessage(HttpMethod.Post, Method.FullName);
            message.Version = new Version(2, 0);

            if (Options.Headers != null && Options.Headers.Count > 0)
            {
                foreach (var entry in Options.Headers)
                {
                    // Deadline is set via CallOptions.Deadline
                    if (entry.Key == GrpcProtocolConstants.TimeoutHeader)
                    {
                        continue;
                    }

                    var value = entry.IsBinary ? Convert.ToBase64String(entry.ValueBytes) : entry.Value;
                    message.Headers.Add(entry.Key, value);
                }
            }

            if (Options.Deadline != null && Options.Deadline != DateTime.MaxValue)
            {
                var deadline = Options.Deadline.Value - _clock.UtcNow;

                // JamesNK(todo) - Replicate C core's logic for formatting grpc-timeout
                message.Headers.Add(GrpcProtocolConstants.TimeoutHeader, Convert.ToInt64(deadline.TotalMilliseconds) + "m");
            }

            return message;
        }

        public void EnsureNotDisposed()
        {
            if (Disposed)
            {
                throw new ObjectDisposedException(nameof(GrpcCall<TRequest, TResponse>));
            }
        }

        public async Task<TResponse> GetResponseAsync()
        {
            _httpResponse = await SendTask.ConfigureAwait(false);
            var responseStream = await _httpResponse.Content.ReadAsStreamAsync().ConfigureAwait(false);

            var message = await responseStream.ReadSingleMessageAsync(Method.ResponseMarshaller.Deserializer, _callCts.Token).ConfigureAwait(false);
            ResponseFinished = true;

            // The task of this method is cached so there is no need to cache the message here
            return message;
        }

        public async Task<Metadata> GetResponseHeadersAsync()
        {
            _httpResponse = await SendTask.ConfigureAwait(false);

            // The task of this method is cached so there is no need to cache the headers here
            return GrpcProtocolHelpers.BuildMetadata(_httpResponse.Headers);
        }

        public Status GetStatus()
        {
            ValidateTrailersAvailable();

            var grpcStatus = SendTask.Result.TrailingHeaders.GetValues(GrpcProtocolConstants.StatusTrailer).FirstOrDefault();
            var grpcMessage = SendTask.Result.TrailingHeaders.GetValues(GrpcProtocolConstants.MessageTrailer).FirstOrDefault();

            int statusValue;
            if (grpcStatus == null)
            {
                throw new InvalidOperationException("Response did not have a grpc-status trailer.");
            }
            else if (!int.TryParse(grpcStatus, out statusValue))
            {
                throw new InvalidOperationException("Unexpected grpc-status value: " + grpcStatus);
            }

            return new Status((StatusCode)statusValue, grpcMessage);
        }

        public Metadata GetTrailers()
        {
            if (_trailers == null)
            {
                ValidateTrailersAvailable();

                _trailers = GrpcProtocolHelpers.BuildMetadata(SendTask.Result.TrailingHeaders);
            }

            return _trailers;
        }

        private void ValidateTrailersAvailable()
        {
            // Async call could have been disposed
            EnsureNotDisposed();

            // HttpClient.SendAsync could have failed
            if (SendTask.IsFaulted)
            {
                throw new InvalidOperationException("Can't get the call trailers because an error occured when making the request.", SendTask.Exception);
            }

            // Response could still be in progress
            if (!ResponseFinished || !SendTask.IsCompletedSuccessfully)
            {
                throw new InvalidOperationException("Can't get the call trailers because the call is not complete.");
            }
        }
    }
}