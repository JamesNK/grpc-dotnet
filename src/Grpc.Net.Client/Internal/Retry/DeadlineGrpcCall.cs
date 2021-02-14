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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace Grpc.Net.Client.Internal.Retry
{
    internal sealed class DeadlineGrpcCall<TRequest, TResponse> : IGrpcCall<TRequest, TResponse>
        where TRequest : class
        where TResponse : class
    {
        private static readonly Status DeadlineStatus = new Status(StatusCode.DeadlineExceeded, string.Empty);
        private static DeadlineGrpcCall<TRequest, TResponse>? _instance;

        public static DeadlineGrpcCall<TRequest, TResponse> Instance
        {
            get { return _instance ??= new DeadlineGrpcCall<TRequest, TResponse>(); }
        }

        public IClientStreamWriter<TRequest>? ClientStreamWriter { get; }
        public IAsyncStreamReader<TResponse>? ClientStreamReader { get; }

        public DeadlineGrpcCall()
        {
            ClientStreamWriter = new DeadClientStreamWriter();
            ClientStreamReader = new DeadlineStreamReader();
        }

        public void Dispose()
        {
        }

        public Task<TResponse> GetResponseAsync()
        {
            return Task.FromException<TResponse>(new RpcException(DeadlineStatus));
        }

        public Task<Metadata> GetResponseHeadersAsync()
        {
            return Task.FromException<Metadata>(new RpcException(DeadlineStatus));
        }

        public Status GetStatus()
        {
            return DeadlineStatus;
        }

        public Metadata GetTrailers()
        {
            throw new InvalidOperationException("Can't get the call trailers because the call has not completed successfully.");
        }

        public void StartClientStreaming()
        {
            throw new NotSupportedException();
        }

        public void StartDuplexStreaming()
        {
            throw new NotSupportedException();
        }

        public void StartServerStreaming(TRequest request)
        {
            throw new NotSupportedException();
        }

        public void StartUnary(TRequest request)
        {
            throw new NotSupportedException();
        }

        public Task WriteClientStreamAsync<TState>(Func<GrpcCall<TRequest, TResponse>, Stream, CallOptions, TState, ValueTask> writeFunc, TState state)
        {
            return Task.FromException(new RpcException(DeadlineStatus));
        }

        private sealed class DeadClientStreamWriter : IClientStreamWriter<TRequest>
        {
            public WriteOptions? WriteOptions { get; set; }

            public Task CompleteAsync()
            {
                return Task.FromException(new RpcException(DeadlineStatus));
            }

            public Task WriteAsync(TRequest message)
            {
                return Task.FromException(new RpcException(DeadlineStatus));
            }
        }

        private sealed class DeadlineStreamReader : IAsyncStreamReader<TResponse>
        {
            public TResponse Current { get; set; } = default!;

            public Task<bool> MoveNext(CancellationToken cancellationToken)
            {
                return Task.FromException<bool>(new RpcException(DeadlineStatus));
            }
        }
    }
}
