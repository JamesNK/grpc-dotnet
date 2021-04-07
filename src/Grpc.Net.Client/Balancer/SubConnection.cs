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

#if NET5_0_OR_GREATER
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client.Balancer;
using Grpc.Shared;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Balancer
{
    public abstract class SubConnection
    {
        public abstract ConnectivityState State { get; }
        public abstract ValueTask<Stream> GetStreamAsync(DnsEndPoint endPoint, CancellationToken cancellationToken);
        public abstract DnsEndPoint? CurrentEndPoint { get; }
        public abstract Task ConnectAsync(CancellationToken cancellationToken);
    }

    public class BalancerState
    {
        [DebuggerStepThrough]
        public BalancerState(ConnectivityState connectivityState, SubConnectionPicker picker)
        {
            ConnectivityState = connectivityState;
            Picker = picker;
        }

        public ConnectivityState ConnectivityState { get; }
        public SubConnectionPicker Picker { get; }
    }

    public class SubConnectionState
    {
        public ConnectivityState ConnectivityState { get; set; }
        public Exception? ConnectionError { get; set; }
    }

    public class ConnectionState
    {
        [DebuggerStepThrough]
        public ConnectionState(AddressResolverResult resolverState, GrpcAttributes options)
        {
            ResolverState = resolverState;
            Options = options;
        }

        public AddressResolverResult ResolverState { get; set; }
        public GrpcAttributes Options { get; set; }
    }

    public enum ConnectivityState
    {
        Idle,
        Connecting,
        Ready,
        TransientFailure,
        Shutdown
    }

    public class PickContext
    {
        public HttpRequestMessage Request { get; }

        [DebuggerStepThrough]
        public PickContext(HttpRequestMessage request)
        {
            Request = request;
        }
    }

    public class PickResult
    {
        private readonly Action<CompleteContext>? _onComplete;

        [DebuggerStepThrough]
        public PickResult(SubConnection? subConnection, Action<CompleteContext>? onComplete)
        {
            SubConnection = subConnection;
            _onComplete = onComplete;
        }

        public SubConnection? SubConnection { get; }

        public void Complete(CompleteContext context)
        {
            _onComplete?.Invoke(context);
        }
    }

    public class CompleteContext
    {
        public Exception? Error { get; set; }
        public Metadata? Trailers { get; set; }
    }

}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif