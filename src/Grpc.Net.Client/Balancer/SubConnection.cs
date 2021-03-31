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
using System.Net.Http;
using System.Threading.Tasks;
using Grpc.Core;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Grpc.Net.Client.Balancer
{
    public class SubConnection
    {
        public Task ConnectAsync()
        {
            return Task.CompletedTask;
        }
    }

    public class BalancerState
    {
        public ConnectivityState ConnectivityState { get; set; }
        public Picker? Picker { get; set; }
    }

    public class SubConnectionState
    {
        public ConnectivityState ConnectivityState { get; set; }
        public Exception? ConnectionError { get; set; }
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

        public PickContext(HttpRequestMessage request)
        {
            Request = request;
        }
    }

    public class PickResult
    {
        private readonly Action<CompleteContext> _onComplete;

        public PickResult(SubConnection subConnection, Action<CompleteContext> onComplete)
        {
            SubConnection = subConnection;
            _onComplete = onComplete;
        }

        public SubConnection SubConnection { get; }

        public void Complete(CompleteContext context)
        {
            _onComplete(context);
        }
    }

    public class CompleteContext
    {
        public Exception? Error { get; set; }
        public Metadata? Trailers { get; set; }
    }

    public class Picker
    {
        public PickResult Pick(PickContext context)
        {
            return new PickResult(new SubConnection(), c => { });
        }
    }

    public class SubConnectionOptions
    {
        public string? Address { get; set; }
    }

    public abstract class ClientConnectionBase
    {
        public abstract SubConnection CreateSubConnection(SubConnectionOptions options);
        public abstract void RemoveSubConnection(SubConnection subConnection);
    }
}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
