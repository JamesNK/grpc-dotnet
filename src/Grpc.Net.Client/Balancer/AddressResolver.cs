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
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

#if NET5_0_OR_GREATER
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Grpc.Net.Client.Balancer
{
    public abstract class AddressResolver : IObservable<AddressResolverResult>
    {
        public abstract IDisposable Subscribe(IObserver<AddressResolverResult> observer);
        public abstract void Shutdown();
        public abstract Task RefreshAsync(CancellationToken cancellationToken);
    }

    public class AddressResolverResult
    {
        private GrpcAttributes? _attributes;

        public AddressResolverResult(IReadOnlyList<DnsEndPoint> addresses)
        {
            Addresses = addresses;
        }

        public IReadOnlyList<DnsEndPoint> Addresses { get; }
        public GrpcAttributes Attributes => _attributes ??= new GrpcAttributes();
    }
}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif
