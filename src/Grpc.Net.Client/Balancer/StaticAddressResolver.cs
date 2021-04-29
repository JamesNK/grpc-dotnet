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
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

#if HAVE_LOAD_BALANCING
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Grpc.Net.Client.Balancer
{
    public class StaticAddressResolver : AddressResolver
    {
        private readonly List<DnsEndPoint> _addresses;
        private readonly List<IObserver<AddressResolverResult>> _subscriptions = new List<IObserver<AddressResolverResult>>();
        private readonly object _lock = new object();

        public StaticAddressResolver(IEnumerable<DnsEndPoint> addresses)
        {
            _addresses = addresses.ToList();
        }

        public override Task RefreshAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public override void Shutdown()
        {
            lock (_lock)
            {
                foreach (var subscription in _subscriptions)
                {
                    subscription.OnCompleted();
                }
                _subscriptions.Clear();
            }
        }

        public override IDisposable Subscribe(IObserver<AddressResolverResult> observer)
        {
            lock (_lock)
            {
                _subscriptions.Add(observer);
            }
            observer.OnNext(new AddressResolverResult(_addresses));
            return new Subscription(this, observer);
        }

        private void Unsubscribe(IObserver<AddressResolverResult> observer)
        {
            lock (_lock)
            {
                _subscriptions.Remove(observer);
            }
        }

        private class Subscription : IDisposable
        {
            private readonly StaticAddressResolver _nameResolver;
            private readonly IObserver<AddressResolverResult> _observer;

            public Subscription(StaticAddressResolver nameResolver, IObserver<AddressResolverResult> observer)
            {
                _nameResolver = nameResolver;
                _observer = observer;
            }

            public void Dispose()
            {
                _nameResolver.Unsubscribe(_observer);
            }
        }
    }
}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif
