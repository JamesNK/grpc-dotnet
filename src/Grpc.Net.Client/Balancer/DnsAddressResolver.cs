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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

#if HAVE_LOAD_BALANCING
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Grpc.Net.Client.Balancer
{
    public class DnsAddressResolver : AddressResolver
    {
        private Timer? _timer;
        private readonly List<IObserver<AddressResolverResult>> _subscriptions = new List<IObserver<AddressResolverResult>>();
        private readonly object _lock = new object();
        private readonly Uri _address;
        private readonly ILogger<DnsAddressResolver> _logger;

        public DnsAddressResolver(Uri address, ILoggerFactory loggerFactory)
        {
            _address = address;
            _logger = loggerFactory.CreateLogger<DnsAddressResolver>();
        }

        public override async Task RefreshAsync(CancellationToken cancellationToken)
        {
            var dnsAddress = _address.Host;
            _logger.LogTrace($"Getting DNS hosts from {dnsAddress}");

            try
            {
                var addresses = await Dns.GetHostAddressesAsync(dnsAddress).ConfigureAwait(false);

                _logger.LogTrace($"{addresses.Length} DNS results from {dnsAddress}: " + string.Join<IPAddress>(", ", addresses));

                var resolvedPort = _address.Port == -1 ? 80 : _address.Port;
                var endpoints = addresses.Select(a => new DnsEndPoint(a.ToString(), resolvedPort)).ToArray();
                var addressResolverResult = new AddressResolverResult(endpoints);
                lock (_lock)
                {
                    foreach (var subscription in _subscriptions)
                    {
                        subscription.OnNext(addressResolverResult);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error getting DNS hosts from {dnsAddress}");
                lock (_lock)
                {
                    foreach (var subscription in _subscriptions)
                    {
                        subscription.OnError(ex);
                    }
                }
            }
        }

        public override void Shutdown()
        {
            _timer?.Dispose();

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
                if (_subscriptions.Count == 0)
                {
                    _timer = new Timer(OnTimerCallback, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                    _timer.Change(TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));
                }
                _subscriptions.Add(observer);
            }

            return new Subscription(this, observer);
        }

        private void OnTimerCallback(object? state)
        {
            _ = RefreshAsync(CancellationToken.None);
        }

        private void Unsubscribe(IObserver<AddressResolverResult> observer)
        {
            lock (_lock)
            {
                _subscriptions.Remove(observer);

                if (_subscriptions.Count == 0)
                {
                    _timer?.Dispose();
                    _timer = null;
                }
            }
        }

        private class Subscription : IDisposable
        {
            private readonly DnsAddressResolver _nameResolver;
            private readonly IObserver<AddressResolverResult> _observer;

            public Subscription(DnsAddressResolver nameResolver, IObserver<AddressResolverResult> observer)
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

    public class DnsAddressResolverFactory : AddressResolverFactory
    {
        private readonly ILoggerFactory _loggerFactory;

        public DnsAddressResolverFactory(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
        }

        public override string Name => "dns";

        public override AddressResolver Create(Uri address, AddressResolverOptions options)
        {
            return new DnsAddressResolver(address, _loggerFactory);
        }
    }
}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif
