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

using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.AspNetCore.FunctionalTests;
using Grpc.Net.Client.Balancer;
using NUnit.Framework;

namespace Grpc.Net.Client.Tests.Balancer
{
    [TestFixture]
    public class DnsAddressResolverTests : FunctionalTestBase
    {
        [Test]
        public async Task RefreshAsync_OneSubscriber_HasResult()
        {
            var testObserver = new TestObserver();

            var dnsResolver = new DnsAddressResolver(new Uri("dns://localhost"), LoggerFactory);
            dnsResolver.Subscribe(testObserver);

            await dnsResolver.RefreshAsync(CancellationToken.None);

            var result = testObserver.Result;
            Assert.NotNull(result);
            Assert.Greater(result!.Addresses.Count, 0);
        }

        private class TestObserver : IObserver<AddressResolverResult>
        {
            public AddressResolverResult? Result { get; private set; }
            public Exception? Error { get; private set; }

            public void OnCompleted()
            {
            }

            public void OnError(Exception error)
            {
                Result = null;
                Error = error;
            }

            public void OnNext(AddressResolverResult value)
            {
                Result = value;
                Error = null;
            }
        }
    }
}

#endif