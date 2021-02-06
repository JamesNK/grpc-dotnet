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
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Retry;

namespace Server
{
    public class RetrierService : Retrier.RetrierBase
    {
        private const double DeliveryChance = 0.8;

        private readonly Random _random = new Random();
        private readonly ILogger<RetrierService> _logger;

        public RetrierService(ILogger<RetrierService> logger)
        {
            _logger = logger;
        }

        public override Task<PackageReceipt> DeliverPackage(PackageMessage request, ServerCallContext context)
        {
            if (_random.NextDouble() > DeliveryChance)
            {
                throw new RpcException(new Status(StatusCode.Unavailable, $"{request.Name} not delivered."));
            }

            return Task.FromResult(new PackageReceipt
            {
                Message = $"{request.Name} successfully delivered."
            });
        }

        public override async Task<PackageReceipt> DeliverPackages(
            IAsyncStreamReader<PackageMessage> requestStream, ServerCallContext context)
        {
            List<PackageMessage> deliveredPackages = new List<PackageMessage>();

            Console.WriteLine("New delivery");
            await foreach (var package in requestStream.ReadAllAsync())
            {
                if (_random.NextDouble() > DeliveryChance)
                {
                    throw new RpcException(new Status(StatusCode.Unavailable, $"Packages not delivered."));
                }

                Console.WriteLine(package.Name);
                //_logger.LogInformation($"{package.Name}");
                deliveredPackages.Add(package);
            }

            return new PackageReceipt
            {
                Message = $"{deliveredPackages.Count} packages successfully delivered."
            };
        }
    }
}
