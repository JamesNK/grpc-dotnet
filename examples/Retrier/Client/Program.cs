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
using System.Threading.Tasks;
using Retry;
using Grpc.Net.Client;
using System.Collections.Generic;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Grpc.Net.Client.Configuration;

namespace Client
{
    public class Program
    {
        static async Task Main(string[] args)
        {
            using var channel = CreateChannel();
            var client = new Retrier.RetrierClient(channel);

            //await UnaryRetry(client);
            await ServerStreamingRetry(client);

            Console.WriteLine("Shutting down");
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        private static async Task UnaryRetry(Retrier.RetrierClient client)
        {
            foreach (var product in Products)
            {
                try
                {
                    var call = client.DeliverPackageAsync(new PackageMessage { Name = product });
                    var receipt = await call;

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.Write(receipt.Message);
                    Console.ResetColor();
                    Console.Write(" " + await GetRetryCount(call));
                    Console.WriteLine();
                }
                catch (RpcException ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(ex.Status.Detail);
                    Console.ResetColor();
                }

                await Task.Delay(200);
            }
        }

        private static async Task<string> GetRetryCount(AsyncUnaryCall<PackageReceipt> call)
        {
            var headers = await call.ResponseHeadersAsync;
            var previousAttemptCount = headers.GetValue("grpc-previous-rpc-attempts");
            return previousAttemptCount != null ? $"(retry count: {previousAttemptCount})" : string.Empty;
        }

        private static async Task ServerStreamingRetry(Retrier.RetrierClient client)
        {
            var call = client.DeliverPackages();

            try
            {
                Products.Clear();
                for (int i = 0; i < 5; i++)
                {
                    Products.Add(((char)('a' + i)).ToString());
                }

                foreach (var product in Products)
                {
                    //string p = Products.IndexOf(product).ToString() + "!!!";
                    Console.WriteLine("Sending " + product);
                    await call.RequestStream.WriteAsync(new PackageMessage { Name = product });

                    await Task.Delay(1000);
                }

                await call.RequestStream.CompleteAsync();

                var receipt = await call;
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine(receipt.Message);
            }
            catch (RpcException ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.ToString());
            }

            Console.ResetColor();
        }

        private static GrpcChannel CreateChannel()
        {
            var factory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
            {
                builder.AddConsole();
                builder.SetMinimumLevel(LogLevel.None);
            });
            var options = new GrpcChannelOptions
            {
                ServiceConfig = new ServiceConfig
                {
                    MethodConfigs =
                    {
                        new MethodConfig
                        {
                            Names = { Name.All },
                            RetryPolicy = new RetryPolicy
                            {
                                MaxAttempts = 100,
                                InitialBackoff = TimeSpan.FromSeconds(0.5),
                                BackoffMultiplier = 2,
                                MaxBackoff = TimeSpan.FromSeconds(1),
                                RetryableStatusCodes = { StatusCode.Unavailable }
                            }
                        }
                    }
                },
                LoggerFactory = factory
            };
            return GrpcChannel.ForAddress("http://localhost:5000", options);
        }

        private static readonly IList<string> Products = new List<string>
        {
            "Secrets of Silicon Valley",
            "The Busy Executive's Database Guide",
            "Emotional Security: A New Algorithm",
            "Prolonged Data Deprivation: Four Case Studies",
            "Cooking with Computers: Surreptitious Balance Sheets",
            "Silicon Valley Gastronomic Treats",
            "Sushi, Anyone?",
            "Fifty Years in Buckingham Palace Kitchens",
            "But Is It User Friendly?",
            "You Can Combat Computer Stress!",
            "Is Anger the Enemy?",
            "Life Without Fear",
            "The Gourmet Microwave",
            "Onions, Leeks, and Garlic: Cooking Secrets of the Mediterranean",
            "The Psychology of Computer Cooking",
            "Straight Talk About Computers",
            "Computer Phobic AND Non-Phobic Individuals: Behavior Variations",
            "Net Etiquette"
        };
    }
}