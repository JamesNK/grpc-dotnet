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
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.AspNetCore.FunctionalTests.Infrastructure;
using Grpc.Core;
using Grpc.Net.Client.Configuration;
using Grpc.Tests.Shared;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Streaming;

namespace Grpc.AspNetCore.FunctionalTests.Client
{
    [TestFixture]
    public class HedgingTests : FunctionalTestBase
    {
        [TestCase(0)]
        [TestCase(20)]
        public async Task Unary_ExceedAttempts_Failure(int hedgingDelay)
        {
            Task<DataMessage> UnaryFailure(DataMessage request, ServerCallContext context)
            {
                return Task.FromException<DataMessage>(new RpcException(new Status(StatusCode.Unavailable, "")));
            }

            // Ignore errors
            SetExpectedErrorsFilter(writeContext =>
            {
                return true;
            });

            // Arrange
            var method = Fixture.DynamicGrpc.AddUnaryMethod<DataMessage, DataMessage>(UnaryFailure);

            var channel = CreateChannel(serviceConfig: ServiceConfigHelpers.CreateHedgingServiceConfig(maxAttempts: 5, hedgingDelay: TimeSpan.FromMilliseconds(hedgingDelay)));

            var client = TestClientFactory.Create(channel, method);

            // Act
            var call = client.UnaryCall(new DataMessage());

            // Assert
            var ex = await ExceptionAssert.ThrowsAsync<RpcException>(() => call.ResponseAsync).DefaultTimeout();
            Assert.AreEqual(StatusCode.Unavailable, ex.StatusCode);
            Assert.AreEqual(StatusCode.Unavailable, call.GetStatus().StatusCode);

            AssertHasLog(LogLevel.Debug, "CallCommited", "Call commited. Reason: FinalCall");
        }

        [Test]
        public async Task Duplex_ManyParallelRequests_MessageRoundTripped()
        {
            var attempts = 100;
            var allUploads = new List<string>();
            var allCompletedTasks = new List<Task>();
            var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);

            async Task MessageUpload(
                IAsyncStreamReader<StringValue> requestStream,
                IServerStreamWriter<StringValue> responseStream,
                ServerCallContext context)
            {
                // Receive chunks
                var chunks = new List<string>();
                await foreach (var chunk in requestStream.ReadAllAsync())
                {
                    chunks.Add(chunk.Value);
                }

                Task completeTask;
                lock (allUploads)
                {
                    allUploads.Add(string.Join(Environment.NewLine, chunks));
                    if (allUploads.Count < attempts)
                    {
                        // Check that unused calls are canceled.
                        completeTask = Task.Run(async () =>
                        {
                            await tcs.Task;

                            var cancellationTcs = new TaskCompletionSource<bool>();
                            context.CancellationToken.Register(s => ((TaskCompletionSource<bool>)s!).SetResult(true), cancellationTcs);
                            await cancellationTcs.Task;
                        });
                    }
                    else
                    {
                        // Write response in used call.
                        completeTask = Task.Run(async () =>
                        {
                            // Write chunks
                            foreach (var chunk in chunks)
                            {
                                await responseStream.WriteAsync(new StringValue
                                {
                                    Value = chunk
                                });
                            }
                        });
                    }
                }

                await completeTask;
            }

            var method = Fixture.DynamicGrpc.AddDuplexStreamingMethod<StringValue, StringValue>(MessageUpload);

            var channel = CreateChannel(serviceConfig: ServiceConfigHelpers.CreateHedgingServiceConfig(maxAttempts: 100, hedgingDelay: TimeSpan.Zero));

            var client = TestClientFactory.Create(channel, method);

            using var call = client.DuplexStreamingCall();

            var lines = ImportantMessage.Split(Environment.NewLine);
            for (var i = 0; i < lines.Length; i++)
            {
                await call.RequestStream.WriteAsync(new StringValue { Value = lines[i] }).DefaultTimeout();
                await Task.Delay(TimeSpan.FromSeconds(0.01)).DefaultTimeout();
            }
            await call.RequestStream.CompleteAsync().DefaultTimeout();

            await TestHelpers.AssertIsTrueRetryAsync(() => allUploads.Count == 100, "Wait for all calls to reach server.").DefaultTimeout();
            tcs.SetResult(null);

            var receivedLines = new List<string>();
            await foreach (var line in call.ResponseStream.ReadAllAsync().DefaultTimeout())
            {
                receivedLines.Add(line.Value);
            }

            Assert.AreEqual(ImportantMessage, string.Join(Environment.NewLine, receivedLines));

            foreach (var upload in allUploads)
            {
                Assert.AreEqual(ImportantMessage, upload);
            }

            await Task.WhenAll(allCompletedTasks).DefaultTimeout();
        }

        private static readonly string ImportantMessage =
@"       _____  _____   _____ 
       |  __ \|  __ \ / ____|
   __ _| |__) | |__) | |     
  / _` |  _  /|  ___/| |     
 | (_| | | \ \| |    | |____ 
  \__, |_|  \_\_|     \_____|
   __/ |                     
  |___/                      
  _                          
 (_)                         
  _ ___                      
 | / __|                     
 | \__ \          _          
 |_|___/         | |         
   ___ ___   ___ | |         
  / __/ _ \ / _ \| |         
 | (_| (_) | (_) | |         
  \___\___/ \___/|_|         
                             
                             ";
    }
}
