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
    public class RetryTests : FunctionalTestBase
    {
        [Test]
        public async Task ClientStreaming_MultipleWritesAndRetries_Failure()
        {
            int nextFailure = 1;

            async Task<DataMessage> UnaryDeadlineExceeded(IAsyncStreamReader<DataMessage> requestStream, ServerCallContext context)
            {
                List<byte> bytes = new List<byte>();
                await foreach (var message in requestStream.ReadAllAsync())
                {
                    if (bytes.Count >= nextFailure)
                    {
                        nextFailure = nextFailure * 2;
                        throw new RpcException(new Status(StatusCode.Unavailable, ""));
                    }

                    bytes.Add(message.Data[0]);
                }

                return new DataMessage
                {
                    Data = ByteString.CopyFrom(bytes.ToArray())
                };
            }

            SetExpectedErrorsFilter(writeContext =>
            {
                return true;
            });

            // Arrange
            var method = Fixture.DynamicGrpc.AddClientStreamingMethod<DataMessage, DataMessage>(UnaryDeadlineExceeded);

            var channel = CreateChannel(serviceConfig: CreateServiceConfig(maxAttempts: 10));

            var client = TestClientFactory.Create(channel, method);

            // Act
            var call = client.ClientStreamingCall();

            for (int i = 0; i < 20; i++)
            {
                await call.RequestStream.WriteAsync(new DataMessage { Data = ByteString.CopyFrom(new byte[] { (byte)i }) }).DefaultTimeout();
                await Task.Delay(1);
            }

            await call.RequestStream.CompleteAsync().DefaultTimeout();

            var result = await call.ResponseAsync.DefaultTimeout();

            // Assert
            foreach (var item in result.Data)
            {
                Console.WriteLine(item);
            }

            Assert.AreEqual(20, result.Data.Length);
        }

        [Test]
        public async Task Unary_ExceedRetryAttempts_Failure()
        {
            Task<DataMessage> UnaryFailure(DataMessage request, ServerCallContext context)
            {
                var metadata = new Metadata();
                metadata.Add("grpc-retry-pushback-ms", "5");

                return Task.FromException<DataMessage>(new RpcException(new Status(StatusCode.Unavailable, ""), metadata));
            }

            // Arrange
            var method = Fixture.DynamicGrpc.AddUnaryMethod<DataMessage, DataMessage>(UnaryFailure);

            var channel = CreateChannel(serviceConfig: CreateServiceConfig());

            var client = TestClientFactory.Create(channel, method);

            // Act
            var call = client.UnaryCall(new DataMessage());

            // Assert
            var ex = await ExceptionAssert.ThrowsAsync<RpcException>(() => call.ResponseAsync).DefaultTimeout();
            Assert.AreEqual(StatusCode.Unavailable, ex.StatusCode);
            Assert.AreEqual(StatusCode.Unavailable, call.GetStatus().StatusCode);

            AssertHasLog(LogLevel.Debug, "RetryPushbackReceived", "Retry pushback of '5' received from the failed gRPC call.");
            AssertHasLog(LogLevel.Debug, "RetryEvaluated", "Evaluated retry decision for failed gRPC call. Status code: 'Unavailable', Attempt: 1, Decision: Retry");
            AssertHasLog(LogLevel.Trace, "StartingRetryDelay", "Starting retry delay of 00:00:00.0050000.");
            AssertHasLog(LogLevel.Debug, "RetryEvaluated", "Evaluated retry decision for failed gRPC call. Status code: 'Unavailable', Attempt: 5, Decision: ExceededAttemptCount");
        }

        [Test]
        public async Task Unary_TriggerRetryThrottling_Failure()
        {
            var callCount = 0;
            Task<DataMessage> UnaryFailure(DataMessage request, ServerCallContext context)
            {
                callCount++;
                return Task.FromException<DataMessage>(new RpcException(new Status(StatusCode.Unavailable, "")));
            }

            // Arrange
            var method = Fixture.DynamicGrpc.AddUnaryMethod<DataMessage, DataMessage>(UnaryFailure);

            var channel = CreateChannel(serviceConfig: CreateServiceConfig(retryThrottling: new RetryThrottlingPolicy
            {
                MaxTokens = 5,
                TokenRatio = 0.1
            }));

            var client = TestClientFactory.Create(channel, method);

            // Act
            var call = client.UnaryCall(new DataMessage());

            // Assert
            var ex = await ExceptionAssert.ThrowsAsync<RpcException>(() => call.ResponseAsync).DefaultTimeout();
            Assert.AreEqual(StatusCode.Unavailable, ex.StatusCode);
            Assert.AreEqual(StatusCode.Unavailable, call.GetStatus().StatusCode);

            AssertHasLog(LogLevel.Debug, "RetryEvaluated", "Evaluated retry decision for failed gRPC call. Status code: 'Unavailable', Attempt: 3, Decision: Throttled");
        }

        private static ServiceConfig CreateServiceConfig(int? maxAttempts = null, double? backoffMultiplier = null, RetryThrottlingPolicy? retryThrottling = null)
        {
            return new ServiceConfig
            {
                MethodConfigs =
                {
                    new MethodConfig
                    {
                        Names = { Name.All },
                        RetryPolicy = new RetryPolicy
                        {
                            MaxAttempts = maxAttempts ?? 5,
                            InitialBackoff = TimeSpan.Zero,
                            MaxBackoff = TimeSpan.Zero,
                            BackoffMultiplier = backoffMultiplier ?? 1.1,
                            RetryableStatusCodes = { StatusCode.Unavailable }
                        }
                    }
                },
                RetryThrottling = retryThrottling
            };
        }
    }
}
