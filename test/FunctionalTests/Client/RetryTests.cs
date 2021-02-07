﻿#region Copyright notice and license

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

            var channel = CreateChannel(serviceConfig: ServiceConfigHelpers.CreateServiceConfig(maxAttempts: 10));

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

            var channel = CreateChannel(serviceConfig: ServiceConfigHelpers.CreateServiceConfig());

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

            var channel = CreateChannel(serviceConfig: ServiceConfigHelpers.CreateServiceConfig(retryThrottling: new RetryThrottlingPolicy
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

        [TestCase(1)]
        [TestCase(2)]
        public async Task Unary_DeadlineExceedAfterServerCall_Failure(int exceptedServerCallCount)
        {
            var callCount = 0;
            var tcs = new TaskCompletionSource<DataMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            Task<DataMessage> UnaryFailure(DataMessage request, ServerCallContext context)
            {
                callCount++;

                if (callCount < exceptedServerCallCount)
                {
                    return Task.FromException<DataMessage>(new RpcException(new Status(StatusCode.DeadlineExceeded, "")));
                }

                return tcs.Task;
            }

            // Arrange
            var method = Fixture.DynamicGrpc.AddUnaryMethod<DataMessage, DataMessage>(UnaryFailure);

            var serviceConfig = ServiceConfigHelpers.CreateServiceConfig(retryableStatusCodes: new List<StatusCode> { StatusCode.DeadlineExceeded });
            var channel = CreateChannel(serviceConfig: serviceConfig);

            var client = TestClientFactory.Create(channel, method);

            // Act
            var call = client.UnaryCall(new DataMessage(), new CallOptions(deadline: DateTime.UtcNow.AddMilliseconds(200)));

            // Assert
            var ex = await ExceptionAssert.ThrowsAsync<RpcException>(() => call.ResponseAsync).DefaultTimeout();
            Assert.AreEqual(StatusCode.DeadlineExceeded, ex.StatusCode);
            Assert.AreEqual(StatusCode.DeadlineExceeded, call.GetStatus().StatusCode);
            Assert.AreEqual(exceptedServerCallCount, callCount);

            AssertHasLog(LogLevel.Debug, "RetryEvaluated", $"Evaluated retry decision for failed gRPC call. Status code: 'DeadlineExceeded', Attempt: {exceptedServerCallCount}, Decision: DeadlineExceeded");

            tcs.SetResult(new DataMessage());
        }

        [Test]
        public async Task Unary_DeadlineExceedBeforeServerCall_Failure()
        {
            var callCount = 0;
            var tcs = new TaskCompletionSource<DataMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            Task<DataMessage> UnaryFailure(DataMessage request, ServerCallContext context)
            {
                callCount++;
                return tcs.Task;
            }

            // Arrange
            var method = Fixture.DynamicGrpc.AddUnaryMethod<DataMessage, DataMessage>(UnaryFailure);

            var serviceConfig = ServiceConfigHelpers.CreateServiceConfig(retryableStatusCodes: new List<StatusCode> { StatusCode.DeadlineExceeded });
            var channel = CreateChannel(serviceConfig: serviceConfig);

            var client = TestClientFactory.Create(channel, method);

            // Act
            var call = client.UnaryCall(new DataMessage(), new CallOptions(deadline: DateTime.UtcNow));

            // Assert
            var ex = await ExceptionAssert.ThrowsAsync<RpcException>(() => call.ResponseAsync).DefaultTimeout();
            Assert.AreEqual(StatusCode.DeadlineExceeded, ex.StatusCode);
            Assert.AreEqual(StatusCode.DeadlineExceeded, call.GetStatus().StatusCode);
            Assert.AreEqual(0, callCount);

            AssertHasLog(LogLevel.Debug, "RetryEvaluated", "Evaluated retry decision for failed gRPC call. Status code: 'DeadlineExceeded', Attempt: 1, Decision: DeadlineExceeded");

            tcs.SetResult(new DataMessage());
        }

        //[Test]
        //public async Task Unary_DeadlineExceed_Failure()
        //{
        //    // Arrange

        //    var callCount = 0;
        //    var httpClient = ClientTestHelpers.CreateTestClient(async request =>
        //    {
        //        callCount++;
        //        await request.Content!.CopyToAsync(new MemoryStream());
        //        return await tcs.Task;
        //    });
        //    var serviceConfig = CreateServiceConfig(retryableStatusCodes: new List<StatusCode> { StatusCode.DeadlineExceeded });
        //    var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

        //    // Act
        //    var call = invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions(deadline: DateTime.UtcNow.AddMilliseconds(50)), new HelloRequest { Name = "World" });

        //    // Assert
        //    Assert.GreaterOrEqual(1, callCount);
        //    var ex = await ExceptionAssert.ThrowsAsync<RpcException>(() => call.ResponseAsync).DefaultTimeout();
        //    Assert.AreEqual(StatusCode.DeadlineExceeded, ex.StatusCode);
        //    Assert.AreEqual(StatusCode.DeadlineExceeded, call.GetStatus().StatusCode);

        //    tcs.SetException(new OperationCanceledException());
        //}
    }
}
