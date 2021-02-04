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
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Greet;
using Grpc.Core;
using Grpc.Net.Client.Internal;
using Grpc.Net.Client.Tests.Infrastructure;
using Grpc.Tests.Shared;
using NUnit.Framework;

namespace Grpc.Net.Client.Tests
{
    [TestFixture]
    public class RetryTests
    {
        [Test]
        public async Task AsyncUnaryCall_SuccessAfterRetry_RequestContentSent()
        {
            // Arrange
            HttpContent? content = null;

            var callCount = 0;
            var httpClient = ClientTestHelpers.CreateTestClient(async request =>
            {
                callCount++;
                content = request.Content!;

                if (callCount == 1)
                {
                    await content.CopyToAsync(new MemoryStream());
                    return ResponseUtils.CreateHeadersOnlyResponse(HttpStatusCode.OK, StatusCode.Unavailable);
                }

                var reply = new HelloReply
                {
                    Message = "Hello world"
                };

                var streamContent = await ClientTestHelpers.CreateResponseContent(reply).DefaultTimeout();

                return ResponseUtils.CreateResponse(HttpStatusCode.OK, streamContent);
            });
            var serviceConfig = CreateServiceConfig();
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var call = invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions(), new HelloRequest { Name = "World" });

            // Assert
            Assert.AreEqual(2, callCount);
            Assert.AreEqual("Hello world", (await call.ResponseAsync.DefaultTimeout()).Message);

            Assert.IsNotNull(content);

            var requestContent = await content!.ReadAsStreamAsync().DefaultTimeout();
            var requestMessage = await ReadRequestMessage(requestContent).DefaultTimeout();

            Assert.AreEqual("World", requestMessage!.Name);
        }

        [Test]
        public async Task AsyncUnaryCall_SuccessAfterRetry_AccessResponseHeaders_SuccessfullyResponseHeadersReturned()
        {
            // Arrange
            HttpContent? content = null;
            var syncPoint = new SyncPoint(runContinuationsAsynchronously: true);

            var callCount = 0;
            var httpClient = ClientTestHelpers.CreateTestClient(async request =>
            {
                callCount++;
                content = request.Content!;

                if (callCount == 1)
                {
                    await content.CopyToAsync(new MemoryStream());

                    await syncPoint.WaitForSyncPoint();

                    return ResponseUtils.CreateHeadersOnlyResponse(
                        HttpStatusCode.OK,
                        StatusCode.Unavailable,
                        customHeaders: new Dictionary<string, string> { ["call-count"] = callCount.ToString() });
                }

                syncPoint.Continue();

                var reply = new HelloReply { Message = "Hello world" };
                var streamContent = await ClientTestHelpers.CreateResponseContent(reply).DefaultTimeout();

                return ResponseUtils.CreateResponse(
                    HttpStatusCode.OK,
                    streamContent,
                    customHeaders: new Dictionary<string, string> { ["call-count"] = callCount.ToString() });
            });
            var serviceConfig = CreateServiceConfig();
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var call = invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions(), new HelloRequest { Name = "World" });
            var headersTask = call.ResponseHeadersAsync;

            // Wait until the first call has failed and the second is on the server
            await syncPoint.WaitToContinue();

            // Assert
            Assert.AreEqual(2, callCount);
            Assert.AreEqual("Hello world", (await call.ResponseAsync.DefaultTimeout()).Message);

            var headers = await headersTask.DefaultTimeout();
            Assert.AreEqual("2", headers.GetValue("call-count"));
        }

        [Test]
        public async Task AsyncUnaryCall_ExceedRetryAttempts_Failure()
        {
            // Arrange
            var callCount = 0;
            var httpClient = ClientTestHelpers.CreateTestClient(async request =>
            {
                callCount++;
                await request.Content!.CopyToAsync(new MemoryStream());
                return ResponseUtils.CreateHeadersOnlyResponse(HttpStatusCode.OK, StatusCode.Unavailable);
            });
            var serviceConfig = CreateServiceConfig(maxAttempts: 3);
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var call = invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions(), new HelloRequest { Name = "World" });

            // Assert
            Assert.AreEqual(3, callCount);
            var ex = await ExceptionAssert.ThrowsAsync<RpcException>(() => call.ResponseAsync).DefaultTimeout();
            Assert.AreEqual(StatusCode.Unavailable, ex.StatusCode);
            Assert.AreEqual(StatusCode.Unavailable, call.GetStatus().StatusCode);
        }

        [TestCase("")]
        [TestCase("-1")]
        [TestCase("stop")]
        public async Task AsyncUnaryCall_PushbackStop_Failure(string header)
        {
            // Arrange
            var callCount = 0;
            var httpClient = ClientTestHelpers.CreateTestClient(async request =>
            {
                callCount++;
                await request.Content!.CopyToAsync(new MemoryStream());
                return ResponseUtils.CreateResponse(HttpStatusCode.OK, new StringContent(""), StatusCode.Unavailable, retryPushbackHeader: header);
            });
            var serviceConfig = CreateServiceConfig();
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var call = invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions(), new HelloRequest { Name = "World" });

            // Assert
            Assert.AreEqual(1, callCount);
            var ex = await ExceptionAssert.ThrowsAsync<RpcException>(() => call.ResponseAsync).DefaultTimeout();
            Assert.AreEqual(StatusCode.Unavailable, ex.StatusCode);
            Assert.AreEqual(StatusCode.Unavailable, call.GetStatus().StatusCode);
        }

        [Test]
        public async Task AsyncUnaryCall_PushbackExpicitDelay_DelayForSpecifiedDuration()
        {
            // Arrange
            var callCount = 0;
            var httpClient = ClientTestHelpers.CreateTestClient(async request =>
            {
                callCount++;
                if (callCount == 1)
                {
                    await request.Content!.CopyToAsync(new MemoryStream());
                    return ResponseUtils.CreateResponse(HttpStatusCode.OK, new StringContent(""), StatusCode.Unavailable, retryPushbackHeader: "50");
                }

                var reply = new HelloReply { Message = "Hello world" };
                var streamContent = await ClientTestHelpers.CreateResponseContent(reply).DefaultTimeout();
                return ResponseUtils.CreateResponse(HttpStatusCode.OK, streamContent);
            });
            var serviceConfig = CreateServiceConfig(backoffMultiplier: 1);
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var delayTask = Task.Delay(50);
            var call = invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions(), new HelloRequest { Name = "World" });
            var completedTask = await Task.WhenAny(call.ResponseAsync, delayTask).DefaultTimeout();
            var rs = await call.ResponseAsync.DefaultTimeout();

            // Assert
            Assert.AreEqual(delayTask, completedTask); // Response task should finish after
            Assert.AreEqual(2, callCount);
            Assert.AreEqual("Hello world", rs.Message);
        }

        [Test]
        public async Task AsyncUnaryCall_PushbackExplicitDelayExceedAttempts_Failure()
        {
            // Arrange
            var callCount = 0;
            var httpClient = ClientTestHelpers.CreateTestClient(async request =>
            {
                callCount++;
                await request.Content!.CopyToAsync(new MemoryStream());
                return ResponseUtils.CreateHeadersOnlyResponse(HttpStatusCode.OK, StatusCode.Unavailable, retryPushbackHeader: "0");
            });
            var serviceConfig = CreateServiceConfig(maxAttempts: 5);
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var call = invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions(), new HelloRequest { Name = "World" });

            // Assert
            Assert.AreEqual(5, callCount);
            var ex = await ExceptionAssert.ThrowsAsync<RpcException>(() => call.ResponseAsync).DefaultTimeout();
            Assert.AreEqual(StatusCode.Unavailable, ex.StatusCode);
        }

        [Test]
        public async Task AsyncUnaryCall_UnsupportedStatusCode_Failure()
        {
            // Arrange
            var callCount = 0;
            var httpClient = ClientTestHelpers.CreateTestClient(async request =>
            {
                callCount++;
                await request.Content!.CopyToAsync(new MemoryStream());
                return ResponseUtils.CreateResponse(HttpStatusCode.OK, new StringContent(""), StatusCode.InvalidArgument);
            });
            var serviceConfig = CreateServiceConfig();
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var call = invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions(), new HelloRequest { Name = "World" });

            // Assert
            Assert.AreEqual(1, callCount);
            var ex = await ExceptionAssert.ThrowsAsync<RpcException>(() => call.ResponseAsync).DefaultTimeout();
            Assert.AreEqual(StatusCode.InvalidArgument, ex.StatusCode);
            Assert.AreEqual(StatusCode.InvalidArgument, call.GetStatus().StatusCode);
        }

        [Test]
        public async Task AsyncUnaryCall_Success_RequestContentSent()
        {
            // Arrange
            HttpContent? content = null;

            var callCount = 0;
            var httpClient = ClientTestHelpers.CreateTestClient(async request =>
            {
                callCount++;
                content = request.Content;

                var reply = new HelloReply { Message = "Hello world" };
                var streamContent = await ClientTestHelpers.CreateResponseContent(reply).DefaultTimeout();

                return ResponseUtils.CreateResponse(HttpStatusCode.OK, streamContent);
            });
            var serviceConfig = CreateServiceConfig();
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var rs = await invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions(), new HelloRequest { Name = "World" });

            // Assert
            Assert.AreEqual(1, callCount);
            Assert.AreEqual("Hello world", rs.Message);
        }

        [Test]
        public async Task AsyncClientStreamingCall_SuccessAfterRetry_RequestContentSent()
        {
            // Arrange
            PushStreamContent<HelloRequest, HelloReply>? content = null;

            var callCount = 0;
            var httpClient = ClientTestHelpers.CreateTestClient(async request =>
            {
                callCount++;

                content = (PushStreamContent<HelloRequest, HelloReply>)request.Content!;
                await content.PushComplete.DefaultTimeout();

                if (callCount == 1)
                {
                    await content.CopyToAsync(new MemoryStream());
                    return ResponseUtils.CreateHeadersOnlyResponse(HttpStatusCode.OK, StatusCode.Unavailable);
                }

                var reply = new HelloReply { Message = "Hello world" };
                var streamContent = await ClientTestHelpers.CreateResponseContent(reply).DefaultTimeout();

                return ResponseUtils.CreateResponse(HttpStatusCode.OK, streamContent);
            });
            var serviceConfig = CreateServiceConfig();
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var call = invoker.AsyncClientStreamingCall<HelloRequest, HelloReply>(ClientTestHelpers.GetServiceMethod(MethodType.ClientStreaming), string.Empty, new CallOptions());

            // Assert
            Assert.IsNotNull(call);
            Assert.IsNotNull(content);

            var responseTask = call.ResponseAsync;
            Assert.IsFalse(responseTask.IsCompleted, "Response not returned until client stream is complete.");

            var streamTask = content!.ReadAsStreamAsync().DefaultTimeout();

            await call.RequestStream.WriteAsync(new HelloRequest { Name = "1" }).DefaultTimeout();
            await call.RequestStream.WriteAsync(new HelloRequest { Name = "2" }).DefaultTimeout();

            await call.RequestStream.CompleteAsync().DefaultTimeout();

            var responseMessage = await responseTask.DefaultTimeout();
            Assert.AreEqual("Hello world", responseMessage.Message);

            var requestContent = await streamTask.DefaultTimeout();
            var requestMessage = await ReadRequestMessage(requestContent).DefaultTimeout();
            Assert.AreEqual("1", requestMessage!.Name);
            requestMessage = await ReadRequestMessage(requestContent).DefaultTimeout();
            Assert.AreEqual("2", requestMessage!.Name);
        }

        [Test]
        public async Task AsyncClientStreamingCall_OneMessageSentThenRetryThenAnotherMessage_RequestContentSent()
        {
            // Arrange
            PushStreamContent<HelloRequest, HelloReply>? content = null;
            var syncPoint = new SyncPoint(runContinuationsAsynchronously: true);

            var callCount = 0;
            var httpClient = ClientTestHelpers.CreateTestClient(async request =>
            {
                callCount++;
                content = (PushStreamContent<HelloRequest, HelloReply>)request.Content!;

                if (callCount == 1)
                {
                    _ = content.CopyToAsync(new MemoryStream());

                    await syncPoint.WaitForSyncPoint();

                    return ResponseUtils.CreateHeadersOnlyResponse(HttpStatusCode.OK, StatusCode.Unavailable);
                }

                syncPoint.Continue();

                await content.PushComplete.DefaultTimeout();

                var reply = new HelloReply { Message = "Hello world" };
                var streamContent = await ClientTestHelpers.CreateResponseContent(reply).DefaultTimeout();

                return ResponseUtils.CreateResponse(HttpStatusCode.OK, streamContent);
            });
            var serviceConfig = CreateServiceConfig();
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var call = invoker.AsyncClientStreamingCall<HelloRequest, HelloReply>(ClientTestHelpers.GetServiceMethod(MethodType.ClientStreaming), string.Empty, new CallOptions());

            // Assert
            Assert.IsNotNull(call);
            Assert.IsNotNull(content);

            var responseTask = call.ResponseAsync;
            Assert.IsFalse(responseTask.IsCompleted, "Response not returned until client stream is complete.");

            await call.RequestStream.WriteAsync(new HelloRequest { Name = "1" }).DefaultTimeout();

            // Wait until the first call has failed and the second is on the server
            await syncPoint.WaitToContinue().DefaultTimeout();

            await call.RequestStream.WriteAsync(new HelloRequest { Name = "2" }).DefaultTimeout();

            await call.RequestStream.CompleteAsync().DefaultTimeout();

            var streamTask = content!.ReadAsStreamAsync().DefaultTimeout();

            var responseMessage = await responseTask.DefaultTimeout();
            Assert.AreEqual("Hello world", responseMessage.Message);

            var requestContent = await streamTask.DefaultTimeout();
            var requestMessage = await ReadRequestMessage(requestContent).DefaultTimeout();
            Assert.AreEqual("1", requestMessage!.Name);
            requestMessage = await ReadRequestMessage(requestContent).DefaultTimeout();
            Assert.AreEqual("2", requestMessage!.Name);
        }

        [Test]
        public async Task AsyncServerStreamingCall_SuccessAfterRetry_RequestContentSent()
        {
            // Arrange
            HttpContent? content = null;
            SyncPoint syncPoint = new SyncPoint(runContinuationsAsynchronously: true);

            var callCount = 0;
            var httpClient = ClientTestHelpers.CreateTestClient(async request =>
            {
                callCount++;
                content = request.Content!;

                if (callCount == 1)
                {
                    await syncPoint.WaitForSyncPoint();

                    await content.CopyToAsync(new MemoryStream());
                    return ResponseUtils.CreateHeadersOnlyResponse(HttpStatusCode.OK, StatusCode.Unavailable);
                }

                syncPoint.Continue();

                var reply = new HelloReply { Message = "Hello world" };
                var streamContent = await ClientTestHelpers.CreateResponseContent(reply).DefaultTimeout();

                return ResponseUtils.CreateResponse(HttpStatusCode.OK, streamContent);
            });
            var serviceConfig = CreateServiceConfig();
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var call = invoker.AsyncServerStreamingCall<HelloRequest, HelloReply>(ClientTestHelpers.GetServiceMethod(MethodType.ServerStreaming), string.Empty, new CallOptions(), new HelloRequest { Name = "World" });
            var moveNextTask = call.ResponseStream.MoveNext(CancellationToken.None);

            // Wait until the first call has failed and the second is on the server
            await syncPoint.WaitToContinue().DefaultTimeout();

            // Assert
            Assert.IsTrue(await moveNextTask);
            Assert.AreEqual("Hello world", call.ResponseStream.Current.Message);

            Assert.IsNotNull(content);

            var requestContent = await content!.ReadAsStreamAsync().DefaultTimeout();
            var requestMessage = await ReadRequestMessage(requestContent).DefaultTimeout();

            Assert.AreEqual("World", requestMessage!.Name);
        }

        [Test]
        public async Task AsyncDuplexStreamingCall_SuccessAfterRetry_RequestContentSent()
        {
            // Arrange
            PushStreamContent<HelloRequest, HelloReply>? content = null;
            SyncPoint syncPoint = new SyncPoint(runContinuationsAsynchronously: true);

            var callCount = 0;
            var httpClient = ClientTestHelpers.CreateTestClient(async request =>
            {
                callCount++;
                content = (PushStreamContent<HelloRequest, HelloReply>)request.Content!;

                if (callCount == 1)
                {
                    _ = content.CopyToAsync(new MemoryStream());

                    await syncPoint.WaitForSyncPoint();

                    return ResponseUtils.CreateHeadersOnlyResponse(HttpStatusCode.OK, StatusCode.Unavailable);
                }

                syncPoint.Continue();

                await content.PushComplete.DefaultTimeout();

                var reply = new HelloReply { Message = "Hello world" };
                var streamContent = await ClientTestHelpers.CreateResponseContent(reply).DefaultTimeout();

                return ResponseUtils.CreateResponse(HttpStatusCode.OK, streamContent);
            });
            var serviceConfig = CreateServiceConfig();
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var call = invoker.AsyncDuplexStreamingCall<HelloRequest, HelloReply>(ClientTestHelpers.GetServiceMethod(MethodType.DuplexStreaming), string.Empty, new CallOptions());
            var moveNextTask = call.ResponseStream.MoveNext(CancellationToken.None);

            await call.RequestStream.WriteAsync(new HelloRequest { Name = "1" }).DefaultTimeout();

            // Wait until the first call has failed and the second is on the server
            await syncPoint.WaitToContinue().DefaultTimeout();

            await call.RequestStream.WriteAsync(new HelloRequest { Name = "2" }).DefaultTimeout();

            await call.RequestStream.CompleteAsync().DefaultTimeout();

            // Assert
            Assert.IsTrue(await moveNextTask.DefaultTimeout());
            Assert.AreEqual("Hello world", call.ResponseStream.Current.Message);

            Assert.IsNotNull(content);

            var streamTask = content!.ReadAsStreamAsync().DefaultTimeout();
            var requestContent = await streamTask.DefaultTimeout();
            var requestMessage = await ReadRequestMessage(requestContent).DefaultTimeout();
            Assert.AreEqual("1", requestMessage!.Name);
            requestMessage = await ReadRequestMessage(requestContent).DefaultTimeout();
            Assert.AreEqual("2", requestMessage!.Name);
        }

        private static Task<HelloRequest?> ReadRequestMessage(Stream requestContent)
        {
            return StreamSerializationHelper.ReadMessageAsync(
                requestContent,
                ClientTestHelpers.ServiceMethod.RequestMarshaller.ContextualDeserializer,
                GrpcProtocolConstants.IdentityGrpcEncoding,
                maximumMessageSize: null,
                GrpcProtocolConstants.DefaultCompressionProviders,
                singleMessage: false,
                CancellationToken.None).AsTask();
        }

        private static ServiceConfig CreateServiceConfig(int? maxAttempts = null, double? backoffMultiplier = null)
        {
            return new ServiceConfig
            {
                MethodConfigs =
                {
                    new MethodConfig
                    {
                        Names = { Name.AllServices },
                        RetryPolicy = new RetryThrottlingPolicy
                        {
                            MaxAttempts = maxAttempts ?? 5,
                            InitialBackoff = TimeSpan.Zero,
                            MaxBackoff = TimeSpan.Zero,
                            BackoffMultiplier = backoffMultiplier ?? 1.1,
                            RetryableStatusCodes = { StatusCode.Unavailable }
                        }
                    }
                }
            };
        }
    }
}
