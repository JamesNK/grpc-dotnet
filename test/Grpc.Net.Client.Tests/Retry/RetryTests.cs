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
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Greet;
using Grpc.Core;
using Grpc.Net.Client.Configuration;
using Grpc.Net.Client.Internal;
using Grpc.Net.Client.Tests.Infrastructure;
using Grpc.Tests.Shared;
using NUnit.Framework;

namespace Grpc.Net.Client.Tests.Retry
{
    [TestFixture]
    public class RetryTests
    {
        [Test]
        public async Task AsyncUnaryCall_SuccessAfterRetry_RequestContentSent()
        {
            // Arrange
            HttpContent? content = null;

            bool? firstRequestPreviousAttemptsHeader = null;
            string? secondRequestPreviousAttemptsHeaderValue = null;

            var callCount = 0;
            var httpClient = ClientTestHelpers.CreateTestClient(async request =>
            {
                callCount++;
                content = request.Content!;

                if (callCount == 1)
                {
                    firstRequestPreviousAttemptsHeader = request.Headers.TryGetValues(GrpcProtocolConstants.RetryPreviousAttemptsHeader, out _);

                    await content.CopyToAsync(new MemoryStream());
                    return ResponseUtils.CreateHeadersOnlyResponse(HttpStatusCode.OK, StatusCode.Unavailable);
                }

                if (request.Headers.TryGetValues(GrpcProtocolConstants.RetryPreviousAttemptsHeader, out var retryAttemptCountValue))
                {
                    secondRequestPreviousAttemptsHeaderValue = retryAttemptCountValue.Single();
                }

                var reply = new HelloReply { Message = "Hello world" };
                var streamContent = await ClientTestHelpers.CreateResponseContent(reply).DefaultTimeout();

                return ResponseUtils.CreateResponse(HttpStatusCode.OK, streamContent, customTrailers: new Dictionary<string, string>
                {
                    ["custom-trailer"] = "Value!"
                });
            });
            var serviceConfig = ServiceConfigHelpers.CreateRetryServiceConfig();
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var call = invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions(), new HelloRequest { Name = "World" });

            // Assert
            Assert.AreEqual(2, callCount);
            Assert.AreEqual("Hello world", (await call.ResponseAsync.DefaultTimeout()).Message);
            Assert.AreEqual("1", (await call.ResponseHeadersAsync.DefaultTimeout()).GetValue(GrpcProtocolConstants.RetryPreviousAttemptsHeader));

            Assert.IsNotNull(content);

            var requestContent = await content!.ReadAsStreamAsync().DefaultTimeout();
            var requestMessage = await ReadRequestMessage(requestContent).DefaultTimeout();

            Assert.AreEqual("World", requestMessage!.Name);

            Assert.IsFalse(firstRequestPreviousAttemptsHeader);
            Assert.AreEqual("1", secondRequestPreviousAttemptsHeaderValue);

            var trailers = call.GetTrailers();
            Assert.AreEqual("Value!", trailers.GetValue("custom-trailer"));
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
            var serviceConfig = ServiceConfigHelpers.CreateRetryServiceConfig();
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
            var serviceConfig = ServiceConfigHelpers.CreateRetryServiceConfig(maxAttempts: 3);
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var call = invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions(), new HelloRequest { Name = "World" });

            // Assert
            Assert.AreEqual(3, callCount);
            var ex = await ExceptionAssert.ThrowsAsync<RpcException>(() => call.ResponseAsync).DefaultTimeout();
            Assert.AreEqual(StatusCode.Unavailable, ex.StatusCode);
            Assert.AreEqual(StatusCode.Unavailable, call.GetStatus().StatusCode);
        }

        [Test]
        public async Task AsyncUnaryCall_FailureWithLongDelay_Dispose_CallImmediatelyDisposed()
        {
            // Arrange
            var callCount = 0;
            var httpClient = ClientTestHelpers.CreateTestClient(async request =>
            {
                callCount++;
                await request.Content!.CopyToAsync(new MemoryStream());
                return ResponseUtils.CreateHeadersOnlyResponse(HttpStatusCode.OK, StatusCode.Unavailable);
            });
            // Very long delay
            var serviceConfig = ServiceConfigHelpers.CreateRetryServiceConfig(initialBackoff: TimeSpan.FromSeconds(30), maxBackoff: TimeSpan.FromSeconds(30));
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var call = invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions(), new HelloRequest { Name = "World" });
            var resultTask = call.ResponseAsync;

            // Test will timeout if dispose doesn't kill the timer.
            call.Dispose();

            // Assert
            var ex = await ExceptionAssert.ThrowsAsync<RpcException>(() => resultTask).DefaultTimeout();
            Assert.AreEqual(StatusCode.Unavailable, ex.StatusCode);
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
            var serviceConfig = ServiceConfigHelpers.CreateRetryServiceConfig();
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
                    return ResponseUtils.CreateHeadersOnlyResponse(HttpStatusCode.OK, StatusCode.Unavailable, retryPushbackHeader: "50");
                }

                var reply = new HelloReply { Message = "Hello world" };
                var streamContent = await ClientTestHelpers.CreateResponseContent(reply).DefaultTimeout();
                return ResponseUtils.CreateResponse(HttpStatusCode.OK, streamContent);
            });
            var serviceConfig = ServiceConfigHelpers.CreateRetryServiceConfig(backoffMultiplier: 1);
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
            var serviceConfig = ServiceConfigHelpers.CreateRetryServiceConfig(maxAttempts: 5);
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
            var serviceConfig = ServiceConfigHelpers.CreateRetryServiceConfig();
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
            var serviceConfig = ServiceConfigHelpers.CreateRetryServiceConfig();
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var call = invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions(), new HelloRequest { Name = "World" });

            // Assert
            Assert.AreEqual(1, callCount);
            Assert.AreEqual("Hello world", (await call.ResponseAsync.DefaultTimeout()).Message);
        }

        [Test]
        public async Task AsyncClientStreamingCall_SuccessAfterRetry_RequestContentSent()
        {
            // Arrange
            var requestContent = new MemoryStream();

            var callCount = 0;
            var httpClient = ClientTestHelpers.CreateTestClient(async request =>
            {
                callCount++;

                await request.Content!.CopyToAsync(requestContent);

                if (callCount == 1)
                {
                    return ResponseUtils.CreateHeadersOnlyResponse(HttpStatusCode.OK, StatusCode.Unavailable);
                }

                var reply = new HelloReply { Message = "Hello world" };
                var streamContent = await ClientTestHelpers.CreateResponseContent(reply).DefaultTimeout();

                return ResponseUtils.CreateResponse(HttpStatusCode.OK, streamContent);
            });
            var serviceConfig = ServiceConfigHelpers.CreateRetryServiceConfig();
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var call = invoker.AsyncClientStreamingCall<HelloRequest, HelloReply>(ClientTestHelpers.GetServiceMethod(MethodType.ClientStreaming), string.Empty, new CallOptions());

            // Assert
            Assert.IsNotNull(call);

            var responseTask = call.ResponseAsync;
            Assert.IsFalse(responseTask.IsCompleted, "Response not returned until client stream is complete.");


            await call.RequestStream.WriteAsync(new HelloRequest { Name = "1" }).DefaultTimeout();
            await call.RequestStream.WriteAsync(new HelloRequest { Name = "2" }).DefaultTimeout();

            await call.RequestStream.CompleteAsync().DefaultTimeout();

            var responseMessage = await responseTask.DefaultTimeout();
            Assert.AreEqual("Hello world", responseMessage.Message);

            requestContent.Seek(0, SeekOrigin.Begin);

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
            var serviceConfig = ServiceConfigHelpers.CreateRetryServiceConfig();
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
            var syncPoint = new SyncPoint(runContinuationsAsynchronously: true);

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
            var serviceConfig = ServiceConfigHelpers.CreateRetryServiceConfig();
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
        public async Task AsyncServerStreamingCall_FailureAfterReadingResponseMessage_Failure()
        {
            // Arrange
            var streamContent = new SyncPointMemoryStream();

            var callCount = 0;
            var httpClient = ClientTestHelpers.CreateTestClient(request =>
            {
                callCount++;
                return Task.FromResult(ResponseUtils.CreateResponse(HttpStatusCode.OK, new StreamContent(streamContent)));
            });
            var serviceConfig = ServiceConfigHelpers.CreateRetryServiceConfig();
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var call = invoker.AsyncServerStreamingCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions(), new HelloRequest());

            var responseStream = call.ResponseStream;

            // Assert
            Assert.IsNull(responseStream.Current);

            var moveNextTask1 = responseStream.MoveNext(CancellationToken.None);
            Assert.IsFalse(moveNextTask1.IsCompleted);

            await streamContent.AddDataAndWait(await ClientTestHelpers.GetResponseDataAsync(new HelloReply
            {
                Message = "Hello world 1"
            }).DefaultTimeout()).DefaultTimeout();

            Assert.IsTrue(await moveNextTask1.DefaultTimeout());
            Assert.IsNotNull(responseStream.Current);
            Assert.AreEqual("Hello world 1", responseStream.Current.Message);

            var moveNextTask2 = responseStream.MoveNext(CancellationToken.None);
            Assert.IsFalse(moveNextTask2.IsCompleted);

            await streamContent.AddExceptionAndWait(new Exception("Exception!")).DefaultTimeout();

            var ex = await ExceptionAssert.ThrowsAsync<RpcException>(() => moveNextTask2).DefaultTimeout();
            Assert.AreEqual(StatusCode.Internal, ex.StatusCode);
            Assert.AreEqual(StatusCode.Internal, call.GetStatus().StatusCode);
            Assert.AreEqual("Error reading next message. Exception: Exception!", call.GetStatus().Detail);
        }

        [Test]
        public async Task AsyncDuplexStreamingCall_SuccessAfterRetry_RequestContentSent()
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
            var serviceConfig = ServiceConfigHelpers.CreateRetryServiceConfig();
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
    }
}
