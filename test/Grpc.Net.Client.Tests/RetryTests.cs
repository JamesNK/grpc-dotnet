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
                content = request.Content!;

                callCount++;
                if (callCount == 1)
                {
                    await content.CopyToAsync(new MemoryStream());
                    return ResponseUtils.CreateResponse(HttpStatusCode.OK, new StringContent(""), StatusCode.Unavailable);
                }

                HelloReply reply = new HelloReply
                {
                    Message = "Hello world"
                };

                var streamContent = await ClientTestHelpers.CreateResponseContent(reply).DefaultTimeout();

                return ResponseUtils.CreateResponse(HttpStatusCode.OK, streamContent);
            });
            var serviceConfig = CreateServiceConfig();
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var rs = await invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions(), new HelloRequest { Name = "World" });

            // Assert
            Assert.AreEqual(2, callCount);
            Assert.AreEqual("Hello world", rs.Message);

            Assert.IsNotNull(content);

            var requestContent = await content!.ReadAsStreamAsync().DefaultTimeout();
            var requestMessage = await StreamSerializationHelper.ReadMessageAsync(
                requestContent,
                ClientTestHelpers.ServiceMethod.RequestMarshaller.ContextualDeserializer,
                GrpcProtocolConstants.IdentityGrpcEncoding,
                maximumMessageSize: null,
                GrpcProtocolConstants.DefaultCompressionProviders,
                singleMessage: true,
                CancellationToken.None).AsTask().DefaultTimeout();

            Assert.AreEqual("World", requestMessage!.Name);
        }

        private static ServiceConfig CreateServiceConfig()
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
                            MaxAttempts = 5,
                            InitialBackoff = TimeSpan.Zero,
                            RetryableStatusCodes = { StatusCode.Unavailable }
                        }
                    }
                }
            };
        }

        [Test]
        public async Task AsyncUnaryCall_Success_RequestContentSent()
        {
            // Arrange
            HttpContent? content = null;

            var httpClient = ClientTestHelpers.CreateTestClient(async request =>
            {
                content = request.Content;

                HelloReply reply = new HelloReply
                {
                    Message = "Hello world"
                };

                var streamContent = await ClientTestHelpers.CreateResponseContent(reply).DefaultTimeout();

                return ResponseUtils.CreateResponse(HttpStatusCode.OK, streamContent);
            });
            var serviceConfig = CreateServiceConfig();
            var invoker = HttpClientCallInvokerFactory.Create(httpClient, serviceConfig: serviceConfig);

            // Act
            var rs = await invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions(), new HelloRequest { Name = "World" });

            // Assert
            Assert.AreEqual("Hello world", rs.Message);
        }

        [Test]
        public async Task AsyncClientStreamingCall_Success_RequestContentSent()
        {
            // Arrange
            PushStreamContent<HelloRequest, HelloReply>? content = null;

            var callCount = 0;
            var httpClient = ClientTestHelpers.CreateTestClient(async request =>
            {
                content = (PushStreamContent<HelloRequest, HelloReply>)request.Content!;
                await content.PushComplete.DefaultTimeout();
                
                callCount++;
                if (callCount == 1)
                {
                    await content.CopyToAsync(new MemoryStream());
                    return ResponseUtils.CreateResponse(HttpStatusCode.OK, new StringContent(""), StatusCode.Unavailable);
                }

                HelloReply reply = new HelloReply
                {
                    Message = "Hello world"
                };

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
            var requestMessage = await StreamSerializationHelper.ReadMessageAsync(
                requestContent,
                ClientTestHelpers.ServiceMethod.RequestMarshaller.ContextualDeserializer,
                GrpcProtocolConstants.IdentityGrpcEncoding,
                maximumMessageSize: null,
                GrpcProtocolConstants.DefaultCompressionProviders,
                singleMessage: false,
                CancellationToken.None).AsTask().DefaultTimeout();
            Assert.AreEqual("1", requestMessage!.Name);
            requestMessage = await StreamSerializationHelper.ReadMessageAsync(
                requestContent,
                ClientTestHelpers.ServiceMethod.RequestMarshaller.ContextualDeserializer,
                GrpcProtocolConstants.IdentityGrpcEncoding,
                maximumMessageSize: null,
                GrpcProtocolConstants.DefaultCompressionProviders,
                singleMessage: false,
                CancellationToken.None).AsTask().DefaultTimeout();
            Assert.AreEqual("2", requestMessage!.Name);
        }
    }
}
