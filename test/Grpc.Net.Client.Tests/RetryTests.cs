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
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Greet;
using Grpc.Core;
using Grpc.Net.Client.Internal;
using Grpc.Net.Client.Tests.Infrastructure;
using Grpc.Shared;
using Grpc.Tests.Shared;
using Microsoft.Extensions.Logging.Abstractions;
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

        //        [Test]
        //        public async Task AsyncUnaryCall_NonOkStatusTrailer_ThrowRpcError()
        //        {
        //            // Arrange
        //            var httpClient = ClientTestHelpers.CreateTestClient(request =>
        //            {
        //                var response = ResponseUtils.CreateResponse(HttpStatusCode.OK, new ByteArrayContent(Array.Empty<byte>()), StatusCode.Unimplemented);
        //                return Task.FromResult(response);
        //            });
        //            var invoker = HttpClientCallInvokerFactory.Create(httpClient);

        //            // Act
        //            var ex = await ExceptionAssert.ThrowsAsync<RpcException>(() => invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions(), new HelloRequest()).ResponseAsync).DefaultTimeout();

        //            // Assert
        //            Assert.AreEqual(StatusCode.Unimplemented, ex.StatusCode);
        //        }

        //        [Test]
        //        public async Task AsyncUnaryCall_SuccessTrailersOnly_ThrowNoMessageError()
        //        {
        //            // Arrange
        //            HttpResponseMessage? responseMessage = null;
        //            var httpClient = ClientTestHelpers.CreateTestClient(request =>
        //            {
        //                responseMessage = ResponseUtils.CreateResponse(HttpStatusCode.OK, new ByteArrayContent(Array.Empty<byte>()), grpcStatusCode: null);
        //                responseMessage.Headers.Add(GrpcProtocolConstants.StatusTrailer, StatusCode.OK.ToString("D"));
        //                responseMessage.Headers.Add(GrpcProtocolConstants.MessageTrailer, "Detail!");
        //                return Task.FromResult(responseMessage);
        //            });
        //            var invoker = HttpClientCallInvokerFactory.Create(httpClient);

        //            // Act
        //            var call = invoker.AsyncUnaryCall<HelloRequest, HelloReply>(ClientTestHelpers.ServiceMethod, string.Empty, new CallOptions(), new HelloRequest());
        //            var headers = await call.ResponseHeadersAsync.DefaultTimeout();
        //            var ex = await ExceptionAssert.ThrowsAsync<RpcException>(() => call.ResponseAsync).DefaultTimeout();

        //            // Assert
        //            Assert.NotNull(responseMessage);
        //            Assert.IsFalse(responseMessage!.TrailingHeaders.Any()); // sanity check that there are no trailers

        //            Assert.AreEqual(StatusCode.Internal, ex.Status.StatusCode);
        //            Assert.AreEqual("Failed to deserialize response message.", ex.Status.Detail);

        //            Assert.AreEqual(StatusCode.Internal, call.GetStatus().StatusCode);
        //            Assert.AreEqual("Failed to deserialize response message.", call.GetStatus().Detail);

        //            Assert.AreEqual(0, headers.Count);
        //            Assert.AreEqual(0, call.GetTrailers().Count);
        //        }
    }
}
