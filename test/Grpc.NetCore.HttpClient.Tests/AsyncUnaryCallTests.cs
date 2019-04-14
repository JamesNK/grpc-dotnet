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
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Greet;
using Grpc.Core;
using Grpc.NetCore.HttpClient.Tests.Infrastructure;
using Grpc.Tests;
using NUnit.Framework;

namespace Grpc.NetCore.HttpClient.Tests
{
    [TestFixture]
    public class AsyncUnaryCallTests
    {
        [Test]
        public async Task AsyncUnaryCall_Success_HttpRequestMessagePopulated()
        {
            // Arrange
            HttpRequestMessage httpRequestMessage = null;

            var httpClient = TestHelpers.CreateTestClient(async request =>
            {
                httpRequestMessage = request;

                HelloReply reply = new HelloReply
                {
                    Message = "Hello world"
                };

                var streamContent = await TestHelpers.CreateResponseContent(reply).DefaultTimeout();

                return ResponseUtils.CreateResponse(HttpStatusCode.OK, streamContent);
            });
            var invoker = new HttpClientCallInvoker(httpClient);

            // Act
            var rs = await invoker.AsyncUnaryCall<HelloRequest, HelloReply>(TestHelpers.ServiceMethod, null, new CallOptions(), new HelloRequest());

            // Assert
            Assert.AreEqual("Hello world", rs.Message);

            Assert.IsNotNull(httpRequestMessage);
            Assert.AreEqual(new Version(2, 0), httpRequestMessage.Version);
            Assert.AreEqual(HttpMethod.Post, httpRequestMessage.Method);
            Assert.AreEqual(new Uri("https://localhost/ServiceName/MethodName"), httpRequestMessage.RequestUri);
            Assert.AreEqual(new MediaTypeHeaderValue("application/grpc"), httpRequestMessage.Content.Headers.ContentType);
        }

        [Test]
        public async Task AsyncUnaryCall_Success_RequestContentSent()
        {
            // Arrange
            HttpContent content = null;

            var httpClient = TestHelpers.CreateTestClient(async request =>
            {
                content = request.Content;

                HelloReply reply = new HelloReply
                {
                    Message = "Hello world"
                };

                var streamContent = await TestHelpers.CreateResponseContent(reply).DefaultTimeout();

                return ResponseUtils.CreateResponse(HttpStatusCode.OK, streamContent);
            });
            var invoker = new HttpClientCallInvoker(httpClient);

            // Act
            var rs = await invoker.AsyncUnaryCall<HelloRequest, HelloReply>(TestHelpers.ServiceMethod, null, new CallOptions(), new HelloRequest { Name = "World" });

            // Assert
            Assert.AreEqual("Hello world", rs.Message);

            Assert.IsNotNull(content);

            var requestContent = await content.ReadAsStreamAsync().DefaultTimeout();
            var requestMessage = await requestContent.ReadSingleMessageAsync(TestHelpers.ServiceMethod.RequestMarshaller.Deserializer, CancellationToken.None).DefaultTimeout();

            Assert.AreEqual("World", requestMessage.Name);
        }
    }
}