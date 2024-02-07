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

using System.Net;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client.Tests.Infrastructure;
using Grpc.Tests.Shared;
using Microsoft.Extensions.Logging.Testing;
using NUnit.Framework;
using Recursion;

namespace Grpc.Net.Client.Tests;

[TestFixture]
public class RecursionLimitTests
{
    public static readonly Marshaller<RecursionRequest> HelloRequestMarshaller = Marshallers.Create<RecursionRequest>(
        r => r.ToByteArray(),
        data => RecursionRequest.Parser.ParseFrom(CodedInputStream.CreateWithLimits(new MemoryStream(data), int.MaxValue, int.MaxValue)));
    public static readonly Marshaller<RecursionResponse> HelloReplyMarshaller = Marshallers.Create<RecursionResponse>(
        r => r.ToByteArray(),
        data => RecursionResponse.Parser.ParseFrom(CodedInputStream.CreateWithLimits(new MemoryStream(data), int.MaxValue, int.MaxValue)));

    public static readonly Method<RecursionRequest, RecursionResponse> ServiceMethod = new Method<RecursionRequest, RecursionResponse>(MethodType.Unary, "ServiceName", "MethodName", HelloRequestMarshaller, HelloReplyMarshaller);

    [Test]
    public async Task AsyncUnaryCall_DeepRecursion_Success()
    {
        // Arrange
        var httpClient = ClientTestHelpers.CreateTestClient(async request =>
        {
            var data = await request.Content!.ReadAsByteArrayAsync().DefaultTimeout();

            var streamContent = await ClientTestHelpers.CreateResponseContent(new RecursionResponse
            {
                Message = CreateRecursiveMessages(150)
            }).DefaultTimeout();

            return ResponseUtils.CreateResponse(HttpStatusCode.OK, streamContent);
        });

        var testSink = new TestSink();
        var loggerFactory = new TestLoggerFactory(testSink, true);

        var invoker = HttpClientCallInvokerFactory.Create(httpClient, loggerFactory);

        // Act
        var rs = await invoker.AsyncUnaryCall<RecursionRequest, RecursionResponse>(ServiceMethod, string.Empty, new CallOptions(), new RecursionRequest
        {
            Message = CreateRecursiveMessages(150)
        });

        // Assert
        Assert.AreEqual(150, GetMaxDepth(rs.Message));
    }

    private static int GetMaxDepth(RecursionMessage message)
    {
        var depth = 0;
        var current = message;

        while (current != null)
        {
            depth++;
            current = current.Child;
        }

        return depth;
    }

    private static RecursionMessage CreateRecursiveMessages(int depth)
    {
        var root = new RecursionMessage();
        root.Depth = 0;
        var current = root;

        for (var i = 0; i < depth - 1; i++)
        {
            current.Child = new RecursionMessage();
            current.Child.Depth = i + 1;

            current = current.Child;
        }

        return root;
    }
}
