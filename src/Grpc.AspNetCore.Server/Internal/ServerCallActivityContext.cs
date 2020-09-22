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
using System.Diagnostics;
using Grpc.Core;

namespace Grpc.AspNetCore.Server.Internal
{
    internal class ServerCallActivityContext
    {
        private readonly Activity _activity;
#if NET5_0
        private int _sentId;
        private int _receivedId;
#endif

        public ServerCallActivityContext(Activity activity)
        {
            _activity = activity;
        }

        public void SetMethod(string method)
        {
            _activity.AddTag(GrpcServerConstants.ActivityMethodTag, method);
        }

        public void SetStatus(StatusCode statusCode)
        {
            _activity.AddTag(GrpcServerConstants.ActivityStatusCodeTag, statusCode.ToTrailerString());
        }

        public void AddReceivedMessage(int compressedSize, int uncompressedSize)
        {
#if NET5_0
            var tags = new ActivityTagsCollection();
            tags.Add("name", "message");
            tags.Add("message.type", "RECEIVED");
            tags.Add("message.id", ++_receivedId);
            tags.Add("message.compressed_size", compressedSize);
            tags.Add("message.uncompressed_size", uncompressedSize);

            _activity.AddEvent(new ActivityEvent(name: "Message Received " + _receivedId, DateTimeOffset.UtcNow, tags));
#endif
        }

        public void AddSentMessage(int compressedSize, int uncompressedSize)
        {
#if NET5_0
            var tags = new ActivityTagsCollection();
            tags.Add("name", "message");
            tags.Add("message.type", "SENT");
            tags.Add("message.id", ++_sentId);
            tags.Add("message.compressed_size", compressedSize);
            tags.Add("message.uncompressed_size", uncompressedSize);

            _activity.AddEvent(new ActivityEvent(name: "Message Sent " + _sentId, DateTimeOffset.UtcNow, tags));
#endif
        }
    }
}
