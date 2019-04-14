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
using System.Net.Http.Headers;
using Grpc.Core;

namespace Grpc.NetCore.HttpClient.Internal
{
    internal static class GrpcProtocolHelpers
    {
        public static bool IsGrpcContentType(string contentType)
        {
            if (contentType == null)
            {
                return false;
            }

            if (!contentType.StartsWith(GrpcProtocolConstants.GrpcContentType, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            if (contentType.Length == GrpcProtocolConstants.GrpcContentType.Length)
            {
                // Exact match
                return true;
            }

            // Support variations on the content-type (e.g. +proto, +json)
            char nextChar = contentType[GrpcProtocolConstants.GrpcContentType.Length];
            if (nextChar == ';')
            {
                return true;
            }
            if (nextChar == '+')
            {
                // Accept any message format. Marshaller could be set to support third-party formats
                return true;
            }

            return false;
        }

        public static byte[] ParseBinaryHeader(string base64)
        {
            string decodable;
            switch (base64.Length % 4)
            {
                case 0:
                    // base64 has the required padding 
                    decodable = base64;
                    break;
                case 2:
                    // 2 chars padding
                    decodable = base64 + "==";
                    break;
                case 3:
                    // 3 chars padding
                    decodable = base64 + "=";
                    break;
                default:
                    // length%4 == 1 should be illegal
                    throw new FormatException("Invalid base64 header value");
            }

            return Convert.FromBase64String(decodable);
        }

        public static Metadata BuildMetadata(HttpResponseHeaders responseHeaders)
        {
            var headers = new Metadata();

            foreach (var header in responseHeaders)
            {
                // ASP.NET Core includes pseudo headers in the set of request headers
                // whereas, they are not in gRPC implementations. We will filter them
                // out when we construct the list of headers on the context.
                if (header.Key.StartsWith(":", StringComparison.Ordinal))
                {
                    continue;
                }
                else if (header.Key.EndsWith(Metadata.BinaryHeaderSuffix, StringComparison.OrdinalIgnoreCase))
                {
                    headers.Add(header.Key, GrpcProtocolHelpers.ParseBinaryHeader(string.Join(",", header.Value)));
                }
                else
                {
                    headers.Add(header.Key, string.Join(",", header.Value));
                }
            }

            return headers;
        }
    }
}