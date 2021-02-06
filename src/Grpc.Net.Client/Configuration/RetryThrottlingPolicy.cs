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

using System.Collections.Generic;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Grpc.Net.Client.Configuration
{
    //"retryThrottling": {
    //  "maxTokens": 10,
    //  "tokenRatio": 0.1
    //}

    public sealed class RetryThrottlingPolicy : ConfigObject
    {
        internal const string MaxTokensPropertyName = "maxTokens";
        internal const string TokenRatioPropertyName = "tokenRatio";

        public int? MaxTokens
        {
            get => GetValue<int>(MaxTokensPropertyName);
            set => SetValue(MaxTokensPropertyName, value);
        }

        public double? TokenRatio
        {
            get => GetValue<double>(TokenRatioPropertyName);
            set => SetValue(TokenRatioPropertyName, value);
        }

        public RetryThrottlingPolicy() { }

        internal RetryThrottlingPolicy(IDictionary<string, object> inner) : base(inner) { }
    }
}
