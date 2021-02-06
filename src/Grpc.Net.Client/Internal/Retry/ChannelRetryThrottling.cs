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
using System.Threading;
using Grpc.Net.Client.Configuration;

namespace Grpc.Net.Client.Internal.Retry
{
    internal class ChannelRetryThrottling
    {
        private readonly object _lock = new object();
        private readonly double _tokenRatio;
        private readonly int _maxTokens;
        private double _tokenCount;
        private double _tokenThreshold;

        public ChannelRetryThrottling(RetryThrottlingPolicy retryThrottling)
        {
            if (retryThrottling.MaxTokens == null)
            {
                throw CreateException(RetryThrottlingPolicy.MaxTokensPropertyName);
            }
            if (retryThrottling.TokenRatio == null)
            {
                throw CreateException(RetryThrottlingPolicy.TokenRatioPropertyName);
            }

            // Truncate token ratio to 3 decimal places
            _tokenRatio = Math.Truncate(retryThrottling.TokenRatio.GetValueOrDefault() * 1000) / 1000;

            _maxTokens = retryThrottling.MaxTokens.GetValueOrDefault();
            _tokenCount = retryThrottling.MaxTokens.GetValueOrDefault();
            _tokenThreshold = _tokenCount / 2;

            static InvalidOperationException CreateException(string propertyName)
            {
                return new InvalidOperationException($"Retry throttling missing required property '{propertyName}'.");
            }
        }

        public bool IsRetryThrottlingActive()
        {
            return Volatile.Read(ref _tokenCount) <= _tokenThreshold;
        }

        public void CallSuccess()
        {
            lock (_lock)
            {
                _tokenCount = Math.Min(_tokenCount + _tokenRatio, _maxTokens);
            }
        }

        public void CallFailure()
        {
            lock (_lock)
            {
                _tokenCount = Math.Max(_tokenCount - 1, 0);
            }
        }
    }
}
