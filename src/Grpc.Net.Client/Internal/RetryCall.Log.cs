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
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Internal
{
    internal partial class RetryCall<TRequest, TResponse> : IGrpcCall<TRequest, TResponse>
        where TRequest : class
        where TResponse : class
    {
        private static class Log
        {
            private static readonly Action<ILogger, StatusCode, int, RetryResult, Exception?> _retryEvaluated =
                LoggerMessage.Define<StatusCode, int, RetryResult>(LogLevel.Debug, new EventId(1, "RetryEvaluated"), "Evaluated retry decision for failed gRPC call. Status code: '{StatusCode}', Attempt: {AttemptCount}, Decision: {RetryResult}");

            private static readonly Action<ILogger, string, Exception?> _retryPushbackReceived =
                LoggerMessage.Define<string>(LogLevel.Debug, new EventId(2, "RetryPushbackReceived"), "Retry pushback of '{RetryPushback}' received from the failed gRPC call.");

            private static readonly Action<ILogger, TimeSpan, Exception?> _startingRetryDelay =
                LoggerMessage.Define<TimeSpan>(LogLevel.Trace, new EventId(3, "StartingRetryDelay"), "Starting retry delay of {DelayDuration}.");

            private static readonly Action<ILogger, Exception> _errorRetryingCall =
                LoggerMessage.Define(LogLevel.Error, new EventId(4, "ErrorRetryingCall"), "Error retrying gRPC call.");

            internal static void RetryEvaluated(ILogger logger, StatusCode statusCode, int attemptCount, RetryResult result)
            {
                _retryEvaluated(logger, statusCode, attemptCount, result, null);
            }

            internal static void RetryPushbackReceived(ILogger logger, string retryPushback)
            {
                _retryPushbackReceived(logger, retryPushback, null);
            }

            internal static void StartingRetryDelay(ILogger logger, TimeSpan delayDuration)
            {
                _startingRetryDelay(logger, delayDuration, null);
            }

            internal static void ErrorRetryingCall(ILogger logger, Exception ex)
            {
                _errorRetryingCall(logger, ex);
            }
        }
    }
}
