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

using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace Grpc.Net.Client.Internal.Retry
{
    internal class RetryCallBaseClientStreamReader<TRequest, TResponse> : IAsyncStreamReader<TResponse>
        where TRequest : class
        where TResponse : class
    {
        private readonly RetryCallBase<TRequest, TResponse> _retryCallBase;

        public RetryCallBaseClientStreamReader(RetryCallBase<TRequest, TResponse> retryCallBase)
        {
            _retryCallBase = retryCallBase;
        }

        // Suppress warning when overriding interface definition
#pragma warning disable CS8613, CS8766 // Nullability of reference types in return type doesn't match implicitly implemented member.
        public TResponse? Current => _retryCallBase.FinalizedCallTask.IsCompletedSuccessfully
                    ? _retryCallBase.FinalizedCallTask.Result.ClientStreamReader!.Current
                    : null;
#pragma warning restore CS8613, CS8766 // Nullability of reference types in return type doesn't match implicitly implemented member.

        public async Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            var call = await _retryCallBase.FinalizedCallTask.ConfigureAwait(false);
            return await call.ClientStreamReader!.MoveNext(cancellationToken).ConfigureAwait(false);
        }
    }
}