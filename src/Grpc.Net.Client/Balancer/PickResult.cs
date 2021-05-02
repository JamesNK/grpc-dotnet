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

#if HAVE_LOAD_BALANCING
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System;
using System.Diagnostics;

namespace Grpc.Net.Client.Balancer
{
    public class PickResult
    {
        private readonly Action<CompleteContext>? _onComplete;

        [DebuggerStepThrough]
        public PickResult(SubChannel? subChannel, Action<CompleteContext>? onComplete)
        {
            SubChannel = subChannel;
            _onComplete = onComplete;
        }

        public SubChannel? SubChannel { get; }

        public void Complete(CompleteContext context)
        {
            _onComplete?.Invoke(context);
        }
    }

}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif