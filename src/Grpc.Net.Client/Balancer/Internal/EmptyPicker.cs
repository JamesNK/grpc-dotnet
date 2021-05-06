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

namespace Grpc.Net.Client.Balancer.Internal
{
    public sealed class EmptyPicker : SubChannelPicker
    {
        public static readonly EmptyPicker Instance = new EmptyPicker();

        private readonly PickResult _pickResult;

        private EmptyPicker()
        {
            _pickResult = new PickResult(null, null, null);
        }

        public override PickResult Pick(PickContext context)
        {
            return _pickResult;
        }
    }
}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif