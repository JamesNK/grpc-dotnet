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

#if NET5_0_OR_GREATER
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System;

namespace Grpc.Net.Client.Balancer
{
    public class FailurePicker : SubConnectionPicker
    {
        private readonly Exception _exception;

        public FailurePicker(Exception exception)
        {
            _exception = exception;
        }

        public override PickResult Pick(PickContext context)
        {
            throw new InvalidOperationException("Error picking connection.", _exception);
        }
    }

}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif