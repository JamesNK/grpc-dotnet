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
using Grpc.Net.Client.Internal.Configuration;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Grpc.Net.Client.Configuration
{
    public sealed class MethodConfig : ConfigObject
    {
        private const string NamePropertyName = "name";
        private const string RetryPolicyPropertyName = "retryPolicy";

        private ConfigProperty<Values<Name, object>, IList<object>> _names =
            new(i => new Values<Name, object>(i ?? new List<object>(), new List<Name>(), s => s.Inner, s => new Name((IDictionary<string, object>)s)), NamePropertyName);

        private ConfigProperty<RetryPolicy, IDictionary<string, object>> _retryPolicy =
            new(i => i != null ? new RetryPolicy(i) : null, RetryPolicyPropertyName);

        public MethodConfig()
        {
        }

        internal MethodConfig(IDictionary<string, object> inner) : base(inner)
        {
        }

        public RetryPolicy? RetryPolicy
        {
            get => _retryPolicy.GetValue(this);
            set => _retryPolicy.SetValue(this, value);
        }

        public IList<Name> Names
        {
            get => _names.GetValue(this)!;
        }
    }
}
