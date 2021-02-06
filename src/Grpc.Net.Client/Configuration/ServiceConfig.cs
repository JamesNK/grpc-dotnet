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
    public sealed class ServiceConfig : ConfigObject
    {
        private const string MethodConfigPropertyName = "method_config";
        private const string RetryThrottlingPropertyName = "retryThrottling";

        private ConfigProperty<Values<MethodConfig, object>, IList<object>> _methods =
            new(i => new Values<MethodConfig, object>(i ?? new List<object>(), new List<MethodConfig>(), s => s.Inner, s => new MethodConfig((IDictionary<string, object>)s)), MethodConfigPropertyName);

        private ConfigProperty<RetryThrottlingPolicy, IDictionary<string, object>> _retryThrottling =
            new(i => i != null ? new RetryThrottlingPolicy(i) : null, RetryThrottlingPropertyName);

        public IList<MethodConfig> MethodConfigs
        {
            get => _methods.GetValue(this)!;
        }

        public RetryThrottlingPolicy? RetryThrottling
        {
            get => _retryThrottling.GetValue(this);
            set => _retryThrottling.SetValue(this, value);
        }

        public ServiceConfig() { }

        internal ServiceConfig(IDictionary<string, object> inner) : base(inner) { }
    }
}
