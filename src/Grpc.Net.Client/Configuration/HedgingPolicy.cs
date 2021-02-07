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
using System.Collections.Generic;
using Grpc.Core;
using Grpc.Net.Client.Internal.Configuration;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Grpc.Net.Client.Configuration
{
    public sealed class HedgingPolicy : ConfigObject
    {
        internal const string MaxAttemptsPropertyName = "maxAttempts";
        internal const string HedgingDelayPropertyName = "hedgingDelay";
        internal const string NonFatalStatusCodesPropertyName = "nonFatalStatusCodes";

        private ConfigProperty<Values<StatusCode, object>, IList<object>> _nonFatalStatusCodes =
            new(i => new Values<StatusCode, object>(i ?? new List<object>(), new List<StatusCode>(), s => ConvertHelpers.ConvertStatusCode(s), s => ConvertHelpers.ConvertStatusCode(s.ToString()!)), NonFatalStatusCodesPropertyName);

        public HedgingPolicy() { }
        internal HedgingPolicy(IDictionary<string, object> inner) : base(inner) { }

        public int? MaxAttempts
        {
            get => GetValue<int>(MaxAttemptsPropertyName);
            set => SetValue(MaxAttemptsPropertyName, value);
        }

        public TimeSpan? HedgingDelay
        {
            get => ConvertHelpers.ConvertDurationText(GetValue<string>(HedgingDelayPropertyName));
            set => SetValue(HedgingDelayPropertyName, ConvertHelpers.ToDurationText(value));
        }

        public IList<StatusCode> NonFatalStatusCodes
        {
            get => _nonFatalStatusCodes.GetValue(this)!;
        }
    }
}
