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

namespace Grpc.Net.Client.Configuration
{
    /// <summary>
    /// The hedging policy for outgoing calls. Hedged calls may execute more than
    /// once on the server, so only idempotent methods should specify a hedging
    /// policy.
    /// </summary>
    public sealed class HedgingPolicy : ConfigObject
    {
        internal const string MaxAttemptsPropertyName = "maxAttempts";
        internal const string HedgingDelayPropertyName = "hedgingDelay";
        internal const string NonFatalStatusCodesPropertyName = "nonFatalStatusCodes";

        private ConfigProperty<Values<StatusCode, object>, IList<object>> _nonFatalStatusCodes =
            new(i => new Values<StatusCode, object>(i ?? new List<object>(), s => ConvertHelpers.ConvertStatusCode(s), s => ConvertHelpers.ConvertStatusCode(s.ToString()!)), NonFatalStatusCodesPropertyName);

        /// <summary>
        /// Initializes a new instance of the <see cref="HedgingPolicy"/> class.
        /// </summary>
        public HedgingPolicy() { }
        internal HedgingPolicy(IDictionary<string, object> inner) : base(inner) { }

        /// <summary>
        /// The hedging policy will send up to this number of calls.
        /// This number represents the total number of all attempts, including
        /// the original attempt.
        ///
        /// This field is required and must be greater than 1.
        /// This value is limited by <see cref="GrpcChannelOptions.MaxRetryAttempts"/>.
        /// </summary>
        public int? MaxAttempts
        {
            get => GetValue<int>(MaxAttemptsPropertyName);
            set => SetValue(MaxAttemptsPropertyName, value);
        }

        /// <summary>
        /// The first call will be sent immediately, but the subsequent
        /// hedged call will be sent at intervals of every delay. Set this
        /// to 0 to immediately send all hedged calls.
        /// </summary>
        public TimeSpan? HedgingDelay
        {
            get => ConvertHelpers.ConvertDurationText(GetValue<string>(HedgingDelayPropertyName));
            set => SetValue(HedgingDelayPropertyName, ConvertHelpers.ToDurationText(value));
        }

        /// <summary>
        /// The set of status codes which indicate other hedged calls may still
        /// succeed. If a non-fatal status code is returned by the server, hedged
        /// calls will continue. Otherwise, outstanding requests will be canceled and
        /// the error returned to the client application layer.
        ///
        /// Specifying status codes is optional.
        /// </summary>
        public IList<StatusCode> NonFatalStatusCodes
        {
            get => _nonFatalStatusCodes.GetValue(this)!;
        }
    }
}
