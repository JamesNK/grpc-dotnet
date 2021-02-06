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
using System.Collections.ObjectModel;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Grpc.Net.Client.Configuration
{
    public sealed class Name : ConfigObject
    {
        public static readonly Name All = new Name(new ReadOnlyDictionary<string, object>(new Dictionary<string, object>()));

        private const string ServicePropertyName = "service";
        private const string MethodPropertyName = "method";

        public string? Service
        {
            get => GetValue<string>(ServicePropertyName);
            set => SetValue(ServicePropertyName, value);
        }

        public string? Method
        {
            get => GetValue<string>(MethodPropertyName);
            set => SetValue(MethodPropertyName, value);
        }

        public Name() { }

        internal Name(IDictionary<string, object> inner) : base(inner) { }
    }
}
