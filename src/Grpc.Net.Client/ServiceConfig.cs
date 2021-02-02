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
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using Grpc.Core;
using Grpc.Net.Client.Internal.ServiceConfig;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Grpc.Net.Client
{
    public sealed class ServiceConfig : ConfigObject
    {
        private const string MethodConfigPropertyName = "method_config";

        private ConfigProperty<Values<MethodConfig, object>, IList<object>> _methods =
            new(i => new Values<MethodConfig, object>(i ?? new List<object>(), new List<MethodConfig>(), s => s.Inner, s => new MethodConfig((IDictionary<string, object>)s)), MethodConfigPropertyName);

        public IList<MethodConfig> MethodConfigs
        {
            get => _methods.GetValue(this)!;
        }

        public ServiceConfig() { }

        internal ServiceConfig(IDictionary<string, object> inner) : base(inner) { }
    }

    public sealed class Name : ConfigObject
    {
        public static readonly Name AllServices = new Name(new ReadOnlyDictionary<string, object>(new Dictionary<string, object>()));

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

    public sealed class MethodConfig : ConfigObject
    {
        private const string NamePropertyName = "name";
        private const string RetryPolicyPropertyName = "retryPolicy";

        private ConfigProperty<Values<Name, object>, IList<object>> _names =
            new(i => new Values<Name, object>(i ?? new List<object>(), new List<Name>(), s => s.Inner, s => new Name((IDictionary<string, object>)s)), NamePropertyName);

        private ConfigProperty<RetryThrottlingPolicy, IDictionary<string, object>> _retryPolicy =
            new(i => i != null ? new RetryThrottlingPolicy(i) : null, RetryPolicyPropertyName);

        public MethodConfig()
        {
        }

        internal MethodConfig(IDictionary<string, object> inner) : base(inner)
        {
        }

        public RetryThrottlingPolicy? RetryPolicy
        {
            get => _retryPolicy.GetValue(this);
            set => _retryPolicy.SetValue(this, value);
        }

        public IList<Name> Names
        {
            get => _names.GetValue(this)!;
        }
    }

    public sealed class RetryThrottlingPolicy : ConfigObject
    {
        private const string MaxAttemptsPropertyName = "maxAttempts";
        private const string InitialBackoffPropertyName = "initialBackoff";
        private const string MaxBackoffPropertyName = "maxBackoff";
        private const string BackoffMultiplierPropertyName = "backoffMultiplier";
        private const string RetryableStatusCodesPropertyName = "retryableStatusCodes";

        private ConfigProperty<Values<StatusCode, object>, IList<object>> _retryableStatusCodes =
            new(i => new Values<StatusCode, object>(i ?? new List<object>(), new List<StatusCode>(), s => ConvertStatusCode(s), s => ConvertStatusCode(s.ToString()!)), RetryableStatusCodesPropertyName);

        public RetryThrottlingPolicy() { }
        internal RetryThrottlingPolicy(IDictionary<string, object> inner) : base(inner) { }

        private static string ConvertStatusCode(StatusCode statusCode)
        {
            return statusCode switch
            {
                StatusCode.OK => "OK",
                StatusCode.Cancelled => "CANCELLED",
                StatusCode.Unknown => "UNKNOWN",
                StatusCode.InvalidArgument => "INVALID_ARGUMENT",
                StatusCode.DeadlineExceeded => "DEADLINE_EXCEEDED",
                StatusCode.NotFound => "NOT_FOUND",
                StatusCode.AlreadyExists => "ALREADY_EXISTS",
                StatusCode.PermissionDenied => "PERMISSION_DENIED",
                StatusCode.Unauthenticated => "UNAUTHENTICATED",
                StatusCode.ResourceExhausted => "RESOURCE_EXHAUSTED",
                StatusCode.FailedPrecondition => "FAILED_PRECONDITION",
                StatusCode.Aborted => "ABORTED",
                StatusCode.OutOfRange => "OUT_OF_RANGE",
                StatusCode.Unimplemented => "UNIMPLEMENTED",
                StatusCode.Internal => "INTERNAL",
                StatusCode.Unavailable => "UNAVAILABLE",
                StatusCode.DataLoss => "DATA_LOSS",
                _ => throw new InvalidOperationException($"Unexpected status code: {statusCode}")
            };
        }

        private static StatusCode ConvertStatusCode(string statusCode)
        {
            return statusCode switch
            {
                "OK" => StatusCode.OK,
                "CANCELLED" => StatusCode.Cancelled,
                "UNKNOWN" => StatusCode.Unknown,
                "INVALID_ARGUMENT" => StatusCode.InvalidArgument,
                "DEADLINE_EXCEEDED" => StatusCode.DeadlineExceeded,
                "NOT_FOUND" => StatusCode.NotFound,
                "ALREADY_EXISTS" => StatusCode.AlreadyExists,
                "PERMISSION_DENIED" => StatusCode.PermissionDenied,
                "UNAUTHENTICATED" => StatusCode.Unauthenticated,
                "RESOURCE_EXHAUSTED" => StatusCode.ResourceExhausted,
                "FAILED_PRECONDITION" => StatusCode.FailedPrecondition,
                "ABORTED" => StatusCode.Aborted,
                "OUT_OF_RANGE" => StatusCode.OutOfRange,
                "UNIMPLEMENTED" => StatusCode.Unimplemented,
                "INTERNAL" => StatusCode.Internal,
                "UNAVAILABLE" => StatusCode.Unavailable,
                "DATA_LOSS" => StatusCode.DataLoss,
                _ => throw new InvalidOperationException($"Unexpected status code: {statusCode}")
            };
        }

        public int? MaxAttempts
        {
            get => GetValue<int>(MaxAttemptsPropertyName);
            set => SetValue(MaxAttemptsPropertyName, value);
        }

        public TimeSpan? InitialBackoff
        {
            get => ConvertDurationText(GetValue<string>(InitialBackoffPropertyName));
            set => SetValue(InitialBackoffPropertyName, ToDurationText(value));
        }

        public TimeSpan? MaxBackoff
        {
            get => ConvertDurationText(GetValue<string>(MaxBackoffPropertyName));
            set => SetValue(MaxBackoffPropertyName, ToDurationText(value));
        }

        public double BackoffMultiplier
        {
            get => GetValue<double>(BackoffMultiplierPropertyName);
            set => SetValue(BackoffMultiplierPropertyName, value);
        }

        public IList<StatusCode> RetryableStatusCodes
        {
            get => _retryableStatusCodes.GetValue(this)!;
        }

        private static TimeSpan? ConvertDurationText(string? text)
        {
            if (text == null)
            {
                return null;
            }

            if (text[text.Length - 1] == 's')
            {
                return TimeSpan.FromSeconds(Convert.ToDouble(text.Substring(0, text.Length - 1), CultureInfo.InvariantCulture));
            }
            else
            {
                throw new FormatException($"'{text}' isn't a valid duration.");
            }
        }

        public static string? ToDurationText(TimeSpan? value)
        {
            if (value == null)
            {
                return null;
            }

            return value.GetValueOrDefault().TotalSeconds + "s";
        }
    }

    public abstract class ConfigObject : IDictionary<string, object>, IConfigValue
    {
        internal readonly IDictionary<string, object> Inner;

        internal ConfigObject() : this(new Dictionary<string, object>())
        {
        }

        internal ConfigObject(IDictionary<string, object> inner)
        {
            Inner = inner;
        }

        object IDictionary<string, object>.this[string key] { get => Inner[key]; set => Inner[key] = value; }

        ICollection<string> IDictionary<string, object>.Keys => Inner.Keys;
        ICollection<object> IDictionary<string, object>.Values => Inner.Values;
        int ICollection<KeyValuePair<string, object>>.Count => Inner.Count;
        bool ICollection<KeyValuePair<string, object>>.IsReadOnly => ((IDictionary<string, object>)Inner).IsReadOnly;

        object IConfigValue.Inner => Inner;

        void IDictionary<string, object>.Add(string key, object value) => Inner.Add(key, value);

        void ICollection<KeyValuePair<string, object>>.Add(KeyValuePair<string, object> item) => ((IDictionary<string, object>)Inner).Add(item);

        void ICollection<KeyValuePair<string, object>>.Clear() => Inner.Clear();

        bool ICollection<KeyValuePair<string, object>>.Contains(KeyValuePair<string, object> item) => ((IDictionary<string, object>)Inner).Contains(item);

        bool IDictionary<string, object>.ContainsKey(string key) => Inner.ContainsKey(key);

        void ICollection<KeyValuePair<string, object>>.CopyTo(KeyValuePair<string, object>[] array, int arrayIndex) => ((IDictionary<string, object>)Inner).CopyTo(array, arrayIndex);

        IEnumerator<KeyValuePair<string, object>> IEnumerable<KeyValuePair<string, object>>.GetEnumerator() => Inner.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => Inner.GetEnumerator();

        bool IDictionary<string, object>.Remove(string key) => Inner.Remove(key);

        bool ICollection<KeyValuePair<string, object>>.Remove(KeyValuePair<string, object> item) => ((IDictionary<string, object>)Inner).Remove(item);

        bool IDictionary<string, object>.TryGetValue(string key, out object value) => Inner.TryGetValue(key, out value!);

        internal T? GetValue<T>(string key)
        {
            if (Inner.TryGetValue(key, out var value))
            {
                return (T?)value;
            }
            return default;
        }

        internal void SetValue<T>(string key, T? value)
        {
            if (value == null)
            {
                Inner.Remove(key);
            }
            else
            {
                Inner[key] = value;
            }
        }
    }
}
