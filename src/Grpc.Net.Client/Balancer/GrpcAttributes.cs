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
using System.Diagnostics.CodeAnalysis;

#if NET5_0_OR_GREATER
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Grpc.Net.Client
{
    public sealed class GrpcAttributes : IDictionary<string, object?>
    {
        public static readonly GrpcAttributes Empty = new GrpcAttributes(new ReadOnlyDictionary<string, object?>(new Dictionary<string, object?>()));

        private readonly IDictionary<string, object?> _attributes;

        public GrpcAttributes() : this(new Dictionary<string, object?>())
        {
        }

        private GrpcAttributes(IDictionary<string, object?> attributes)
        {
            _attributes = attributes;
        }

        object? IDictionary<string, object?>.this[string key]
        {
            get
            {
                return _attributes[key];
            }
            set
            {
                _attributes[key] = value;
            }
        }
        ICollection<string> IDictionary<string, object?>.Keys => _attributes.Keys;
        ICollection<object?> IDictionary<string, object?>.Values => _attributes.Values;
        int ICollection<KeyValuePair<string, object?>>.Count => _attributes.Count;
        bool ICollection<KeyValuePair<string, object?>>.IsReadOnly => ((IDictionary<string, object?>)_attributes).IsReadOnly;
        void IDictionary<string, object?>.Add(string key, object? value) => _attributes.Add(key, value);
        void ICollection<KeyValuePair<string, object?>>.Add(KeyValuePair<string, object?> item) => ((IDictionary<string, object?>)_attributes).Add(item);
        void ICollection<KeyValuePair<string, object?>>.Clear() => _attributes.Clear();
        bool ICollection<KeyValuePair<string, object?>>.Contains(KeyValuePair<string, object?> item) => ((IDictionary<string, object?>)_attributes).Contains(item);
        bool IDictionary<string, object?>.ContainsKey(string key) => _attributes.ContainsKey(key);
        void ICollection<KeyValuePair<string, object?>>.CopyTo(KeyValuePair<string, object?>[] array, int arrayIndex) =>
            ((IDictionary<string, object?>)_attributes).CopyTo(array, arrayIndex);
        IEnumerator<KeyValuePair<string, object?>> IEnumerable<KeyValuePair<string, object?>>.GetEnumerator() => _attributes.GetEnumerator();
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => ((System.Collections.IEnumerable)_attributes).GetEnumerator();
        bool IDictionary<string, object?>.Remove(string key) => _attributes.Remove(key);
        bool ICollection<KeyValuePair<string, object?>>.Remove(KeyValuePair<string, object?> item) => ((IDictionary<string, object?>)_attributes).Remove(item);
        bool IDictionary<string, object?>.TryGetValue(string key, out object? value) => _attributes.TryGetValue(key, out value);
        public bool TryGetValue<TValue>(GrpcAttributesKey<TValue> key, [MaybeNullWhen(false)] out TValue value)
        {
            if (_attributes.TryGetValue(key.Key, out object? _value) && _value is TValue tvalue)
            {
                value = tvalue;
                return true;
            }

            value = default(TValue);
            return false;
        }

        public void Set<TValue>(GrpcAttributesKey<TValue> key, TValue value)
        {
            _attributes[key.Key] = value;
        }
    }
}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif