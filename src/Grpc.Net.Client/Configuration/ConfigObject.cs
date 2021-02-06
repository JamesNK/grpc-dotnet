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

using System.Collections;
using System.Collections.Generic;
using Grpc.Net.Client.Internal.Config;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Grpc.Net.Client.Configuration
{
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
