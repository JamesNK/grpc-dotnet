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
using System.Diagnostics.CodeAnalysis;

namespace Grpc.Net.Client.Internal.Config
{
    internal class KeyedValues<T> : IDictionary<string, T>, IConfigValue
    {
        private Dictionary<string, T> _keyValues;
        private readonly IDictionary<string, object> _inner;
        private readonly Func<T, object> _convertTo;
        private readonly Func<object, T> _convertFrom;

        public KeyedValues(IDictionary<string, object> inner, Func<T, object> convertTo, Func<object, T> convertFrom)
        {
            _keyValues = new Dictionary<string, T>();
            _inner = inner;
            _convertTo = convertTo;
            _convertFrom = convertFrom;

            foreach (var item in _inner)
            {
                _keyValues[item.Key] = _convertFrom(item);
            }
        }

        public T this[string key]
        {
            get => _keyValues[key];
            set
            {
                if (_keyValues.TryGetValue(key, out var existingValue) && Equals(existingValue, value))
                {
                    return;
                }

                _keyValues[key] = value;
                _inner[key] = _convertTo(value);
            }
        }

        public ICollection<string> Keys => _keyValues.Keys;
        public ICollection<T> Values => _keyValues.Values;

        public int Count => _keyValues.Count;
        public bool IsReadOnly => false;

        object IConfigValue.Inner => _inner;

        public void Add(string key, T value)
        {
            _keyValues.Add(key, value);
            _inner.Add(key, _convertTo(value));
        }

        public void Add(KeyValuePair<string, T> item) => Add(item.Key, item.Value);

        public bool Contains(KeyValuePair<string, T> item) => ((IDictionary<string, T>)_keyValues).Contains(item);

        public bool ContainsKey(string key) => _keyValues.ContainsKey(key);

        public void CopyTo(KeyValuePair<string, T>[] array, int arrayIndex) => ((IDictionary<string, T>)_keyValues).CopyTo(array, arrayIndex);

        public bool Remove(string key) => _keyValues.Remove(key) && _inner.Remove(key);

        public bool Remove(KeyValuePair<string, T> item) => ((IDictionary<string, T>)_keyValues).Remove(item) && _inner.Remove(new KeyValuePair<string, object>(item.Key, _convertTo(item.Value)));

#pragma warning disable CS8767 // Nullability of reference types in type of parameter doesn't match implicitly implemented member (possibly because of nullability attributes).
        public bool TryGetValue(string key, [MaybeNullWhen(false)] out T value)
#pragma warning restore CS8767 // Nullability of reference types in type of parameter doesn't match implicitly implemented member (possibly because of nullability attributes).
        {
            return _keyValues.TryGetValue(key, out value);
        }

        IEnumerator<KeyValuePair<string, T>> IEnumerable<KeyValuePair<string, T>>.GetEnumerator()
        {
            return _keyValues.GetEnumerator();
        }

        public void Clear()
        {
            _keyValues.Clear();
            _inner.Clear();
        }

        public IEnumerator GetEnumerator()
        {
            return _keyValues.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }

}
