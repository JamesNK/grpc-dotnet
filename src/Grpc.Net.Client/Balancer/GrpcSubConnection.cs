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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client.Balancer.Internal;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Balancer
{
    internal class GrpcSubConnection : SubConnection
    {
        private readonly List<DnsEndPoint> _addresses;
        private readonly GrpcConnection _connection;
        private readonly SemaphoreSlim _connectionCreateLock;
        private readonly List<(Socket Socket, Stream? Stream)> _activeTransports;

        private DnsEndPoint? _currentEndPoint;
        private ConnectivityState _state;

        public override DnsEndPoint? CurrentEndPoint => _currentEndPoint;
        public IReadOnlyList<DnsEndPoint> Addresses => _addresses;
        public ILogger Logger => _connection.Logger;

        public GrpcSubConnection(GrpcConnection connection, IReadOnlyList<DnsEndPoint> addresses)
        {
            _addresses = addresses.ToList();
            _connection = connection;
            _connectionCreateLock = new SemaphoreSlim(1);
            _activeTransports = new List<(Socket, Stream?)>();
        }

        public void UpdateAddresses(IReadOnlyList<DnsEndPoint> addresses)
        {
            _addresses.Clear();
            _addresses.AddRange(addresses);

            if (CurrentEndPoint != null && !_addresses.Contains(CurrentEndPoint))
            {
                _ = ConnectAsync(CancellationToken.None);
            }
        }

        public override async Task ConnectAsync(CancellationToken cancellationToken)
        {
            if (_state == ConnectivityState.Shutdown)
            {
                throw new InvalidOperationException("Sub-connection has been shutdown.");
            }

            if (_state != ConnectivityState.Idle)
            {
                return;
            }

            UpdateConnectivityState(ConnectivityState.Connecting);

            await ResetTransportAsync().ConfigureAwait(false);
        }

        private void UpdateConnectivityState(ConnectivityState state)
        {
            if (_state == state)
            {
                return;
            }
            _state = state;
            _connection.OnSubConnectionStateChange(this, _state);
        }

        private async Task ResetTransportAsync()
        {
            for (var attempt = 0; ; attempt++)
            {
                if (attempt > 0)
                {
                    await _connection.ResolveNowAsync().ConfigureAwait(false);
                }

                if (_state == ConnectivityState.Shutdown)
                {
                    return;
                }

                UpdateConnectivityState(ConnectivityState.Connecting);

                _currentEndPoint = await TryConnectAsync(CancellationToken.None).ConfigureAwait(false);
                if (_currentEndPoint != null)
                {
                    return;
                }
            }
        }

        public async ValueTask<DnsEndPoint?> TryConnectAsync(CancellationToken cancellationToken)
        {
            Debug.Assert(_addresses.Count > 0);
            Debug.Assert(CurrentEndPoint == null);

            await _connectionCreateLock.WaitAsync().ConfigureAwait(false);
            try
            {
                // Loop through endpoints and attempt to connect
                Exception? firstConnectionError = null;
                foreach (var currentEndPoint in _addresses)
                {
                    Socket socket;

                    Logger.LogInformation("Creating socket: " + currentEndPoint);
                    socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
                    UpdateConnectivityState(ConnectivityState.Connecting);

                    try
                    {
                        Logger.LogInformation("Connecting: " + currentEndPoint);
                        await socket.ConnectAsync(currentEndPoint, cancellationToken).ConfigureAwait(false);
                        Logger.LogInformation("Connected: " + currentEndPoint);

                        _activeTransports.Add((socket, null));

                        UpdateConnectivityState(ConnectivityState.Ready);
                        return currentEndPoint;
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError("Connect error: " + currentEndPoint + " " + ex);

                        if (firstConnectionError == null)
                        {
                            firstConnectionError = ex;
                        }
                    }
                }

                // All connections failed
                UpdateConnectivityState(ConnectivityState.TransientFailure);
                throw new InvalidOperationException("All connections failed.", firstConnectionError);
            }
            finally
            {
                _connectionCreateLock.Release();
            }
        }

        public override async ValueTask<Stream> GetStreamAsync(DnsEndPoint endPoint, CancellationToken cancellationToken)
        {
            Logger.LogInformation("GetStreamAsync: " + CurrentEndPoint);

            if (!Equals(endPoint, CurrentEndPoint))
            {
                throw new InvalidOperationException();
            }

            if (_activeTransports.Count == 1 && _activeTransports[0].Stream == null)
            {
                var networkStream = new NetworkStream(_activeTransports[0].Socket, ownsSocket: true);
                var stream = new StreamWrapper(networkStream, OnStreamDisposed);

                _activeTransports[0] = (_activeTransports[0].Socket, stream);

                return stream;
            }
            else
            {
                var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
                await socket.ConnectAsync(endPoint, cancellationToken).ConfigureAwait(false);

                var networkStream = new NetworkStream(socket, ownsSocket: true);
                var stream = new StreamWrapper(networkStream, OnStreamDisposed);

                _activeTransports.Add((socket, stream));

                return stream;
            }

            throw new InvalidOperationException();
        }

        private void OnStreamDisposed(Stream streamWrapper)
        {
            for (var i = _activeTransports.Count - 1; i >= 0; i--)
            {
                var t = _activeTransports[i];
                if (t.Stream == streamWrapper)
                {
                    _activeTransports.RemoveAt(i);
                    Logger.LogInformation("Disconnected: " + CurrentEndPoint);

                    if (_activeTransports.Count == 0)
                    {
                        _currentEndPoint = null;
                        UpdateConnectivityState(ConnectivityState.Idle);
                    }

                    return;
                }
            }
        }
    }

}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif