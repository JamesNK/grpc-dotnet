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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
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
        internal readonly List<(DnsEndPoint EndPoint, Socket Socket, Stream? Stream)> _activeTransports;
        private readonly object _lock;
        private readonly Timer _socketConnectedTimer;

        private Socket? _initialSocket;
        private DnsEndPoint? _currentEndPoint;
        private ConnectivityState _state;
        private Task? _connectTask;

        public override DnsEndPoint? CurrentEndPoint => _currentEndPoint;
        public IReadOnlyList<DnsEndPoint> Addresses => _addresses;
        public ILogger Logger => _connection.Logger;

        public override ConnectivityState State => _state;

        public GrpcSubConnection(GrpcConnection connection, IReadOnlyList<DnsEndPoint> addresses)
        {
            _lock = new object();
            _addresses = addresses.ToList();
            _connection = connection;
            _connectionCreateLock = new SemaphoreSlim(1);
            _activeTransports = new List<(DnsEndPoint, Socket, Stream?)>();
            _socketConnectedTimer = new Timer(OnSocketConnected, state: null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        }

        public void UpdateAddresses(IReadOnlyList<DnsEndPoint> addresses)
        {
            var connect = false;
            lock (_lock)
            {
                _addresses.Clear();
                _addresses.AddRange(addresses);

                connect = (CurrentEndPoint != null && !_addresses.Contains(CurrentEndPoint));
            }
            if (connect)
            {
                _ = ConnectAsync(CancellationToken.None);
            }
        }

        public override Task ConnectAsync(CancellationToken cancellationToken)
        {
            lock (_lock)
            {
                if (!IsConnectInProgressUnsynchronized)
                {
                    _connectTask = ConnectCoreAsync(cancellationToken);
                }
                else
                {
                    Logger.LogInformation("Connect already in progress " + this);
                }

                return _connectTask;
            }
        }

        private async Task ConnectCoreAsync(CancellationToken cancellationToken)
        {
            Debug.Assert(Monitor.IsEntered(_lock));

            if (_state == ConnectivityState.Shutdown)
            {
                throw new InvalidOperationException("Sub-connection has been shutdown.");
            }

            if (_state != ConnectivityState.Idle)
            {
                return;
            }

            UpdateConnectivityState(ConnectivityState.Connecting);

            await ResetTransportAsync(cancellationToken).ConfigureAwait(false);
        }

        [MemberNotNullWhen(true, nameof(_connectTask))]
        private bool IsConnectInProgressUnsynchronized
        {
            get
            {
                Debug.Assert(Monitor.IsEntered(_lock));

                var connectTask = _connectTask;
                return connectTask != null && !connectTask.IsCompleted;
            }
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

        private async Task ResetTransportAsync(CancellationToken cancellationToken)
        {
            for (var attempt = 0; ; attempt++)
            {
                if (attempt > 0)
                {
                    await _connection.ResolveNowAsync(cancellationToken).ConfigureAwait(false);
                }

                if (_state == ConnectivityState.Shutdown)
                {
                    return;
                }

                UpdateConnectivityState(ConnectivityState.Connecting);

                var r = await TryConnectAsync(cancellationToken).ConfigureAwait(false);
                if (r != null)
                {
                    var result = r.GetValueOrDefault();
                    lock (_lock)
                    {
                        _initialSocket = result.Socket;
                        _currentEndPoint = result.EndPoint;
                        _socketConnectedTimer.Change(TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
                    }
                    return;
                }
            }
        }

        private async ValueTask<(DnsEndPoint EndPoint, Socket Socket)?> TryConnectAsync(CancellationToken cancellationToken)
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


                        UpdateConnectivityState(ConnectivityState.Ready);
                        return (currentEndPoint, socket);
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
                _socketConnectedTimer.Change(TimeSpan.Zero, TimeSpan.Zero);
                throw new InvalidOperationException("All connections failed.", firstConnectionError);
            }
            finally
            {
                _connectionCreateLock.Release();
            }
        }

        private async void OnSocketConnected(object? state)
        {
            try
            {
                var socket = _initialSocket;
                if (socket != null)
                {
                    try
                    {
                        _connection.Logger.LogTrace("Pinging socket.");
                        await socket.SendAsync(Array.Empty<byte>(), SocketFlags.None).ConfigureAwait(false);
                        _connection.Logger.LogTrace("Successfully socket.");
                    }
                    catch (Exception ex)
                    {
                        _connection.Logger.LogTrace(ex, "Error when pinging socket.");

                        lock (_lock)
                        {
                            if (_initialSocket == socket)
                            {
                                _initialSocket = null;
                                _currentEndPoint = null;
                                UpdateConnectivityState(ConnectivityState.Idle);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _connection.Logger.LogError(ex, "Error when checking socket.");
            }
        }

        public override async ValueTask<Stream> GetStreamAsync(DnsEndPoint endPoint, CancellationToken cancellationToken)
        {
            Logger.LogInformation("GetStreamAsync: " + CurrentEndPoint);

            Socket? socket = null;
            lock (_lock)
            {
                if (_initialSocket != null)
                {
                    socket = _initialSocket;
                    _initialSocket = null;
                }
            }

            // Check to see if we've received anything on the connection; if we have, that's
            // either erroneous data (we shouldn't have received anything yet) or the connection
            // has been closed; either way, we can't use it.
            if (socket != null)
            {
                if (!CanUseSocket(socket))
                {
                    socket.Dispose();
                    socket = null;
                }
            }

            if (socket == null)
            {
                socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
                await socket.ConnectAsync(endPoint, cancellationToken).ConfigureAwait(false);
            }

            var networkStream = new NetworkStream(socket, ownsSocket: true);
            var stream = new StreamWrapper(networkStream, OnStreamDisposed);

            lock (_lock)
            {
                _activeTransports.Add((endPoint, socket, stream));
                Logger.LogInformation("Transport created");
            }

            return stream;
        }

        private static bool CanUseSocket(Socket socket)
        {
            try
            {
                return !socket.Poll(0, SelectMode.SelectRead);
            }
            catch (Exception e) when (e is SocketException || e is ObjectDisposedException)
            {
                return false;
            }
        }

        private void OnStreamDisposed(Stream streamWrapper)
        {
            lock (_lock)
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
                            _socketConnectedTimer.Change(TimeSpan.Zero, TimeSpan.Zero);
                            UpdateConnectivityState(ConnectivityState.Idle);
                        }

                        return;
                    }
                }
            }
        }

        public override string ToString()
        {
            return string.Join(", ", _addresses);
        }

        public override IList<DnsEndPoint> GetAddresses()
        {
            return _addresses.ToArray();
        }

        public override void Shutdown()
        {
            _socketConnectedTimer.Dispose();
        }
    }
}

#endif