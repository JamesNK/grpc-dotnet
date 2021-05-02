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

#if HAVE_LOAD_BALANCING
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Balancer.Internal
{
    internal interface ITransport : IDisposable
    {
        void OnRequestError(Exception ex);
        void OnRequestSuccess();
        DnsEndPoint? CurrentEndPoint { get; }

#if NET5_0_OR_GREATER
        ValueTask<Stream> GetStreamAsync(DnsEndPoint endPoint, CancellationToken cancellationToken);
#endif
        ValueTask<bool> TryConnectAsync(CancellationToken cancellationToken);
    }

    internal class ActiveHealthTransport : ITransport, IDisposable
    {
        private readonly SemaphoreSlim _connectionCreateLock;
        private readonly SubChannel _subChannel;
        private int _lastEndPointIndex;

#if NET5_0_OR_GREATER
        internal readonly List<(DnsEndPoint EndPoint, Socket Socket, Stream? Stream)> _activeStreams;
        private readonly Timer _socketConnectedTimer;
        private Socket? _initialSocket;
#endif
        private DnsEndPoint? _currentEndPoint;

        public ActiveHealthTransport(SubChannel subChannel)
        {
            _connectionCreateLock = new SemaphoreSlim(1);
            _subChannel = subChannel;
            _lastEndPointIndex = -1; // Start -1 so first attempt is at index 0

#if NET5_0_OR_GREATER
            _activeStreams = new List<(DnsEndPoint, Socket, Stream?)>();
            _socketConnectedTimer = new Timer(OnSocketConnected, state: null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
#endif
        }

        public object Lock => _subChannel.Lock;
        public DnsEndPoint? CurrentEndPoint => _currentEndPoint;
        public bool HasStream { get; }

        public void OnRequestError(Exception ex)
        {
            throw new NotImplementedException();
        }

        public void OnRequestSuccess()
        {
            throw new NotImplementedException();
        }

        public async ValueTask<bool> TryConnectAsync(CancellationToken cancellationToken)
        {
            Debug.Assert(_subChannel._addresses.Count > 0);
            Debug.Assert(CurrentEndPoint == null);

            await _connectionCreateLock.WaitAsync().ConfigureAwait(false);
            try
            {
                // Loop through endpoints and attempt to connect
                Exception? firstConnectionError = null;

#pragma warning disable CS0162 // Unreachable code detected
                for (int i = _lastEndPointIndex; (i - _lastEndPointIndex) < _subChannel._addresses.Count; i++)
#pragma warning restore CS0162 // Unreachable code detected
                {
                    var currentIndex = (i + _subChannel._addresses.Count) % _subChannel._addresses.Count;
                    var currentEndPoint = _subChannel._addresses[currentIndex];

#if NET5_0_OR_GREATER
                    Socket socket;

                    _subChannel.Logger.LogInformation("Creating socket: " + currentEndPoint);
                    socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
                    _subChannel.UpdateConnectivityState(ConnectivityState.Connecting);

                    try
                    {
                        _subChannel.Logger.LogInformation("Connecting: " + currentEndPoint);
                        await socket.ConnectAsync(currentEndPoint, cancellationToken).ConfigureAwait(false);
                        _subChannel.Logger.LogInformation("Connected: " + currentEndPoint);

                        _subChannel.UpdateConnectivityState(ConnectivityState.Ready);

                        lock (Lock)
                        {
                            _currentEndPoint = currentEndPoint;
                            _lastEndPointIndex = currentIndex;
#if NET5_0_OR_GREATER
                            _initialSocket = socket;
                            _socketConnectedTimer.Change(TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
#endif
                            return true;
                        }
                    }
                    catch (Exception ex)
                    {
                        _subChannel.Logger.LogError("Connect error: " + currentEndPoint + " " + ex);

                        if (firstConnectionError == null)
                        {
                            firstConnectionError = ex;
                        }
                    }

#else
                    _subChannel.UpdateConnectivityState(ConnectivityState.Connecting);
                    _subChannel.UpdateConnectivityState(ConnectivityState.Ready);
                    lock (Lock)
                    {
                        _currentEndPoint = currentEndPoint;
                        _lastEndPointIndex = currentIndex;
                    }
#endif
                }

                // All connections failed
                _subChannel.UpdateConnectivityState(ConnectivityState.TransientFailure);
#if NET5_0_OR_GREATER
                _socketConnectedTimer.Change(TimeSpan.Zero, TimeSpan.Zero);
#endif
                throw new InvalidOperationException("All connections failed.", firstConnectionError);
            }
            finally
            {
                _connectionCreateLock.Release();
            }
        }

#if NET5_0_OR_GREATER
        private async void OnSocketConnected(object? state)
        {
            try
            {
                var socket = _initialSocket;
                if (socket != null)
                {
                    try
                    {
                        _subChannel.Logger.LogTrace("Pinging socket.");
                        await socket.SendAsync(Array.Empty<byte>(), SocketFlags.None).ConfigureAwait(false);
                        _subChannel.Logger.LogTrace("Successfully socket.");
                    }
                    catch (Exception ex)
                    {
                        _subChannel.Logger.LogTrace(ex, "Error when pinging socket.");

                        lock (Lock)
                        {
                            if (_initialSocket == socket)
                            {
                                _initialSocket = null;
                                _currentEndPoint = null;
                                _subChannel.UpdateConnectivityState(ConnectivityState.Idle);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _subChannel.Logger.LogError(ex, "Error when checking socket.");
            }
        }

        public async ValueTask<Stream> GetStreamAsync(DnsEndPoint endPoint, CancellationToken cancellationToken)
        {
            _subChannel.Logger.LogInformation("GetStreamAsync: " + CurrentEndPoint);

            Socket? socket = null;
            lock (Lock)
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

            lock (Lock)
            {
                _activeStreams.Add((endPoint, socket, stream));
                _subChannel.Logger.LogInformation("Transport created");
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
            lock (Lock)
            {
                for (var i = _activeStreams.Count - 1; i >= 0; i--)
                {
                    var t = _activeStreams[i];
                    if (t.Stream == streamWrapper)
                    {
                        _activeStreams.RemoveAt(i);
                        _subChannel.Logger.LogInformation("Disconnected: " + CurrentEndPoint);

                        if (_activeStreams.Count == 0)
                        {
                            _currentEndPoint = null;
                            _socketConnectedTimer.Change(TimeSpan.Zero, TimeSpan.Zero);
                            _subChannel.UpdateConnectivityState(ConnectivityState.Idle);
                        }

                        return;
                    }
                }
            }
        }
#endif

        public void Dispose()
        {
#if NET5_0_OR_GREATER
            _socketConnectedTimer.Dispose();
#endif
        }
    }
}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif