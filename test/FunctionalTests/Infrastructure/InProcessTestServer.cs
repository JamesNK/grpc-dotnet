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
using System.IO;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Grpc.AspNetCore.FunctionalTests.Infrastructure
{
    public abstract class InProcessTestServer : IDisposable
    {
        internal abstract event Action<LogRecord> ServerLogged;

        public abstract string GetUrl(TestServerEndpointName endpointName);

        public abstract IWebHost? Host { get; }

        public abstract void StartServer();

        public abstract void Dispose();
    }

    public class InProcessTestServer<TStartup> : InProcessTestServer
        where TStartup : class
    {
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger _logger;
        private readonly LogSinkProvider _logSinkProvider;
        private readonly Action<IServiceCollection>? _initialConfigureServices;
        private IWebHost? _host;
        private IHostApplicationLifetime? _lifetime;
        private Dictionary<TestServerEndpointName, string>? _urls;

        internal override event Action<LogRecord> ServerLogged
        {
            add => _logSinkProvider.RecordLogged += value;
            remove => _logSinkProvider.RecordLogged -= value;
        }

        public override string GetUrl(TestServerEndpointName endpointName)
        {
            if (_urls == null)
            {
                throw new InvalidOperationException();
            }

            return _urls[endpointName];
        }

        public override IWebHost? Host => _host;

        public InProcessTestServer(Action<IServiceCollection>? initialConfigureServices)
        {
            _logSinkProvider = new LogSinkProvider();
            _loggerFactory = new LoggerFactory();
            _loggerFactory.AddProvider(_logSinkProvider);
            _logger = _loggerFactory.CreateLogger<InProcessTestServer<TStartup>>();

            _initialConfigureServices = initialConfigureServices;
        }

        public override void StartServer()
        {
            var basePath = Path.GetDirectoryName(typeof(InProcessTestServer).Assembly.Location);
            var certPath = Path.Combine(basePath!, "server1.pfx");
            var cert = new X509Certificate2(certPath, "1111");

            // If cert above is used then error is thrown by server:
            // QuicException: Failed to open stream to peer. Error Code: INVALID_STATE
            var certLocal = CertificateLoader.LoadFromStoreCert("localhost", StoreName.My.ToString(), StoreLocation.CurrentUser, false);

            _host = new WebHostBuilder()
                .ConfigureLogging(builder => builder
                    .SetMinimumLevel(LogLevel.Trace)
                    .AddProvider(new ForwardingLoggerProvider(_loggerFactory)))
                .ConfigureServices(services =>
                {
                    _initialConfigureServices?.Invoke(services);
                })
                .UseStartup(typeof(TStartup))
#if NET6_0
                .UseQuic(options =>
                {
                    options.Certificate = certLocal;
                    options.Alpn = "h3-29";
                })
#endif
                .UseKestrel(options =>
                {
#if NET6_0
                    options.EnableAltSvc = true;
#endif

                    options.ListenLocalhost(50050, listenOptions =>
                    {
                        listenOptions.Protocols = HttpProtocols.Http2;
                        listenOptions.UseConnectionLogging();
                    });
                    options.ListenLocalhost(50040, listenOptions =>
                    {
                        listenOptions.Protocols = HttpProtocols.Http1;
                    });
                    options.ListenLocalhost(50030, listenOptions =>
                    {
                        listenOptions.Protocols = HttpProtocols.Http2;
                        listenOptions.UseHttps(cert);
                    });
                    options.ListenLocalhost(50020, listenOptions =>
                    {
                        listenOptions.Protocols = HttpProtocols.Http1;
                        listenOptions.UseHttps(cert);
                    });
#if NET6_0
                    options.ListenLocalhost(50010, listenOptions =>
                    {
                        listenOptions.Protocols = HttpProtocols.Http1AndHttp2AndHttp3;
                        listenOptions.UseConnectionLogging();
                        listenOptions.UseHttps(certLocal);
                    });
#endif
                })
                .UseContentRoot(Directory.GetCurrentDirectory())
                .Build();

            var t = Task.Run(() => _host.Start());
            _logger.LogInformation("Starting test server...");
            _lifetime = _host.Services.GetRequiredService<IHostApplicationLifetime>();

            // This only happens once per fixture, so we can afford to wait a little bit on it.
            if (!_lifetime.ApplicationStarted.WaitHandle.WaitOne(TimeSpan.FromSeconds(20)))
            {
                // t probably faulted
                if (t.IsFaulted)
                {
                    throw t.Exception!.InnerException!;
                }

                var logs = _logSinkProvider.GetLogs();
                throw new TimeoutException($"Timed out waiting for application to start.{Environment.NewLine}Startup Logs:{Environment.NewLine}{RenderLogs(logs)}");
            }
            _logger.LogInformation("Test Server started");

            // Get the URL from the server
            _urls = new Dictionary<TestServerEndpointName, string>
            {
                [TestServerEndpointName.Http2] = "http://localhost:50050",
                [TestServerEndpointName.Http1] = "http://localhost:50040",
                [TestServerEndpointName.Http2WithTls] = "https://localhost:50030",
                [TestServerEndpointName.Http1WithTls] = "https://localhost:50020",
                [TestServerEndpointName.Http3WithTls] = "https://localhost:50010"
            };

            _lifetime.ApplicationStopped.Register(() =>
            {
                _logger.LogInformation("Test server shut down");
            });
        }

        private string RenderLogs(IList<LogRecord> logs)
        {
            var builder = new StringBuilder();
            foreach (var log in logs)
            {
                builder.AppendLine($"{log.Timestamp:O} {log.LoggerName} {log.LogLevel}: {log.Formatter(log.State, log.Exception)}");
                if (log.Exception != null)
                {
                    var message = log.Exception.ToString();
                    foreach (var line in message.Split(new[] { Environment.NewLine }, StringSplitOptions.None))
                    {
                        builder.AppendLine($"| {line}");
                    }
                }
            }
            return builder.ToString();
        }

        public override void Dispose()
        {
            _logger.LogInformation("Shutting down test server");
            _host?.Dispose();
            _loggerFactory.Dispose();
        }

        private class ForwardingLoggerProvider : ILoggerProvider
        {
            private readonly ILoggerFactory _loggerFactory;

            public ForwardingLoggerProvider(ILoggerFactory loggerFactory)
            {
                _loggerFactory = loggerFactory;
            }

            public void Dispose()
            {
            }

            public ILogger CreateLogger(string categoryName)
            {
                return _loggerFactory.CreateLogger(categoryName);
            }
        }
    }
}
