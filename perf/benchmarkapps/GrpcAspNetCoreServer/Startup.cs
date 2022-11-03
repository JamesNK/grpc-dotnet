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

using Grpc.Shared;

namespace GrpcAspNetCoreServer;

public class Startup
{
    public void ConfigureServices(IServiceCollection services)
    {
        services.AddGrpc(o =>
        {
#if NET5_0_OR_GREATER
            // Small performance benefit to not add catch-all routes to handle UNIMPLEMENTED for unknown services
            o.IgnoreUnknownServices = true;
#endif
        });
        services.Configure<RouteOptions>(c =>
        {
            // Small performance benefit to skip checking for security metadata on endpoint
            c.SuppressCheckForUnhandledSecurityMetadata = true;
        });
        services.AddSingleton<BenchmarkServiceImpl>();
    }

    public void Configure(IApplicationBuilder app, IHostApplicationLifetime applicationLifetime)
    {
        // Required to notify performance infrastructure that it can begin benchmarks
        applicationLifetime.ApplicationStarted.Register(() => Console.WriteLine("Application started."));

        app.UseRouting();

        app.UseMiddleware<ServiceProvidersMiddleware>();

        app.UseEndpoints(endpoints =>
        {
            endpoints.MapGrpcService<BenchmarkServiceImpl>();
        });
    }
}
