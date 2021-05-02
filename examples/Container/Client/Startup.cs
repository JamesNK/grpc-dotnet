using System;
using System.Linq;
using System.Net.Http;
using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Balancer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Client
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddRazorPages();
            services.AddServerSideBlazor();

            services.AddSingleton(services =>
            {
                // Get the service address from appsettings.json
                var config = services.GetRequiredService<IConfiguration>();
                var backendUrl = config["BackendUrl"];

                // If no address is set then fallback to the current webpage URL
                if (string.IsNullOrEmpty(backendUrl))
                {
                    var navigationManager = services.GetRequiredService<NavigationManager>();
                    backendUrl = navigationManager.BaseUri;
                }

                var loggerFactory = services.GetRequiredService<ILoggerFactory>();
                var grpcConnection = new ClientChannel(new DnsAddressResolver(new Uri(backendUrl), loggerFactory), loggerFactory);
                grpcConnection.ConfigureBalancer(c => new RoundRobinBalancer(c, loggerFactory));

                return grpcConnection;
            });

            services.AddSingleton(services =>
            {
                var grpcConnection = services.GetRequiredService<ClientChannel>();
                var loggerFactory = services.GetRequiredService<ILoggerFactory>();

                var channel = GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions
                {
                    Credentials = ChannelCredentials.Insecure,
                    LoggerFactory = loggerFactory,
                    HttpHandler = new BalancerHttpHandler(new SocketsHttpHandler(), grpcConnection)
                });

                return channel;
            });
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            else
            {
                app.UseExceptionHandler("/Error");
                // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
                app.UseHsts();
            }

            app.UseHttpsRedirection();
            app.UseStaticFiles();

            app.UseRouting();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapBlazorHub();
                endpoints.MapFallbackToPage("/_Host");
            });
        }
    }
}
