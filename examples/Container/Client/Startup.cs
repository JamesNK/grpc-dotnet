using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Balancer;
using Grpc.Net.Client.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Client
{
    public class ReportingLoadBalancer : LoadBalancer, ISubChannelReporter
    {
        private readonly LoadBalancer _innerLoadBalancer;

        public List<SubChannel> SubChannels { get; private set; }

        public ReportingLoadBalancer(LoadBalancer innerLoadBalancer)
        {
            _innerLoadBalancer = innerLoadBalancer;
            SubChannels = new List<SubChannel>();
        }

        public override void Close()
        {
            _innerLoadBalancer.Close();
        }

        public override void ResolverError(Exception exception)
        {
            _innerLoadBalancer.ResolverError(exception);
        }

        public override void UpdateChannelState(ChannelState state)
        {
            _innerLoadBalancer.UpdateChannelState(state);
        }

        public override void UpdateSubChannelState(SubChannel subChannel, SubChannelState state)
        {
            if (!SubChannels.Contains(subChannel))
            {
                SubChannels.Add(subChannel);
            }

            _innerLoadBalancer.UpdateSubChannelState(subChannel, state);
        }
    }

    public class ReportingLoadBalancerFactory : LoadBalancerFactory, ISubChannelReporter
    {
        private readonly LoadBalancerFactory _loadBalancerFactory;
        public ISubChannelReporter Reporter { get; private set; } = default!;

        public override string Name { get; } = "reporter";
        public List<SubChannel> SubChannels => Reporter?.SubChannels ?? new List<SubChannel>();

        public ReportingLoadBalancerFactory(LoadBalancerFactory loadBalancerFactory)
        {
            _loadBalancerFactory = loadBalancerFactory;
        }

        public override LoadBalancer Create(IChannelControlHelper channelControlHelper, ILoggerFactory loggerFactory, IDictionary<string, object> options)
        {
            var loadBalancer = new ReportingLoadBalancer(_loadBalancerFactory.Create(channelControlHelper, loggerFactory, options));
            Reporter = loadBalancer;
            return loadBalancer;
        }
    }

    public interface ISubChannelReporter
    {
        List<SubChannel> SubChannels { get; }
    }

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

            //var reportingLoadBalancer = new ReportingLoadBalancer(new RoundRobinBalancer();

            ReportingLoadBalancerFactory f = new ReportingLoadBalancerFactory(new RoundRobinBalancerFactory());

            services.AddSingleton<LoadBalancerFactory>(f);
            services.AddSingleton<ISubChannelReporter>(f);

            /*
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
            */

            services.AddSingleton(services =>
            {
                //var grpcConnection = services.GetRequiredService<ClientChannel>();
                //var loggerFactory = services.GetRequiredService<ILoggerFactory>();

                // Get the service address from appsettings.json
                var config = services.GetRequiredService<IConfiguration>();
                var backendUrl = config["BackendUrl"];

                // If no address is set then fallback to the current webpage URL
                if (string.IsNullOrEmpty(backendUrl))
                {
                    var navigationManager = services.GetRequiredService<NavigationManager>();
                    backendUrl = navigationManager.BaseUri;
                }

                var channel = GrpcChannel.ForAddress(backendUrl, new GrpcChannelOptions
                {
                    Credentials = ChannelCredentials.Insecure,
                    ServiceProvider = services,
                    ServiceConfig = new ServiceConfig { LoadBalancingConfigs = { new LoadBalancingConfig("reporter") } }
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
