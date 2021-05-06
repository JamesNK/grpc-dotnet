using System;
using Client.Balancer;
using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Balancer;
using Grpc.Net.Client.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

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

            var reportingFactory = new ReportingLoadBalancerFactory(new RoundRobinBalancerFactory());

            services.AddSingleton<LoadBalancerFactory>(reportingFactory);
            services.AddSingleton<ISubChannelReporter>(reportingFactory);

            services.AddSingleton(services =>
            {
                var config = services.GetRequiredService<IConfiguration>();
                var backendUrl = config["BackendUrl"];

                var channel = GrpcChannel.ForAddress(backendUrl, new GrpcChannelOptions
                {
                    Credentials = ChannelCredentials.Insecure,
                    ServiceProvider = services,
                    ServiceConfig = new ServiceConfig
                    {
                        LoadBalancingConfigs = { new LoadBalancingConfig("reporter") },
                        //MethodConfigs =
                        //{
                        //    new MethodConfig
                        //    {
                        //        Names = { MethodName.Default },
                        //        RetryPolicy = new RetryPolicy
                        //        {
                        //            MaxAttempts = 5,
                        //            InitialBackoff = TimeSpan.FromSeconds(2),
                        //            MaxBackoff = TimeSpan.FromSeconds(2),
                        //            BackoffMultiplier = 1,
                        //            RetryableStatusCodes = { StatusCode.Unavailable }
                        //        }
                        //    }
                        //}
                    }
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
