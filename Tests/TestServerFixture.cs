using System;
using AspNetCore.Kafka;
using AspNetCore.Kafka.Mock;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;

namespace Tests
{
    public abstract class TestServerFixture
    {
        private readonly TestServer _server;
        
        protected readonly ITestOutputHelper Logger;
        protected IServiceProvider Services => _server.Services;

        public TestServerFixture(IConfiguration config)
        {
        }
        
        protected TestServerFixture(ITestOutputHelper log)
        {
            var config = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();
            
            Logger = log;
            
            var builder = new WebHostBuilder()
                .ConfigureServices(services =>
                {
                    services.AddKafka(config).UseInMemoryBroker();
                })
                .UseConfiguration(config)
                .UseStartup<Startup>();

            _server = new TestServer(builder);
        }

        protected void Log(string x) => Logger.WriteLine(x);
    }

    internal class Startup
    {
        public void ConfigureServices(IServiceCollection services) { }
        public void Configure(IApplicationBuilder app) { }
    }
}