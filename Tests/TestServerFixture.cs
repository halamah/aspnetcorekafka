using System;
using AspNetCore.Kafka;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Mock;
using AspNetCore.Kafka.Mock.Abstractions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Tests.Mock;
using Xunit.Abstractions;

namespace Tests
{
    public abstract class TestServerFixture
    {
        private readonly TestServer _server;
        
        protected readonly ITestOutputHelper Logger;

        public TestServerFixture(IConfiguration config) { }
        
        protected TestServerFixture(ITestOutputHelper log)
        {
            var config = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();
            
            Logger = log;

            var builder = new WebHostBuilder()
                .ConfigureServices(services =>
                {
                    services
                        .AddKafka(config)
                        .UseInMemoryBroker();
                })
                .ConfigureTestServices(services =>
                {
                    services
                        .AddSingleton(log)
                        .AddTransient(typeof(ILogger<>), typeof(TestLogger<>));
                })
                .UseConfiguration(config)
                .UseStartup<Startup>();

            _server = new TestServer(builder);
        }

        protected void Log(string x)
        {
            #if (DEBUG)
                Logger.WriteLine(x);
            #endif
        }

        protected IKafkaProducer Producer => GetRequiredService<IKafkaProducer>();
        protected IKafkaConsumer Consumer => GetRequiredService<IKafkaConsumer>();
        protected IKafkaMemoryBroker Broker => GetRequiredService<IKafkaMemoryBroker>();
        protected ISubscriptionManager Manager => GetRequiredService<ISubscriptionManager>();
        
        protected T GetRequiredService<T>() => _server.Services.GetRequiredService<T>();
    }

    internal class Startup
    {
        public void ConfigureServices(IServiceCollection services) { }
        public void Configure(IApplicationBuilder app) { }
    }
}