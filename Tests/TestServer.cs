using System.Linq;
using AspNetCore.Kafka;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Mock;
using AspNetCore.Kafka.Mock.Abstractions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Tests.Mock;
using Xunit.Abstractions;

namespace Tests
{
    public class TestServer : WebApplicationFactory<Startup>
    {
        public ITestOutputHelper Output { get; private set; }

        public TestServer SetOutput(ITestOutputHelper output)
        {
            Output = output;
            return this;
        }
        
        protected override IHostBuilder CreateHostBuilder() => Host.CreateDefaultBuilder();
        
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            var config = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();
            
            builder
                .UseEnvironment("test")
                .UseStartup<Startup>()
                .ConfigureServices(services =>
                {
                    services
                        .AddSingleton(Output)
                        .AddSingleton(typeof(ILogger<>), typeof(TestLogger<>))
                        .AddKafka(config)
                        .AddInterceptor<TestInterceptor>()
                        .UseInMemoryBroker();
                })
                .ConfigureTestServices(services =>
                {
                    services
                        .AddTransient(typeof(ILogger<>), typeof(TestLogger<>));
                });
        }

        protected IKafkaProducer Producer => GetRequiredService<IKafkaProducer>();
        protected IKafkaConsumer Consumer => GetRequiredService<IKafkaConsumer>();
        protected IKafkaMemoryBroker Broker => GetRequiredService<IKafkaMemoryBroker>();
        protected ISubscriptionManager Manager => GetRequiredService<ISubscriptionManager>();

        public TestInterceptor Interceptor =>
            Services.GetServices<IMessageInterceptor>().Cast<TestInterceptor>().First();
        
        protected T GetRequiredService<T>() => Services.GetRequiredService<T>();
    }

    public class Startup
    {
        public void ConfigureServices(IServiceCollection services) { }
        public void Configure(IApplicationBuilder app) { }
    }
}