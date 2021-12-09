using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using App.Metrics.AspNetCore;
using App.Metrics.Formatters.Prometheus;
using AspNetCore.Kafka;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Metrics;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;
using ILogger = Microsoft.Extensions.Logging.ILogger;

namespace Sample
{
    class Interceptor : IMessageInterceptor
    {
        private readonly ILogger _log;
        public Interceptor(ILogger<Interceptor> log) => _log = log;

        public Task ConsumeAsync(KafkaInterception interception)
        {
            _log.LogInformation(
                interception.Exception,
                "{Key}, {Topic}, {Name}, {Value}, {Group}",
                interception.Messages.First().Key, interception.Messages.First().Topic,
                interception.Messages.First().Name, interception.Messages.First().Value,
                interception.Messages.First().Group);
            
            return Task.CompletedTask;
        }

        public Task ProduceAsync(KafkaInterception interception) => Task.CompletedTask;
    }
    
    class SampleHandler : IMessageHandler
    {
        private readonly ILogger _log;
        public SampleHandler(ILogger<SampleHandler> log) => _log = log;

        [Message(Topic = "event.sample-{env}", Name = "Sample")]
        //[Config("state: enabled, batch(10, 5000), offset(stored, -100)")]
        //[State(MessageState.Disabled)]
        //[Batch(10, 5000)]
        //[Offset(TopicOffset.Begin, 0)]
        //[Retry(5, 100)]
        public Task HandleAsync(JsonDocument doc)
        {
            Console.WriteLine(doc);
            return Task.CompletedTask;
        }
    }

    public class Program
    {
        private readonly IConfiguration _config;

        public static void Main(string[] args) {
            Host.CreateDefaultBuilder()
                .UseSerilog((_, x) => x.WriteTo.Console())
                .UseMetrics(options => options.EndpointOptions = x =>
                {
                    x.MetricsEndpointOutputFormatter = new MetricsPrometheusTextOutputFormatter();
                })
                .ConfigureWebHostDefaults(webBuilder => webBuilder.UseStartup<Program>())
                .Build()
                .Run();
        }

        public Program(IConfiguration config) => _config = config;
        
        public void ConfigureServices(IServiceCollection services)
        {
            services
                .AddMetrics()
                .AddKafka(_config)
                .Subscribe(x => x.AddAssembly())
                .AddInterceptor<Interceptor>()
                .AddMetrics()
                .Configure(x => x.Server = "127.0.0.1");
        }

        public void Configure(IApplicationBuilder app) { }
    }
}