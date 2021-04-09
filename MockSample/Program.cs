using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Attributes;
using AspNetCore.Kafka.Extensions.Abstractions;
using AspNetCore.Kafka.Extensions.Attributes;
using AspNetCore.Kafka.Mock;
using AspNetCore.Kafka.Options;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog.Sinks.SystemConsole.Themes;
using Serilog;

namespace Sample
{
    public class SampleMessage
    {
        public int Id { get; set; }
    }
    
    public class BackgroundHostedService : BackgroundService
    {
        private readonly IKafkaProducer _producer;
        private readonly IKafkaConsumer _consumer;

        public BackgroundHostedService(IKafkaProducer producer, IKafkaConsumer consumer)
        {
            _producer = producer;
            _consumer = consumer;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            const string topic = "test";

            _consumer.Subscribe<SampleMessage>(topic, message =>
            {
                Console.WriteLine($"Received message {message.Offset}");
                return Task.CompletedTask;
            });
                
            Task.Run(async () =>
            {
                for (var i = 1;; )
                {
                    await _producer.ProduceAsync(topic, null, new SampleMessage {Id = i});
                    await Task.Delay(1000, stoppingToken);
                }
            });

            return Task.CompletedTask;
        }
    }
    
    public class Program
    {
        private readonly IConfiguration _config;

        public static void Main(string[] args) =>
            Host.CreateDefaultBuilder()
                .UseSerilog((context, config) =>
                {
                    config
                        .Enrich.FromLogContext()
                        .WriteTo.Console(theme: AnsiConsoleTheme.Code,
                            outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {SourceContext}: {Message}{NewLine}{Exception}");
                })
                .ConfigureWebHostDefaults(webBuilder => webBuilder.UseStartup<Program>())
                .Build()
                .Run();

        public Program(IConfiguration config)
        {
            _config = config;
        }
        
        public void ConfigureServices(IServiceCollection services)
        {
            services
                .AddHostedService<BackgroundHostedService>()
                .AddKafka(_config)
                .UseInMemoryProvider();
        }

        public void Configure(IApplicationBuilder app)
        {
        }
    }
}