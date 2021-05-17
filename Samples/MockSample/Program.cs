using System;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Mock;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace MockSample
{
    public class SampleMessage
    {
        public Guid Id { get; set; }
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

        protected override Task ExecuteAsync(CancellationToken cancellationToken)
        {
            const string topic = "test";

            _consumer.Subscribe<SampleMessage>(topic, message =>
            {
                Console.WriteLine($"Received message Id = {message.Value.Id} Offset = {message.Offset}");
                return Task.CompletedTask;
            });

            Task.Run(async () =>
                {
                    for (;;)
                    {
                        await _producer.ProduceAsync(topic, new SampleMessage {Id = Guid.NewGuid()});
                        await Task.Delay(1000, cancellationToken);
                    }
                },
                cancellationToken);

            return Task.CompletedTask;
        }
    }
    
    public class Program
    {
        private readonly IConfiguration _config;

        public static void Main(string[] args) =>
            Host.CreateDefaultBuilder()
                .UseSerilog((_, x) => x.WriteTo.Console())
                .ConfigureWebHostDefaults(webBuilder => webBuilder.UseStartup<Program>())
                .Build()
                .Run();

        public Program(IConfiguration config) => _config = config;
        
        public void ConfigureServices(IServiceCollection services)
        {
            services
                .AddHostedService<BackgroundHostedService>()
                .AddKafka(_config)
                .UseInMemoryBroker();
        }

        public void Configure(IApplicationBuilder app) { }
    }
}