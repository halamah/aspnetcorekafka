using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AspNetCore.Kafka;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Attributes;
using AspNetCore.Kafka.Extensions.Attributes;
using AspNetCore.Kafka.Options;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Sample
{
    [Message(Topic = "test.topic-uat")]
    public record TestMessage
    {
        public int Id { get; set; }
        public DateTimeOffset Timestamp { get; set; }
    }

    [Message]
    public class MessageHandler
    {
        [Message(Offset = TopicOffset.Begin)]
        [MessageBatch(Size = 100, Timeout = 1000)]
        public Task Handle(IEnumerable<IMessage<TestMessage>> messages)
        {
            Console.WriteLine($"Batch size {messages.Count()}, Offset {messages.Max(x => x.Offset)}");
            return Task.CompletedTask;
        }
    }
    
    public class Program
    {
        private readonly IConfiguration _config;

        public static void Main(string[] args) =>
            Host.CreateDefaultBuilder()
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
                .AddKafka(_config)
                .Configure(x =>
                {
                    x.Server = "192.168.30.173:9092,192.168.30.221:9092,192.168.30.222:9092";
                    x.SchemaRegistry = "http://10.79.65.150:8084";
                });
        }

        public void Configure(IApplicationBuilder app)
        {
        }
    }
}