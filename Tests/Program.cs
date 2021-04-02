using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AspNetCore.Kafka;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Attributes;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Extensions.Attributes;
using AspNetCore.Kafka.Options;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Tests
{
    [Message(Topic = "event.player.profile-STAGE", Offset = TopicOffset.Begin)]
    public class MirrorDocumentItem
    {
    }
    
    [Message]
    public class Program
    {
        private readonly IConfiguration _config;

        public static void Main(string[] args)
            => Host.CreateDefaultBuilder()
                .ConfigureWebHostDefaults(webBuilder => webBuilder.UseStartup<Program>())
                .Build()
                .Run();
        
        public Program(IConfiguration config) => _config = config;
        
        [Message]
        [MessageBatch(Latency = 15)]
        public Task Handle(IEnumerable<IMessage<MirrorDocumentItem>> message)
        {
            Console.WriteLine($"Batch {message.Count()}");
            return Task.CompletedTask;
        }

        public void ConfigureServices(IServiceCollection services)
        {
            services
                .AddKafka(_config);
        }

        public void Configure(IApplicationBuilder app)
        {
            
        }
    }
}