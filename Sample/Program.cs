using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
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
using Microsoft.Extensions.Logging;
using Serilog.Sinks.SystemConsole.Themes;
using Serilog;
using Serilog.Enrichers.Span;

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
        private readonly ILogger<MessageHandler> _log;

        public MessageHandler(ILogger<MessageHandler> logger) => _log = logger;

        //[Message(Offset = TopicOffset.Begin)]
        //[MessageBatch(Size = 10, Time = 1000)]
        public async Task Batch(IEnumerable<IMessage<TestMessage>> messages)
        {
            await Task.Delay(100);
            _log.LogInformation("[2] Batch size {Size}  Offset {Offset}", messages.Count(), messages.Max(x => x.Offset));
            await Task.Delay(100);
        }
        
        [Message(Offset = TopicOffset.Begin)]
        //[MessageBatch(Size = 10, Time = 1000)]
        public async Task Message1(IMessage<TestMessage> message)
        {
            using var _ = message.GetCommitDisposable();
            
            _log.LogInformation("[1] Message, Offset {Offset}", message?.Offset);
        }
        
        [Message(Offset = TopicOffset.Begin, Buffer = 50)]
        //[MessageBatch(Size = 10, Time = 1000)]
        public async Task Message2(IMessage<TestMessage> message)
        {
            _log.LogInformation("[2] Message, Offset {Offset}", message?.Offset);
        }
        
        //[Message(Offset = TopicOffset.Begin)]
        //[MessageBatch(Size = 10, Time = 1000)]
        public async Task Message(IMessage<TestMessage> message)
        {
            await Task.Delay(100);
            _log.LogInformation("[1] Message, Offset {Offset}", message?.Offset);
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
                        .Enrich.WithMachineName()
                        .Enrich.WithSpan()
                        .WriteTo.Console(theme: AnsiConsoleTheme.Code,
                            outputTemplate:
                            "[{Timestamp:HH:mm:ss} {Level:u3}] {SourceContext}: {Message}{NewLine}{Exception}");
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