using System;
using System.Linq;
using System.Threading.Tasks;
using AspNetCore.Kafka;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Attributes;
using AspNetCore.Kafka.Extensions.Abstractions;
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

namespace Sample
{
    public class TestBatchOptions : IMessageBatchOptions
    {
        public int Size { get; set; }
        public int Time { get; set; }
        public bool Commit { get; set; }
    }
    
    [Message(Topic = "test.topic-uat")]
    public record TestMessage
    {
        public int Id { get; set; }
        public DateTimeOffset Timestamp { get; set; }
    }

    [MessageHandler]
    public class EventHandler
    {
        private readonly ILogger<EventHandler> _log;

        public EventHandler(ILogger<EventHandler> logger) => _log = logger;

        [Message(Offset = TopicOffset.Begin)]
        [MessageBatch(Size = 10, Time = 1000, Commit = true)]
        //[MessageBatch(typeof(TestBatchOptions))]
        public async Task Batch(IMessageEnumerable<TestMessage> messages)
        {
            //await Task.Delay(100);
            _log.LogInformation("[2] Batch size {Size}  Offset {Offset}", messages.Count(), messages.Max(x => x.Offset));
        }
        
        //[Message(Offset = TopicOffset.Begin)]
        public async Task Message1(IMessage<TestMessage> message)
        {
            _log.LogInformation("[1] Message, Offset {Offset}", message?.Offset);
        }
        
        //[Message(Offset = TopicOffset.Begin, Buffer = 50)]
        public async Task Message2(IMessage<TestMessage> message)
        {
            _log.LogInformation("[2] Message, Offset {Offset}", message?.Offset);
        }
    }
    
    public class Program
    {
        private readonly IConfiguration _config;

        public static void Main(string[] args) =>
            Host.CreateDefaultBuilder()
                .UseSerilog((context, config) =>
                {
                    config.WriteTo.Console(
                        theme: AnsiConsoleTheme.Code,
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
                .Configure<TestBatchOptions>(_config.GetSection("TestBatch"))
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