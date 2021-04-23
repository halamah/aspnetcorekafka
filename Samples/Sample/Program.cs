using System;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using App.Metrics.AspNetCore;
using App.Metrics.Formatters.Prometheus;
using AspNetCore.Kafka;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Attributes;
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
    public enum SampleType
    {
        FirstSampleType,
        SecondSampleType
    }
    
    [Message(Topic = "test.topic-uat")]
    public record TestMessage
    {
        public int Index { get; set; }
        
        public Guid Id { get; set; }
        
        public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
        
        [JsonConverter(typeof(JsonStringEnumConverter))]
        public SampleType Type { get; set; }
    }

    [MessageHandler]
    public class EventHandler
        //IMessageHandler<TestMessage>
        //IMessageHandler<IMessage<TestMessage>>
    {
        private readonly ILogger<EventHandler> _log;

        public EventHandler(ILogger<EventHandler> logger) => _log = logger;

        //[Message(Offset = TopicOffset.Begin)]
        public async Task Handler(TestMessage x) => _log.LogInformation("Message/{Id}", x?.Id);
        
        //[Message(Offset = TopicOffset.Begin)]
        public async Task Handler(IMessage<TestMessage> x) => _log.LogInformation("Message/{Offset}", x?.Offset);

        //[Message]
        [Buffer(Size = 100)]
        [Batch(Size = 500, Time = 5000)]
        [Parallel(DegreeOfParallelism = 2)]
        public async Task Handler(IMessageEnumerable<TestMessage> x)
        {
            _log.LogInformation("Batch/{Count}, Partition/{Partition}", x.Count(), x.First().Partition);
            await Task.Delay(2000);
        }
        
        [Message]
        [Offset(TopicOffset.End, -100)]
        public async Task FailureHandler(TestMessage x)
        {
            throw new Exception();
        }

        public async Task HandleAsync(TestMessage x) => _log.LogInformation("Message/{Id}", x?.Id);
        
        public async Task HandleAsync(IMessage<TestMessage> x) => _log.LogInformation("Message/{Offset}", x?.Offset);
    }
    
    public class Program
    {
        private readonly IConfiguration _config;

        public static void Main(string[] args)
        {
            Host.CreateDefaultBuilder()
                .UseSerilog((context, config) =>
                {
                    config.WriteTo.Console(
                        theme: AnsiConsoleTheme.Code,
                        outputTemplate:
                        "[{Timestamp:HH:mm:ss} {Level:u3}] {SourceContext}: {Message}{NewLine}{Exception}");
                })
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
                .AddMetrics()
                //.AddAssemblies(typeof(AnotherHandler).Assembly)
                .Configure(x =>
                {
                    //*
                    x.Server = "192.168.30.173:9092,192.168.30.221:9092,192.168.30.222:9092";
                    x.SchemaRegistry = "http://10.79.65.150:8084";
                    //*/
                    
                    /*
                    x.SchemaRegistry = "http://schema-registry.eva-prod.pmcorp.loc";
                    x.Server = "10.79.128.12";
                    //*/
                });
        }

        public void Configure(IApplicationBuilder app, IKafkaProducer p)
        {
            /*
            Task.WhenAll(Enumerable.Range(0, 30000)
                .Select(x => new TestMessage {Index = x, Id = Guid.NewGuid()})
                .Select(x => p.ProduceAsync("test.topic-uat", x, x.Index.ToString())))
                .GetAwaiter()
                .GetResult(); //*/
        }
    }
}