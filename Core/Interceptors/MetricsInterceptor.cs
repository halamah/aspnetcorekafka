using System.Linq;
using System.Threading.Tasks;
using App.Metrics;
using App.Metrics.Meter;
using App.Metrics.Timer;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Interceptors
{
    public class MetricsInterceptor : IMessageInterceptor
    {
        private readonly IMetrics _metrics;

        public MetricsInterceptor(IMetrics metrics)
        {
            _metrics = metrics;
        }

        public Task ConsumeAsync(KafkaInterception interception) => MeterAsync(interception, "Consume");

        public Task ProduceAsync(KafkaInterception interception) => MeterAsync(interception, "Produce");
        
        private Task MeterAsync(KafkaInterception interception, string name)
        {
            var topic = interception.Messages.Select(x => x.Topic ?? "?").First();
            
            var tags = new MetricTags(
                new[] { "topic", "status" },
                new[] { topic, interception.Exception is not null ? "fail" : "success" });

            _metrics.Measure.Timer.Time(new TimerOptions
                {
                    Context = "Kafka",
                    MeasurementUnit = Unit.Events,
                    Name = name,
                    DurationUnit = TimeUnit.Milliseconds,
                    RateUnit = TimeUnit.Milliseconds
                },
                tags,
                (long) interception.Metrics.ProcessingTime.TotalMilliseconds);

            return Task.CompletedTask;
        }
    }
}