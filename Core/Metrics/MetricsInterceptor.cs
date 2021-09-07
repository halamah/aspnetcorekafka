using System.Linq;
using System.Threading.Tasks;
using App.Metrics;
using App.Metrics.Meter;
using App.Metrics.Timer;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Metrics
{
    public class MetricsInterceptor : IMessageInterceptor
    {
        private readonly IMetrics _metrics;

        public MetricsInterceptor(IMetrics metrics)
        {
            _metrics = metrics;
        }

        public Task ConsumeAsync(KafkaInterception interception)
        {
            var topic = interception.Messages.Select(x => x.Topic ?? "?").First();
            
            var tags = new MetricTags(
                new[] { "topic", "status" },
                new[] { topic, interception.Exception is not null ? "fail" : "success" });

            _metrics.Measure.Timer.Time(new TimerOptions
                {
                    Context = "Kafka",
                    MeasurementUnit = Unit.Events,
                    Name = "Consume",
                    DurationUnit = TimeUnit.Milliseconds,
                    RateUnit = TimeUnit.Milliseconds
                },
                tags,
                (long) (interception.Metrics?.ProcessingTime.TotalMilliseconds ?? 0));

            return Task.CompletedTask;
        }

        public Task ProduceAsync(KafkaInterception interception)
        {
            foreach (var message in interception.Messages)
            {
                _metrics.Measure.Meter.Mark(new MeterOptions
                {
                    Context = "Kafka",
                    MeasurementUnit = Unit.Events,
                    Name = "Produce",
                    Tags = new MetricTags(
                        new[] {"topic", "status"},
                        new[] {message.Topic, interception.Exception is not null ? "fail" : "success"})
                });
            }

            return Task.CompletedTask;
        }
    }
}