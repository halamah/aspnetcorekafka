using System.Threading.Tasks;
using App.Metrics;
using App.Metrics.Meter;
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
            foreach (var message in interception.Messages)
            {
                _metrics.Measure.Meter.Mark(new MeterOptions
                {
                    Context = "Kafka",
                    MeasurementUnit = Unit.Events,
                    Name = name,
                    Tags = new MetricTags(
                        new[] {"topic", "status"},
                        new[] {message.Topic, interception.Exception is not null ? "fail" : "success"})
                });
            }

            return Task.CompletedTask;
        }
    }
}