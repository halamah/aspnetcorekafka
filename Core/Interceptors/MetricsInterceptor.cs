using System.Collections.Generic;
using System.Linq;
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

        public Task ConsumeAsync(KafkaInterception interception) => MeterAsync(interception, "Produce");

        public Task ProduceAsync(KafkaInterception interception) => MeterAsync(interception, "Produce");
        
        private Task MeterAsync(KafkaInterception interception, string name)
        {
            foreach (var message in interception.Messages)
            {
                var tags = new Dictionary<string, string>
                {
                    {"topic", message.Topic},
                    {"status", interception.Exception is not null ? "fail" : "success"}
                };
                
                var status = interception.Exception != null ? "fail" : "success";

                _metrics.Measure.Meter.Mark(new MeterOptions
                {
                    Context = "Kafka",
                    MeasurementUnit = Unit.Events,
                    Name = name,
                    Tags = new MetricTags(tags.Keys.ToArray(), tags.Values.ToArray())
                });
            }

            return Task.CompletedTask;
        }
    }
}