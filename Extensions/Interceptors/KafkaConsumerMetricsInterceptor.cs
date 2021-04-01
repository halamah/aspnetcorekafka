using System;
using System.Threading.Tasks;
using App.Metrics;
using App.Metrics.Meter;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Extensions.Interceptors
{
    public class KafkaConsumerMetricsInterceptor : IMessageInterceptor
    {
        private readonly IMetrics _metrics;

        public KafkaConsumerMetricsInterceptor(IMetrics metrics)
        {
            _metrics = metrics;
        }

        public Task InterceptAsync(IMessage<object> payload, Exception exception)
        {
            var status = exception != null ? "fail" : "success";
            
            _metrics.Measure.Meter.Mark(new MeterOptions
            {
                Context = "Kafka",
                MeasurementUnit = Unit.Events,
                Name = "Consumer",
                Tags = new MetricTags(new[] {"topic", "status"}, new[]{payload.Topic, status})
            });

            return Task.CompletedTask;
        }
    }
}