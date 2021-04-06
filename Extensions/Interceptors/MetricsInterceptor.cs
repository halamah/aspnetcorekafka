using System;
using System.Threading.Tasks;
using App.Metrics;
using App.Metrics.Meter;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Extensions.Interceptors
{
    public class MetricsInterceptor : IMessageInterceptor
    {
        private readonly IMetrics _metrics;

        public MetricsInterceptor(IMetrics metrics)
        {
            _metrics = metrics;
        }

        public Task ConsumeAsync(IMessage<object> message, Exception exception)
        {
            var status = exception != null ? "fail" : "success";
            
            _metrics.Measure.Meter.Mark(new MeterOptions
            {
                Context = "Kafka",
                MeasurementUnit = Unit.Events,
                Name = "Consume",
                Tags = new MetricTags(new[] {"topic", "status"}, new[]{message.Topic, status})
            });

            return Task.CompletedTask;
        }

        public Task ProduceAsync(string topic, object key, object message, Exception exception)
        {
            var status = exception != null ? "fail" : "success";
            
            _metrics.Measure.Meter.Mark(new MeterOptions
            {
                Context = "Kafka",
                MeasurementUnit = Unit.Events,
                Name = "Produce",
                Tags = new MetricTags(new[] {"topic", "status"}, new[]{topic, status})
            });

            return Task.CompletedTask;
        }
    }
}