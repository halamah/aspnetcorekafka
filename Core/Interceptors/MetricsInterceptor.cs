using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using App.Metrics;
using App.Metrics.Meter;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Interceptors
{
    public class MetricsInterceptor : IMessageInterceptor
    {
        private readonly IMetrics _metrics;

        public MetricsInterceptor(IMetrics metrics)
        {
            _metrics = metrics;
        }

        public Task ConsumeAsync(ICommittable committable, Exception exception)
        {
            var tags = new Dictionary<string, string>();
            
            if(committable is IMessage message)
                tags.Add("topic", message.Topic);
            
            tags.Add("status", exception is not null ? "fail" : "success");
            
            _metrics.Measure.Meter.Mark(new MeterOptions
            {
                Context = "Kafka",
                MeasurementUnit = Unit.Events,
                Name = "Consume",
                Tags = new MetricTags(tags.Keys.ToArray(), tags.Values.ToArray())
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