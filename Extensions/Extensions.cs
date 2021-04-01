using System;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Extensions.Interceptors;

namespace AspNetCore.Kafka.Extensions
{
    public static class Extensions
    {
        public static KafkaServiceConfiguration AddMetrics(this KafkaServiceConfiguration configuration)
        {
            return configuration.AddConsumerInterceptor<KafkaConsumerMetricsInterceptor>();
        }
        
        public static KafkaServiceConfiguration AddConsumerInterceptor(
            this KafkaServiceConfiguration config, 
            Action<IMessage<object>> handler)
        {
            _ = handler ?? throw new ArgumentNullException(nameof(handler));

            return config.AddConsumerInterceptor(new ConsumerInterceptorAction(x =>
            {
                handler(x);
                return Task.CompletedTask;
            }));
        }
        
        public static KafkaServiceConfiguration AddConsumerInterceptor(
            this KafkaServiceConfiguration config, 
            Func<IMessage<object>, Task> handler)
        {
            return config.AddConsumerInterceptor(new ConsumerInterceptorAction(handler));
        }
    }
}