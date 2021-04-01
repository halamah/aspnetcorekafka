using AspNetCore.Kafka.Extensions.Interceptors;

namespace AspNetCore.Kafka.Extensions
{
    public static class Extensions
    {
        public static KafkaServiceConfiguration AddMetrics(this KafkaServiceConfiguration configuration)
        {
            return configuration
                .AddInterceptor<MetricsInterceptor>();
        }
    }
}