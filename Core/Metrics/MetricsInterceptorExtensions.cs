namespace AspNetCore.Kafka.Metrics
{
    public static class MetricsInterceptorExtensions
    {
        public static KafkaConfigurationBuilder AddMetrics(this KafkaConfigurationBuilder builder)
        {
            return builder.AddInterceptor<MetricsInterceptor>();
        }
    }
}