namespace AspNetCore.Kafka.Metrics
{
    public static class MetricsInterceptorExtensions
    {
        public static KafkaServiceConfiguration AddMetrics(this KafkaServiceConfiguration configuration)
        {
            return configuration.AddInterceptor<MetricsInterceptor>();
        }
    }
}