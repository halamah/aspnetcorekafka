namespace AspNetCore.Kafka.Metrics
{
    public static class MetricsInterceptorExtensions
    {
        public static ConfigurationBuilder AddMetrics(this ConfigurationBuilder configurationBuilder)
        {
            return configurationBuilder.AddInterceptor<MetricsInterceptor>();
        }
    }
}