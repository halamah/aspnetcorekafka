using AspNetCore.Kafka.Data;
using Microsoft.Extensions.DependencyInjection;

namespace AspNetCore.Kafka.Client
{
    public class SubscriptionConfiguration
    {
        public string Topic { get; init; }

        public string Group { get; init; }
        
        public SourceOptions Options { get; init; }
        
        public IServiceScope Scope { get; init; }
    }
}