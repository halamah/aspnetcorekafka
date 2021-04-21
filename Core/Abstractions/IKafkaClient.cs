using App.Metrics;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IKafkaClient
    {
        ILogger Logger { get; }
    }
}