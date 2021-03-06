using System;
using System.Threading.Tasks;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IKafkaProducer : IKafkaClient, IDisposable
    {
        internal Task ProduceInternalAsync<T>(string topic, T message, string key = null);

        int Flush(TimeSpan? timeout);
    }
}