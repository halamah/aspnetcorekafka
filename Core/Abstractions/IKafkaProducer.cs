using System;
using System.Threading.Tasks;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IKafkaProducer : IDisposable
    {
        Task ProduceAsync<T>(string topic, T message, string key = null);

        int Flush(TimeSpan? timeout);
    }
}