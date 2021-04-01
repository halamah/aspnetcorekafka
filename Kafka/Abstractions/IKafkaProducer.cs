using System;
using System.Threading.Tasks;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IKafkaProducer : IDisposable
    {
        Task ProduceAsync<T>(string topic, object key, T message);

        int Flush(TimeSpan? timeout);
    }
}