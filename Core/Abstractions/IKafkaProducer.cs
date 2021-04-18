using System;
using System.Threading.Tasks;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IKafkaProducer : IDisposable
    {
        internal Task ProduceAsync<T>(string topic, T message, string key = null);

        int Flush(TimeSpan? timeout);
    }
}