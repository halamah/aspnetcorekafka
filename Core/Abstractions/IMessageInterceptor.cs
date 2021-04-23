using System;
using System.Threading.Tasks;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageInterceptor
    {
        Task ConsumeAsync(ICommittable committable, Exception exception);

        Task ProduceAsync(string topic, object key, object message, Exception exception);
    }
}