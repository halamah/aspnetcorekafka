using System;
using System.Threading.Tasks;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageInterceptor
    {
        Task ConsumeAsync(IMessage message, Exception exception);
        
        Task ProduceAsync(string topic, object key, object message, Exception exception);
    }
}