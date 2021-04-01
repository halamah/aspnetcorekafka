using System;
using System.Threading.Tasks;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageInterceptor
    {
        Task InterceptAsync(IMessage<object> payload, Exception exception);
    }
}