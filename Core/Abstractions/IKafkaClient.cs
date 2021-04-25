using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IKafkaClient
    {
        ILogger Log { get; }
        
        IEnumerable<IMessageInterceptor> Interceptors { get; }
    }
}