using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IKafkaClient
    {
        internal ILogger Log { get; }
        
        internal IEnumerable<IMessageInterceptor> Interceptors { get; }
    }
}