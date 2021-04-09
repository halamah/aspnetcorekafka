using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace AspNetCore.Kafka.Abstractions
{

    public interface IMessageHandler
    {
        
    }
    
    public interface IMessageHandler<T> : IMessageHandler
    {
        Task Handle(IMessage<T> message);
    }
}