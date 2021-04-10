using System.Threading.Tasks;

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