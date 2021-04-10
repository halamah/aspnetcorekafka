using System.Threading.Tasks;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageHandler
    {
    }
    
    public interface IMessageHandler<in T> : IMessageHandler
    {
        Task Handle(IMessage<T> message);
    }
}