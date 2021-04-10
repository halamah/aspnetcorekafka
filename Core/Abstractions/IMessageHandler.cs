using System.Threading.Tasks;
using AspNetCore.Kafka.Attributes;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageHandler
    {
    }
    
    [MessageHandler]
    public interface IMessageHandler<in T> : IMessageHandler
    {
        Task HandleAsync(IMessage<T> message);
    }
}