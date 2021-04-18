using System.Threading.Tasks;
using AspNetCore.Kafka.Automation.Attributes;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageHandler
    {
    }
    
    [MessageHandler]
    public interface IMessageHandler<in T> : IMessageHandler
    {
        Task HandleAsync(T message);
    }
}