using System.Threading.Tasks;
using AspNetCore.Kafka.Automation.Attributes;

namespace AspNetCore.Kafka.Abstractions
{
    [MessageHandler]
    public interface IMessageEnumerableHandler<in T> : IMessageHandler
    {
        Task HandleAsync(IMessageEnumerable<T> messages);
    }
}