using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Attributes;

namespace AspNetCore.Kafka.Extensions.Abstractions
{
    [MessageHandler]
    public interface IMessageEnumerableHandler<in T> : IMessageHandler
    {
        Task HandleAsync(IMessageEnumerable<T> messages);
    }
}