using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Extensions.Abstractions
{
    public interface IMessageBatchHandler<T> : IMessageHandler
    {
        Task Handle(IMessageEnumerable<T> messages);
    }
}