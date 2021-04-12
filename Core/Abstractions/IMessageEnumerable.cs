using System.Collections.Generic;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageEnumerable<out T> : IMessageOffset, IEnumerable<IMessage<T>>
    {
    }
}