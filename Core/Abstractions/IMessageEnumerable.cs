using System.Collections.Generic;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageEnumerable<out T> : IStorable, IEnumerable<IMessage<T>>
    {
    }
}