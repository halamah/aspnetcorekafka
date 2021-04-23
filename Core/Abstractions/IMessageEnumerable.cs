using System.Collections.Generic;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageEnumerable<out T> : ICommittable, IEnumerable<IMessage<T>>
    {
    }
}