using System.Collections.Generic;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Extensions.Abstractions
{
    public interface IMessageEnumerable<out T> : IMessageOffset, IEnumerable<IMessage<T>>
    {
    }
}