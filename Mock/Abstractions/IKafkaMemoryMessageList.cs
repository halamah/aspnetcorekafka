using System.Collections.Generic;

namespace AspNetCore.Kafka.Mock.Abstractions
{
    public interface IKafkaMemoryMessageList<out TKey, out TValue> : IEnumerable<IKafkaMemoryMessage<TKey, TValue>>
    {
        IEnumerable<TKey> Keys { get; }

        IEnumerable<TValue> Values { get; }

        IKafkaMemoryMessageList<TOutKey, TOutValue> Deserialize<TOutKey, TOutValue>();
        
        IKafkaMemoryMessageList<TKey, TOutValue> Deserialize<TOutValue>();
    }
}