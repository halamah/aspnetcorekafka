using System.Collections.Generic;
using System.Threading.Tasks;

namespace AspNetCore.Kafka.Mock.Abstractions
{
    public interface IKafkaMemoryTopic<out TKey, out TValue>
    {
        string Name { get; }

        int PartitionsCount { get; set; }

        Task WhenConsumedAll();
        
        IKafkaMemoryTopic<TKey, TValue> Clear();

        IEnumerable<IKafkaMemoryMessage<TKey, TValue>> Produced { get; }

        IEnumerable<IKafkaMemoryMessage<TKey, TValue>> Consumed { get; }
    }
}