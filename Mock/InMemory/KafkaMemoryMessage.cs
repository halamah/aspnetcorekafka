using AspNetCore.Kafka.Mock.Abstractions;

namespace AspNetCore.Kafka.Mock.InMemory
{
    public class KafkaMemoryMessage<TKey, TValue> : IKafkaMemoryMessage<TKey, TValue>
    {
        public TKey Key { get; set; }
        
        public TValue Value { get; set; }
    }
}