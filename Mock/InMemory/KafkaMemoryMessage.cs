using AspNetCore.Kafka.Mock.Abstractions;

namespace AspNetCore.Kafka.Mock.InMemory
{
    internal static class KafkaMemoryMessage
    {
        public static KafkaMemoryMessage<TK, TV> Create<TK, TV>(TK key, TV value) => new(key, value);
    }
    
    internal class KafkaMemoryMessage<TKey, TValue> : IKafkaMemoryMessage<TKey, TValue>
    {
        public KafkaMemoryMessage() { }

        public KafkaMemoryMessage(TKey key, TValue value)
        {
            Key = key;
            Value = value;
        }

        public TKey Key { get; set; }
        
        public TValue Value { get; set; }
    }
}