using AspNetCore.Kafka.Mock.Abstractions;

namespace AspNetCore.Kafka.Mock.InMemory
{
    internal class KafkaMemoryBroker : IKafkaMemoryBroker
    {
        public long ConsumeCount { get; set; }
        
        public long ProduceCount { get; set; }
        
        public long CommitCount { get; set; }
    }
}