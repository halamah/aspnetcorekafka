namespace AspNetCore.Kafka.Mock.Abstractions
{
    public interface IKafkaMemoryBroker
    {
        public long ConsumeCount { get; }
        
        public long ProduceCount { get; }
    }
}