namespace AspNetCore.Kafka.Mock.Abstractions
{
    public interface IKafkaMemoryBroker
    {
        public long ConsumeCount { get; }
        
        public long ProduceCount { get; }
        
        public long CommitCount { get; }

        public int GetTopicPartitions(string topic);
        
        public void SetTopicPartitions(string topic, int count);
    }
}