namespace AspNetCore.Kafka.Mock.Abstractions
{
    public interface IKafkaMemoryBroker
    {
        public IKafkaMemoryTopic GetTopic(string topic);
        
        public IKafkaMemoryTopic GetTopic<T>();
    }
}