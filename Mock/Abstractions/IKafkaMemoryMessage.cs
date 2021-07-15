namespace AspNetCore.Kafka.Mock.Abstractions
{
    public interface IKafkaMemoryMessage<out TKey, out TValue>
    {
        TKey Key { get; }
        
        TValue Value { get; }
    }
}