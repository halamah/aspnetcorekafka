using Confluent.Kafka;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IKafkaClientFactory
    {
        IProducer<TKey, TValue> CreateProducer<TKey, TValue>();
        
        IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(string topic);
    }
}