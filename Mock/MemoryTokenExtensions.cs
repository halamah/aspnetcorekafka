using System;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Mock.Abstractions;
using AspNetCore.Kafka.Mock.InMemory;

namespace AspNetCore.Kafka.Mock
{
    public static class MemoryTokenExtensions
    {
        public static IKafkaMemoryTopic<TKey, T> Deserialize<TKey, TValue, T>(
            this IKafkaMemoryTopic<TKey, TValue> topic,
            IKafkaMessageSerializer<TValue> serializer,
            Func<T, bool> selector = null)
            => new KafkaMemoryTopicDeserializer<TKey, TValue, T>(topic, serializer, selector);
    }
}