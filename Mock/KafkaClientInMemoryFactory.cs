using System;
using System.Collections.Concurrent;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Mock.InMemory;
using Confluent.Kafka;

namespace AspNetCore.Kafka.Mock
{
    public class KafkaClientInMemoryFactory : IKafkaClientFactory
    {
        private readonly ConcurrentDictionary<(Type, Type), object> _topics = new();

        public IProducer<TKey, TValue> CreateProducer<TKey, TValue>()
            => new InMemoryKafkaProducer<TKey, TValue>(
                (InMemoryTopicCollection<TKey, TValue>) _topics.GetOrAdd(
                    (typeof(TKey), typeof(TValue)),
                    new InMemoryTopicCollection<TKey, TValue>()
                ));

        public IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(string topic)
            => new InMemoryKafkaConsumer<TKey, TValue>(
                ((InMemoryTopicCollection<TKey, TValue>) _topics.GetOrAdd(
                    (typeof(TKey), typeof(TValue)),
                    new InMemoryTopicCollection<TKey, TValue>()
                )).GetTopic(topic));
    }
}