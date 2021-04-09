using System;
using System.Collections.Concurrent;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Client.Consumer;
using AspNetCore.Kafka.Mock.InMemory;
using AspNetCore.Kafka.Options;
using Confluent.Kafka;

namespace AspNetCore.Kafka.Mock
{
    public class KafkaClientInMemoryFactory : IKafkaClientFactory
    {
        private readonly ConcurrentDictionary<(Type, Type), object> _topics = new();

        public IProducer<TKey, TValue> CreateProducer<TKey, TValue>(KafkaOptions options,
            Action<IClient, LogMessage> logHandler)
            => new InMemoryKafkaProducer<TKey, TValue>(
                (InMemoryTopicCollection<TKey, TValue>) _topics.GetOrAdd(
                    (typeof(TKey), typeof(TValue)),
                    new InMemoryTopicCollection<TKey, TValue>()));

        public IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(KafkaOptions options,
            SubscriptionConfiguration config)
            => new InMemoryKafkaConsumer<TKey, TValue>(
                (InMemoryTopicCollection<TKey, TValue>) _topics.GetOrAdd(
                    (typeof(TKey), typeof(TValue)),
                    new InMemoryTopicCollection<TKey, TValue>()));
    }
}