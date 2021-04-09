using System;
using System.Collections.Concurrent;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Client.Consumer;
using AspNetCore.Kafka.Mock.Abstractions;
using AspNetCore.Kafka.Mock.InMemory;
using AspNetCore.Kafka.Options;
using Confluent.Kafka;

namespace AspNetCore.Kafka.Mock
{
    public class KafkaClientInMemoryFactory : IKafkaClientFactory
    {
        private readonly ConcurrentDictionary<(Type, Type), object> _topics = new();
        private readonly IKafkaMemoryBroker _broker;

        public KafkaClientInMemoryFactory(IKafkaMemoryBroker broker)
        {
            _broker = broker;
        }

        public IProducer<TKey, TValue> CreateProducer<TKey, TValue>(KafkaOptions options,
            Action<IClient, LogMessage> logHandler)
            => new InMemoryKafkaProducer<TKey, TValue>(
                _broker,
                (InMemoryTopicCollection<TKey, TValue>) _topics.GetOrAdd(
                    (typeof(TKey), typeof(TValue)),
                    new InMemoryTopicCollection<TKey, TValue>()));

        public IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(KafkaOptions options,
            SubscriptionConfiguration config)
            => new InMemoryKafkaConsumer<TKey, TValue>(
                _broker,
                (InMemoryTopicCollection<TKey, TValue>) _topics.GetOrAdd(
                    (typeof(TKey), typeof(TValue)),
                    new InMemoryTopicCollection<TKey, TValue>()));
    }
}