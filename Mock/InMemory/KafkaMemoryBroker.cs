using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Client;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Mock.Abstractions;
using AspNetCore.Kafka.Options;
using Confluent.Kafka;

namespace AspNetCore.Kafka.Mock.InMemory
{
    internal class KafkaMemoryBroker : IKafkaMemoryBroker, IKafkaClientFactory
    {
        private readonly ConcurrentDictionary<string, IKafkaMemoryTopic> _topics = new ();
        private readonly IServiceProvider _provider;

        public KafkaMemoryBroker(IServiceProvider provider)
        {
            _provider = provider;
        }

        public IProducer<TKey, TValue> CreateProducer<TKey, TValue>(KafkaOptions options)
            => new KafkaMemoryProducer<TKey, TValue>(this);

        public IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(KafkaOptions options, SubscriptionConfiguration config)
            => new KafkaMemoryConsumer<TKey, TValue>(this);

        public IKafkaMemoryTopic GetTopic(string topic) => GetTopic<string, string>(topic);
        
        public IKafkaMemoryTopic GetTopic<T>() => GetTopic(TopicDefinition.FromType<T>().Topic);

        public IEnumerable<IKafkaMemoryTopic> Topics => _topics.Values;

        public void Bounce() => _topics.Clear();

        internal KafkaMemoryTopic<TKey, TValue> GetTopic<TKey, TValue>(string topic)
            => (KafkaMemoryTopic<TKey, TValue>) _topics.GetOrAdd(topic, x => new KafkaMemoryTopic<TKey, TValue>(x, _provider));
    }
}