using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Client;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Mock.Abstractions;
using AspNetCore.Kafka.Options;
using AspNetCore.Kafka.Utility;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace AspNetCore.Kafka.Mock.InMemory
{
    internal class KafkaMemoryBroker : IKafkaMemoryBroker, IKafkaClientFactory
    {
        private readonly ConcurrentDictionary<string, IKafkaMemoryTopic<object, object>> _topics = new ();
        private readonly KafkaOptions _options;
        private readonly IServiceScopeFactory _scopeFactory;

        public KafkaMemoryBroker(IOptions<KafkaOptions> options, IServiceScopeFactory scopeFactory)
        {
            _options = options.Value;
            _scopeFactory = scopeFactory;
        }

        public IProducer<TKey, TValue> CreateProducer<TKey, TValue>(KafkaOptions options)
            => new KafkaMemoryProducer<TKey, TValue>(this);

        public IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(KafkaOptions options, SubscriptionConfiguration config)
            => new KafkaMemoryConsumer<TKey, TValue>(this);

        public IKafkaMemoryTopic<string, string> GetTopic(string topic) => GetTopic<string, string>(topic);

        public IKafkaMemoryTopic<string, T> GetTopic<T>(Func<T, bool> selector = null)
        {
            using var scope = _scopeFactory.CreateScope();
            
            var definition = TopicDefinition.FromType<T>();
            var topic = GetTopic<string, string>(definition.Topic);
            var serializer = scope.ServiceProvider.GetRequiredService<IKafkaMessageSerializer<string>>();
            
            return topic.Deserialize(serializer, selector);
        }

        public IEnumerable<IKafkaMemoryTopic<object, object>> Topics => _topics.Values;

        public IKafkaMemoryBroker Bounce()
        {
            foreach (var topic in _topics)
                topic.Value.Clear();
                    
            return this;
        }

        internal KafkaMemoryTopic<TKey, TValue> GetTopic<TKey, TValue>(string topic)
            => (KafkaMemoryTopic<TKey, TValue>)_topics.GetOrAdd(
                _options.ExpandTemplate(topic),
                x => (IKafkaMemoryTopic<object, object>) new KafkaMemoryTopic<TKey, TValue>(x));
    }
}