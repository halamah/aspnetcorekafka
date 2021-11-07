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

namespace AspNetCore.Kafka.Mock.InMemory
{
    internal class KafkaMemoryBroker : IKafkaMemoryBroker, IKafkaClientFactory
    {
        private readonly ConcurrentDictionary<string, IKafkaMemoryTopic> _topics = new ();
        private readonly IKafkaEnvironment _environment;
        private readonly IKafkaMessageJsonSerializer _json;
        private readonly IKafkaMessageAvroSerializer _avro;

        public KafkaMemoryBroker(
            IKafkaEnvironment environment,
            IKafkaMessageJsonSerializer json,
            IKafkaMessageAvroSerializer avro)
        {
            _environment = environment;
            _json = json;
            _avro = avro;
        }

        public IProducer<TKey, TValue> CreateProducer<TKey, TValue>(KafkaOptions options)
            => new KafkaMemoryProducer<TKey, TValue>(this);

        public IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(KafkaOptions options, SubscriptionConfiguration config)
            => new KafkaMemoryConsumer<TKey, TValue>(this);

        public IKafkaMemoryTopic<string, string> GetTopic(string topic) => GetTopic<string, string>(topic);

        public IKafkaMemoryTopic<string, T> GetTopic<T>(Func<T, bool> selector = null)
        {
            var definition = TopicDefinition.FromType<T>();
            var topic = GetTopic(definition.Topic);
            var parser = new KafkaMessageParser(_json, _avro);
            
            return topic.Parse(parser, selector);
        }

        public IEnumerable<IKafkaMemoryTopic> Topics => _topics.Values;

        public IKafkaMemoryBroker Bounce()
        {
            _topics.Clear();
            return this;
        }

        internal KafkaMemoryTopic<TKey, TValue> GetTopic<TKey, TValue>(string topic)
            => (KafkaMemoryTopic<TKey, TValue>)_topics.GetOrAdd(
                _environment.ExpandTemplate(topic),
                x => new KafkaMemoryTopic<TKey, TValue>(x));
    }
}