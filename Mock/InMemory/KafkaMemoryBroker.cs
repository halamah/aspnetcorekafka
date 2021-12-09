using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Client;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Mock.Abstractions;
using AspNetCore.Kafka.Options;
using AspNetCore.Kafka.Utility;
using Avro.Generic;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace AspNetCore.Kafka.Mock.InMemory
{
    internal class KafkaMemoryBroker : IKafkaMemoryBroker, IKafkaClientFactory
    {
        private readonly ConcurrentDictionary<string, IKafkaMemoryTopic<object, object>> _topics = new ();
        private readonly KafkaOptions _options;
        private readonly IKafkaMessageSerializer<string> _text;
        private readonly IKafkaMessageSerializer<GenericRecord> _avro;

        public KafkaMemoryBroker(
            IOptions<KafkaOptions> options,
            IKafkaMessageSerializer<string> text,
            IKafkaMessageSerializer<GenericRecord> avro)
        {
            _options = options.Value;
            _text = text;
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
            var parser = new KafkaMessageParser(_text, _avro);
            
            return topic.Parse(parser, selector);
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