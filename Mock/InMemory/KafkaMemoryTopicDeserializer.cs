using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AspNetCore.Kafka.Client;
using AspNetCore.Kafka.Mock.Abstractions;

namespace AspNetCore.Kafka.Mock.InMemory
{
    internal class KafkaMemoryTopicDeserializer<TKey, TValue, TFinal> : IKafkaMemoryTopic<TKey, TFinal>
    {
        private readonly IKafkaMemoryTopic<TKey, TValue> _root;
        private readonly KafkaMessageParser _parser;
        private readonly Func<TFinal, bool> _selector;

        public KafkaMemoryTopicDeserializer(
            IKafkaMemoryTopic<TKey, TValue> root, 
            KafkaMessageParser parser,
            Func<TFinal, bool> selector)
        {
            _root = root;
            _parser = parser;
            _selector = selector;
        }

        public string Name => _root.Name;
        
        public int PartitionsCount
        {
            get => _root.PartitionsCount;
            set => _root.PartitionsCount = value;
        }

        public Task WhenConsumedAny() => _root.WhenConsumedAny();

        public Task WhenConsumedAll() => _root.WhenConsumedAll();

        IKafkaMemoryTopic<TKey, TFinal> IKafkaMemoryTopic<TKey, TFinal>.Clear()
        {
            _root.Clear();
            return this;
        }

        IKafkaMemoryTopic<TKey, T> IKafkaMemoryTopic<TKey, TFinal>.Parse<T>(KafkaMessageParser parser, Func<T, bool> selector) =>
            _root.Parse(parser, selector);

        public IEnumerable<IKafkaMemoryMessage<TKey, TFinal>> Produced =>
            _root.Produced
                .Select(x => KafkaMemoryMessage.Create(x.Key, _parser.Parse<TFinal>(x.Value)))
                .Where(x => _selector?.Invoke(x.Value) ?? true);
        
        public IEnumerable<IKafkaMemoryMessage<TKey, TFinal>> Consumed =>
            _root.Consumed
                .Select(x => KafkaMemoryMessage.Create(x.Key, _parser.Parse<TFinal>(x.Value)))
                .Where(x => _selector?.Invoke(x.Value) ?? true);
    }
}