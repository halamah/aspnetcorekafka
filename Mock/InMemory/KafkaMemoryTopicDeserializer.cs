using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Mock.Abstractions;

namespace AspNetCore.Kafka.Mock.InMemory
{
    internal class KafkaMemoryTopicDeserializer<TKey, TValue, TFinal> : IKafkaMemoryTopic<TKey, TFinal>
    {
        private readonly IKafkaMemoryTopic<TKey, TValue> _root;
        private readonly IMessageSerializer<TValue> _serializer;
        private readonly Func<TFinal, bool> _selector;

        public KafkaMemoryTopicDeserializer(
            IKafkaMemoryTopic<TKey, TValue> root, 
            IMessageSerializer<TValue> serializer,
            Func<TFinal, bool> selector)
        {
            _root = root;
            _serializer = serializer;
            _selector = selector;
        }

        public string Name => _root.Name;
        
        public int PartitionsCount
        {
            get => _root.PartitionsCount;
            set => _root.PartitionsCount = value;
        }

        public Task WhenConsumedAll() => _root.WhenConsumedAll();

        public IKafkaMemoryTopic<TKey, TFinal> Clear()
        {
            _root.Clear();
            return this;
        }

        public IEnumerable<IKafkaMemoryMessage<TKey, TFinal>> Produced =>
            _root.Produced
                .Select(x => KafkaMemoryMessage.Create(x.Key, _serializer.Deserialize<TFinal>(x.Value)))
                .Where(x => _selector?.Invoke(x.Value) ?? true);
        
        public IEnumerable<IKafkaMemoryMessage<TKey, TFinal>> Consumed =>
            _root.Consumed
                .Select(x => KafkaMemoryMessage.Create(x.Key, _serializer.Deserialize<TFinal>(x.Value)))
                .Where(x => _selector?.Invoke(x.Value) ?? true);
    }
}