using System.Collections;
using System.Collections.Generic;
using System.Linq;
using AspNetCore.Kafka.Client;
using AspNetCore.Kafka.Mock.Abstractions;

namespace AspNetCore.Kafka.Mock.InMemory
{
    public class KafkaMemoryMessageList<TKey, TValue> : IKafkaMemoryMessageList<TKey, TValue>
    {
        private readonly DefaultKafkaMessageParser _parser;

        public KafkaMemoryMessageList(IEnumerable<IKafkaMemoryMessage<TKey, TValue>> messages)
        {
            Messages.AddRange(messages);
        }
        
        public KafkaMemoryMessageList(DefaultKafkaMessageParser parser)
        {
            _parser = parser;
        }

        public List<IKafkaMemoryMessage<TKey, TValue>> Messages { get; } = new();

        public IEnumerator<IKafkaMemoryMessage<TKey, TValue>> GetEnumerator() => Messages.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => Messages.GetEnumerator();

        public IEnumerable<TKey> Keys => Messages.Select(x => x.Key);
        
        public IEnumerable<TValue> Values => Messages.Select(x => x.Value);

        public IKafkaMemoryMessageList<TOutKey, TOutValue> Deserialize<TOutKey, TOutValue>() => new KafkaMemoryMessageList<TOutKey, TOutValue>(Messages.Select(
            x =>
            new KafkaMemoryMessage<TOutKey, TOutValue>
            {
                Key = _parser.Parse<TOutKey>(x.Value),
                Value = _parser.Parse<TOutValue>(x.Value),
            }));

        public IKafkaMemoryMessageList<TKey, TOut> Deserialize<TOut>() => new KafkaMemoryMessageList<TKey, TOut>(Messages.Select(
            x =>
                new KafkaMemoryMessage<TKey, TOut>
                {
                    Key = x.Key,
                    Value = _parser.Parse<TOut>(x.Value),
                }));
    }
}