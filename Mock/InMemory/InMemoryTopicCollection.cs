using System.Collections.Concurrent;
using AspNetCore.Kafka.Mock.Abstractions;

namespace AspNetCore.Kafka.Mock.InMemory
{
    public class InMemoryTopicCollection<TKey, TValue>
    {
        private readonly IKafkaMemoryBroker _broker;
        private readonly ConcurrentDictionary<string, InMemoryTopic<TKey, TValue>> _topics = new ();

        public InMemoryTopicCollection(IKafkaMemoryBroker broker)
        {
            _broker = broker;
        }

        public InMemoryTopic<TKey, TValue> GetTopic(string name) =>
            _topics.GetOrAdd(name, x => new InMemoryTopic<TKey, TValue>(name, _broker));
    }
}