using System.Collections.Concurrent;

namespace AspNetCore.Kafka.Mock.InMemory
{
    public class InMemoryTopicCollection<TKey, TValue>
    {
        private readonly ConcurrentDictionary<string, InMemoryTopic<TKey, TValue>> _topics = new ();

        public InMemoryTopic<TKey, TValue> GetTopic(string name) =>
            _topics.GetOrAdd(name, x => new InMemoryTopic<TKey, TValue>(name));
    }
}