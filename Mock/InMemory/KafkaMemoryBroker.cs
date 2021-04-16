using System.Collections.Concurrent;
using AspNetCore.Kafka.Mock.Abstractions;

namespace AspNetCore.Kafka.Mock.InMemory
{
    internal class KafkaMemoryBroker : IKafkaMemoryBroker
    {
        private readonly ConcurrentDictionary<string, int> _topicPartitions = new();
            
        public long ConsumeCount { get; set; }
        
        public long ProduceCount { get; set; }
        
        public long CommitCount { get; set; }
        
        public int GetTopicPartitions(string topic)
        {
            return _topicPartitions.TryGetValue(topic, out var x) ? x : 1;
        }

        public void SetTopicPartitions(string topic, int count)
        {
            _topicPartitions.TryAdd(topic, count);
        }
    }
}