using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;

namespace AspNetCore.Kafka.Mock.InMemory
{
    internal class KafkaMemoryConsumer<TKey, TValue> : IConsumer<TKey, TValue>
    {
        private readonly KafkaMemoryBroker _broker;
        private KafkaMemoryTopic<TKey, TValue> _topic;

        public KafkaMemoryConsumer(KafkaMemoryBroker broker)
        {
            _broker = broker;
        }

        public void Dispose() { }

        public int AddBrokers(string brokers) => 0;

        public Handle Handle => default;

        public string Name => "KafkaConsumerMock";

        public ConsumeResult<TKey, TValue> Consume(int millisecondsTimeout)
            => _topic.GetMessage(millisecondsTimeout);

        public ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken = default)
            => _topic.GetMessage(cancellationToken);

        public ConsumeResult<TKey, TValue> Consume(TimeSpan timeout) => _topic.GetMessage(timeout);

        public void Subscribe(IEnumerable<string> topics) => throw new NotImplementedException();

        public void Subscribe(string topic) => _topic = _broker.GetTopic<TKey, TValue>(topic);

        public void Unsubscribe() { }

        public void Assign(TopicPartition partition) { }

        public void Assign(TopicPartitionOffset partition) { }

        public void Assign(IEnumerable<TopicPartitionOffset> partitions) { }

        public void Assign(IEnumerable<TopicPartition> partitions) { }

        public void IncrementalAssign(IEnumerable<TopicPartitionOffset> partitions) { }

        public void IncrementalAssign(IEnumerable<TopicPartition> partitions) { }

        public void IncrementalUnassign(IEnumerable<TopicPartition> partitions) { }

        public void Unassign() { }

        public void StoreOffset(ConsumeResult<TKey, TValue> result) { }

        public void StoreOffset(TopicPartitionOffset offset) { }

        public List<TopicPartitionOffset> Commit() => new();

        public void Commit(IEnumerable<TopicPartitionOffset> offsets) => _topic.Commit(offsets);

        public void Commit(ConsumeResult<TKey, TValue> result) => _topic.Commit(result);

        public void Seek(TopicPartitionOffset tpo) { }

        public void Pause(IEnumerable<TopicPartition> partitions) { }

        public void Resume(IEnumerable<TopicPartition> partitions) { }

        public List<TopicPartitionOffset> Committed(TimeSpan timeout) => new();

        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
            => new();

        public Offset Position(TopicPartition partition) => new();

        public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
            => new();

        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition) => new(new(), new());

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout) => new(new(), new());

        public void Close() { }

        public string MemberId { get; }
        
        public List<TopicPartition> Assignment { get; }
        
        public List<string> Subscription { get; }
        
        public IConsumerGroupMetadata ConsumerGroupMetadata { get; }
    }
}