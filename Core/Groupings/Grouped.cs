using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Groupings
{
    internal class Grouped<T> : KafkaMessage<T>, IGroupedMessage<T>
    {
        public Grouped(IMessage<T> other, int @group) : base(() => other.Commit())
        {
            Value = other.Value;
            Partition = other.Partition;
            Offset = other.Offset;
            Key = other.Key;
            Topic = other.Topic;
            Group = @group;
        }

        public int Group { get; }
    }
}