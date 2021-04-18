using System.Collections.Generic;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Groups
{
    internal class GroupedMessage<T> : KafkaMessage<T>, IGroupedMessage<T>
    {
        public GroupedMessage(IMessage<T> other, int @group) : base(() => other.Commit())
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

    internal class GroupedMessageEnumerable<T> : KafkaMessageEnumerable<T>, IGroupedMessageEnumerable<T>
    {
        public int Group { get; }

        public GroupedMessageEnumerable(IEnumerable<IMessage<T>> collection, int @group) : base(collection)
        {
            Group = @group;
        }
    }
}