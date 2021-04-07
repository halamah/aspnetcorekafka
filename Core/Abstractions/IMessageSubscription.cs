using System.Collections.Generic;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageSubscription
    {
        void Unsubscribe();

        IEnumerable<int> Partitions { get; }

        IEnumerable<long> CommittedOffsets { get; }

        string Topic { get; }
    }
}