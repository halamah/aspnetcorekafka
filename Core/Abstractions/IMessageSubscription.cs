using System;
using System.Collections.Generic;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageSubscription : IDisposable
    {
        void Unsubscribe();

        IEnumerable<int> Partitions { get; }

        IEnumerable<long> CommittedOffsets { get; }

        string Topic { get; }
    }
}