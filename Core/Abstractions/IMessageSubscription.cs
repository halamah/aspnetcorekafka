using System;
using System.Collections.Generic;
using System.Threading;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageSubscription : IDisposable
    {
        WaitHandle Unsubscribe();

        IEnumerable<int> Partitions { get; }

        IEnumerable<long> CommittedOffsets { get; }

        string Topic { get; }
    }
}