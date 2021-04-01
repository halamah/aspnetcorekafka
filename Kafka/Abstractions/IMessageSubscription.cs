using System;
using System.Collections.Generic;
using System.Threading;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageSubscription
    {
        void Unsubscribe();

        IEnumerable<int> Partitions { get; }

        IEnumerable<long> CommittedOffsets { get; }

        string Topic { get; }
        
        bool IsReadToEnd();

        void WaitReadToEnd();
        
        void WaitReadToEnd(TimeSpan timeout);
        
        CancellationToken CancellationToken { get; }
    }
}