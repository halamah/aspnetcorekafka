using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace AspNetCore.Kafka.Abstractions
{

    public interface IMessageSubscription : IDisposable
    {
        Task UnsubscribeAsync();

        IEnumerable<int> Partitions { get; }

        IEnumerable<long> CommittedOffsets { get; }

        string Topic { get; }

        event RevokeHandlerDelegate Revoke;
    }
}