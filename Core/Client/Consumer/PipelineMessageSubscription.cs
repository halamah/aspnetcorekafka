using System;
using System.Collections.Generic;
using System.Threading;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Client.Consumer
{
    internal class PipelineMessageSubscription<TKey, TValue> : IMessageSubscription
    {
        private readonly Lazy<WaitHandle> _unsubscribe;
        private readonly IMessageSubscription _subscription;

        public PipelineMessageSubscription(IMessageSubscription subscription)
        {
            _subscription = subscription;
            _unsubscribe = new(() => _subscription.Unsubscribe());
        }

        public void Dispose()
        {
            var _ = _unsubscribe.Value;
        }

        public WaitHandle Unsubscribe()
        {
            var handle = _unsubscribe.Value;
            return handle;
        }

        public IEnumerable<int> Partitions => _subscription.Partitions;

        public IEnumerable<long> CommittedOffsets => _subscription.CommittedOffsets;

        public string Topic => _subscription.Topic;
    }
}