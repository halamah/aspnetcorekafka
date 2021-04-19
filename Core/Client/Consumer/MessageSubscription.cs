using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using AspNetCore.Kafka.Abstractions;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Client.Consumer
{
    internal class MessageSubscription<TKey, TValue> : IMessageSubscription
    {
        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly Lazy<WaitHandle> _unsubscribe;

        public MessageSubscription(
            IConsumer<TKey, TValue> consumer,
            string topic,
            CancellationTokenSource cts,
            ILogger log, 
            WaitHandle shutdown)
        {
            Topic = topic;

            _consumer = consumer;
            _unsubscribe = new(() =>
            {
                log.LogInformation("Unsubscribe consumer for topic '{Topic}'", Topic);
                _consumer.Unsubscribe();
                cts.Cancel();
                return shutdown;
            });
        }

        public WaitHandle Unsubscribe() => _unsubscribe.Value;
        
        public void Dispose()
        {
            var _ = _unsubscribe.Value;
        }

        public IEnumerable<int> Partitions => _consumer.Assignment.Select(x => x.Partition.Value);

        public IEnumerable<long> CommittedOffsets => _consumer.Committed(_consumer.Assignment, TimeSpan.FromSeconds(5))
            .OrderBy(x => x.Partition.Value)
            .Select(x => x.Offset.Value)
            .ToArray();

        public string Topic { get; }
    }
}