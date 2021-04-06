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
        private readonly CancellationTokenSource _cts;
        private readonly AutoResetEvent _signal;
        private readonly ILogger _log;

        public MessageSubscription(
            IConsumer<TKey, TValue> consumer,
            string topic,
            CancellationTokenSource cts,
            AutoResetEvent signal,
            ILogger logger)
        {
            Topic = topic;

            _consumer = consumer;
            _cts = cts;
            _signal = signal;
            _log = logger;
        }

        public void Unsubscribe()
        {
            _log.LogInformation("Consumer {Topic} unsubscribe", Topic);
            _consumer.Unsubscribe();
            _cts.Cancel();
        }

        public IEnumerable<int> Partitions => _consumer.Assignment.Select(x => x.Partition.Value);

        public IEnumerable<long> CommittedOffsets => _consumer.Committed(_consumer.Assignment, TimeSpan.FromSeconds(5))
            .OrderBy(x => x.Partition.Value)
            .Select(x => x.Offset.Value)
            .ToArray();

        public string Topic { get; }

        public bool IsReadToEnd()
        {
            if (!_consumer.Subscription.Any())
                return true;
            
            var partitions = _consumer.Assignment;
            
            var highOffs = partitions
                .OrderBy(x => x.Partition.Value)
                .Select(x => _consumer.QueryWatermarkOffsets(x, TimeSpan.FromSeconds(5)).High)
                .ToArray();

            var currentOffs = partitions
                .OrderBy(x => x.Partition.Value)
                .Select(x => _consumer.Position(x));
            
            return partitions.Any() &&
                   Zip3(highOffs, CommittedOffsets, currentOffs)
                       .All(x => (x.committed == Offset.Unset && x.current == Offset.Unset) ||
                                 Math.Max(x.committed, x.current) >= x.high);
        }

        private static IEnumerable<(T1 high, T2 committed, T3 current)> Zip3<T1, T2, T3>(
            IEnumerable<T1> collectionHigh,
            IEnumerable<T2> collectionCommitted,
            IEnumerable<T3> collectionCurrent)
        {
            using var e1 = collectionHigh.GetEnumerator();
            using var e2 = collectionCommitted.GetEnumerator();
            using var e3 = collectionCurrent.GetEnumerator();
            while (e1.MoveNext() && e2.MoveNext() && e3.MoveNext())
                yield return (e1.Current, e2.Current, e3.Current);
        }

        public void WaitReadToEnd() => WaitReadToEnd(TimeSpan.MaxValue);

        public void WaitReadToEnd(TimeSpan timeout)
        {
            while (!IsReadToEnd())
            {
                if (timeout == TimeSpan.MaxValue)
                    _signal.WaitOne();
                else 
                    _signal.WaitOne(timeout);
            }
        }

        public CancellationToken CancellationToken => _cts.Token;
    }
}