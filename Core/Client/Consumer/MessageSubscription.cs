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
        private readonly ILogger _log;

        public MessageSubscription(
            IConsumer<TKey, TValue> consumer,
            string topic,
            CancellationTokenSource cts,
            ILogger logger)
        {
            Topic = topic;

            _consumer = consumer;
            _cts = cts;
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
    }
}