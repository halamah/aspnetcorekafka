using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Client
{
    internal class MessageSubscription<TKey, TValue> : IMessageSubscription
    {
        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly RevokeHandler _revokeHandler;
        private readonly Lazy<Task> _unsubscribe;

        public MessageSubscription(
            IConsumer<TKey, TValue> consumer,
            RevokeHandler revokeHandler,
            string topic,
            CancellationTokenSource cts,
            ILogger log,
            Task readerCompleted)
        {
            Topic = topic;

            _revokeHandler = revokeHandler;
            _consumer = consumer;
            _unsubscribe = new(() =>
            {
                log.LogInformation("Unsubscribe consumer {Consumer} for topic '{Topic}'", consumer.Name, Topic);
                _consumer.Unsubscribe();
                cts.Cancel();
                return readerCompleted;
            });
        }

        public Task UnsubscribeAsync() => _unsubscribe.Value;
        
        public void Dispose() => _unsubscribe.Value.GetAwaiter().GetResult();

        public IEnumerable<int> Partitions => _consumer.Assignment.Select(x => x.Partition.Value);

        public IEnumerable<long> CommittedOffsets => _consumer.Committed(_consumer.Assignment, TimeSpan.FromSeconds(5))
            .OrderBy(x => x.Partition.Value)
            .Select(x => x.Offset.Value)
            .ToArray();

        public string Topic { get; }
        
        public event RevokeHandlerDelegate Revoke
        {
            add => _revokeHandler.Revoke += value;
            remove => _revokeHandler.Revoke -= value;
        }
    }
}