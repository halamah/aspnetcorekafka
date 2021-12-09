using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Mock.Abstractions;
using Confluent.Kafka;

namespace AspNetCore.Kafka.Mock.InMemory
{
    internal class KafkaMemoryTopic<TKey, TValue> : IKafkaMemoryTopic<TKey, TValue>
    {
        private readonly ConcurrentQueue<ConsumeResult<TKey, TValue>> _queue = new();
        private readonly AutoResetEvent _putSignal = new(false);
        private readonly AutoResetEvent _getSignal = new(false);
        private readonly TaskCompletionSource _consumedAny = new();
        private readonly ConcurrentQueue<IKafkaMemoryMessage<TKey, TValue>> _produced = new();
        private readonly ConcurrentQueue<IKafkaMemoryMessage<TKey, TValue>> _consumed = new();
            
        private long _currentOffset;

        public KafkaMemoryTopic(string name) => Name = name;

        public void Put(Message<TKey, TValue> message)
        {
            var offset = Interlocked.Increment(ref _currentOffset);
            var partitionsCount = Math.Max(PartitionsCount, 1);
            var partition = Math.Abs((message.Key?.GetHashCode() ?? 0) % partitionsCount);

            var result = new ConsumeResult<TKey, TValue>
            {
                IsPartitionEOF = false,
                Message = message,
                Offset = offset,
                Partition = partition,
                Topic = Name,
                TopicPartitionOffset = new TopicPartitionOffset(Name, partition, new Offset(offset))
            };
            
            _queue.Enqueue(result);
            _produced.Enqueue(KafkaMemoryMessage.Create(message.Key, message.Value));
            _putSignal.Set();
        }

        public ConsumeResult<TKey, TValue> GetMessage(TimeSpan timeout)
            => GetMessage(new CancellationTokenSource(timeout).Token);
        
        public ConsumeResult<TKey, TValue> GetMessage(int timeout) 
            => GetMessage(new CancellationTokenSource(timeout).Token);

        public ConsumeResult<TKey, TValue> GetMessage(CancellationToken cancellationToken)
        {
            ConsumeResult<TKey, TValue> result;
            
            while (!_queue.TryDequeue(out result))
            {
                WaitHandle.WaitAny(new[] {_putSignal, cancellationToken.WaitHandle});

                if (cancellationToken.IsCancellationRequested)
                    throw new OperationCanceledException();
            }
            
            _consumed.Enqueue(KafkaMemoryMessage.Create(result.Message.Key, result.Message.Value));
            _consumedAny.TrySetResult();
            _getSignal.Set();

            return result;
        }

        public string Name { get; }

        public int PartitionsCount { get; set; } = 1;
        
        public Task WhenConsumedAll()
        {
            while (!_queue.IsEmpty)
                _getSignal.WaitOne(100);

            return Task.CompletedTask;
        }

        public IKafkaMemoryTopic<TKey, TValue> Clear()
        {
            _produced.Clear();
            _consumed.Clear();

            return this;
        }

        public IEnumerable<IKafkaMemoryMessage<TKey, TValue>> Produced => _produced.ToImmutableList();

        public IEnumerable<IKafkaMemoryMessage<TKey, TValue>> Consumed => _consumed.ToImmutableList();

        public Task WhenConsumedAny() => _consumedAny.Task;

        public void Commit(IEnumerable<TopicPartitionOffset> offsets) { }

        public void Commit(ConsumeResult<TKey, TValue> result) { }
    }
}