using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Client;
using AspNetCore.Kafka.Mock.Abstractions;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;

namespace AspNetCore.Kafka.Mock.InMemory
{
    public class KafkaMemoryTopic<TKey, TValue> : IKafkaMemoryTopic
    {
        private readonly ConcurrentQueue<ConsumeResult<TKey, TValue>> _queue = new();
        private readonly AutoResetEvent _putSignal = new(false);
        private readonly AutoResetEvent _getSignal = new(false);
        private readonly TaskCompletionSource _consumedAny = new();
        private readonly KafkaMemoryMessageList<object, object> _produced;
        private readonly KafkaMemoryMessageList<object, object> _consumed;
            
        private long _currentOffset;

        public KafkaMemoryTopic(string name, IServiceProvider provider)
        {
            Name = name;
            
            var parser = new DefaultKafkaMessageParser(
                provider.GetRequiredService<IKafkaMessageJsonSerializer>(), 
                provider.GetRequiredService<IKafkaMessageAvroSerializer>());

            _produced = new(parser);
            _consumed = new(parser);
        }

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
            _produced.Messages.Add(new KafkaMemoryMessage<object, object>
            {
                Key = message.Key,
                Value = message.Value,
            });
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
            
            _consumed.Messages.Add(new KafkaMemoryMessage<object, object>
            {
                Key = result.Message.Key,
                Value = result.Message.Value,
            });
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

        public IKafkaMemoryMessageList<object, object> Produced => _produced;

        public IKafkaMemoryMessageList<object, object> Consumed => _consumed;

        public Task WhenConsumedAny() => _consumedAny.Task;

        public void Commit(IEnumerable<TopicPartitionOffset> offsets) { }

        public void Commit(ConsumeResult<TKey, TValue> result) { }
    }
}