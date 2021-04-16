using System;
using System.Collections.Concurrent;
using System.Threading;
using AspNetCore.Kafka.Mock.Abstractions;
using Confluent.Kafka;

namespace AspNetCore.Kafka.Mock.InMemory
{
    public class InMemoryTopic<TKey, TValue>
    {
        private readonly string _name;
        private readonly IKafkaMemoryBroker _broker;
        private readonly ConcurrentQueue<ConsumeResult<TKey, TValue>> _queue = new();
        private readonly AutoResetEvent _event = new(false);
            
        private long _offset;

        public InMemoryTopic(string name, IKafkaMemoryBroker broker)
        {
            _name = name;
            _broker = broker;
        }
        
        public ConsumeResult<TKey, TValue> Put(Message<TKey, TValue> message)
        {
            var offset = Interlocked.Increment(ref _offset);
            var partition = Math.Abs(message.Key?.GetHashCode() % _broker.GetTopicPartitions(_name) ?? 0);

            var result = new ConsumeResult<TKey, TValue>
            {
                IsPartitionEOF = false,
                Message = message,
                Offset = offset,
                Partition = partition,
                Topic = _name,
                TopicPartitionOffset = new TopicPartitionOffset(_name, partition, new Offset(offset))
            };
                
            _queue.Enqueue(result);
            _event.Set();
            return result;
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
                WaitHandle.WaitAny(new[] {_event, cancellationToken.WaitHandle});
                
                if (cancellationToken.IsCancellationRequested)
                    break;
            }

            return result;
        }
    }
}