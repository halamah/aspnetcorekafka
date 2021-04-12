using System;
using System.Threading;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.MessageBlocks
{
    public class BatchMessageBlock : IMessageBlock
    {
        private readonly ILogger _log;
        private readonly int _size;
        private readonly int _time;

        public BatchMessageBlock(ILogger<BatchMessageBlock> log, IMessageBatchOptions options)
        {
            _log = log;
            _size = options.Size > 1 ? options.Size : throw new ArgumentException("Batch size must be greater than 1");
            _time = options.Time > 0 ? options.Time : Timeout.Infinite;
        }

        public IDataflowBlock CreateBlock<T>()
        {
            var batch = new BatchBlock<IMessage<T>>(_size, new GroupingDataflowBlockOptions
            {
                EnsureOrdered = true,
                BoundedCapacity = _size
            });
            
            var timer = new Timer(_ => batch.TriggerBatch(), null, _time, Timeout.Infinite);

            var transform = new TransformBlock<IMessage<T>[], IMessageEnumerable<T>>(x =>
                {
                    timer.Change(_time, Timeout.Infinite);
                    return new KafkaMessageEnumerable<T>(x);
                },
                new ExecutionDataflowBlockOptions
                {
                    EnsureOrdered = true,
                    BoundedCapacity = 1
                });
            
                batch.LinkTo(transform);

            return DataflowBlock.Encapsulate(batch, transform);
        }

        public override string ToString()
        {
            var timeStr = _time == Timeout.Infinite ? "Unset" : _time.ToString();
            return  $"Batch(Size: {_size}, Time: {timeStr})";
        }
    }
}