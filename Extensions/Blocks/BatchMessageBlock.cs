using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MoreLinq.Extensions;

namespace AspNetCore.Kafka.Extensions.Blocks
{
    public class BatchMessageBlock
    {
        private readonly ILogger _log;
        private readonly int _size;
        private readonly int _time;
        private readonly bool _manual;

        public BatchMessageBlock(ILogger<BatchMessageBlock> log, IOptions<KafkaOptions> options, dynamic arg)
        {
            _log = log;
            _size = arg.Size > 1 ? arg.Size : throw new ArgumentException("Batch size must be greater than 1");
            _time = arg.Time > 0 ? arg.Time : Timeout.Infinite;
            _manual = options.Value.IsManualCommit();
        }

        public Func<IMessage<T>, Task> Create<T>(Func<IEnumerable<IMessage<T>>, Task> next)
        {
            var batch = new BatchBlock<IMessage<T>>(_size, new GroupingDataflowBlockOptions
            {
                EnsureOrdered = true,
                BoundedCapacity = _size
            });
            
            Timer timer = null;
            
            var action = new ActionBlock<IMessage<T>[]>(async x =>
                {
                    try
                    {
                        await next(x);

                        if (_manual)
                            x.OrderByDescending(m => m.Offset).DistinctBy(m => m.Partition).ForEach(m => m.Commit(true));
                        
                        timer?.Change(_time, Timeout.Infinite);
                        
                    }
                    catch (Exception e)
                    {
                        _log.LogError(e, "Message failure");
                    }
                },
                new ExecutionDataflowBlockOptions
                {
                    EnsureOrdered = true,
                    BoundedCapacity = 1
                });

            batch.LinkTo(action);

            return x =>
            {
                timer ??= new(_ => batch.TriggerBatch(), null, _time, Timeout.Infinite);
                x.SuppressCommit();
                return batch.SendAsync(x);
            };
        }

        public override string ToString()
        {
            var timeStr = _time == Timeout.Infinite ? "Unset" : _time.ToString();
            return  $"Batch(Size: {_size}, Time: {timeStr})";
        }
    }
}