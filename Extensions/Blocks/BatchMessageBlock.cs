using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Extensions.Abstractions;
using AspNetCore.Kafka.Extensions.Data;
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
        private readonly bool _commit;

        public BatchMessageBlock(ILogger<BatchMessageBlock> log, IMessageBatchOptions options)
        {
            _log = log;
            _size = options.Size > 1 ? options.Size : throw new ArgumentException("Batch size must be greater than 1");
            _time = options.Time > 0 ? options.Time : Timeout.Infinite;
            _commit = options.Commit;
        }

        //public Func<IMessage<T>, Task> Create<T>(Func<IEnumerable<IMessage<T>>, Task> next) => Create<T>(next);
        
        public Func<IMessage<T>, Task> Create<T>(Func<IMessageEnumerable<T>, Task> next)
        {
            var batch = new BatchBlock<IMessage<T>>(_size, new GroupingDataflowBlockOptions
            {
                EnsureOrdered = true,
                BoundedCapacity = _size
            });
            
            Timer timer = null;
            
            var action = new ActionBlock<IMessage<T>[]>(async x =>
                {
                    var collection = new KafkaMessageEnumerable<T>(x);
                    
                    try
                    {
                        await next(collection);

                        timer?.Change(_time, Timeout.Infinite);

                    }
                    catch (Exception e)
                    {
                        _log.LogError(e, "Message failure");
                    }
                    finally
                    {
                        if (_commit)
                            collection.Commit();
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
                timer ??= new Timer(_ => batch.TriggerBatch(), null, _time, Timeout.Infinite);
                return batch.SendAsync(x);
            };
        }

        public override string ToString()
        {
            var timeStr = _time == Timeout.Infinite ? "Unset" : _time.ToString();
            return  $"Batch(Size: {_size}, Time: {timeStr}, Commit: {_commit})";
        }
    }
}