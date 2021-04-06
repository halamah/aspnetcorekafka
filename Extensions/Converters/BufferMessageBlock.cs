using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Extensions.Converters
{
    public class BufferMessageBlock
    {
        private readonly int _size;
        private readonly ILogger _log;

        public BufferMessageBlock(ILogger<BufferMessageBlock> log, dynamic arg)
        {
            _log = log;
            _size = arg.Size > 1 ? arg.Size : throw new ArgumentException("Batch size must be greater than 1");
        }

        public Func<IMessage<T>, Task> Create<T>(Func<IMessage<T>, Task> next)
        {
            var action = new ActionBlock<IMessage<T>>(async x =>
                {
                    try { await next(x); }
                    catch (Exception e) { _log.LogError(e, "Message failure"); }
                },
                new ExecutionDataflowBlockOptions
                {
                    EnsureOrdered = true,
                    BoundedCapacity = _size
                });

            return x => action.SendAsync(x);
        }

        public override string ToString() => $"Buffer(Size: {_size})";
    }
}