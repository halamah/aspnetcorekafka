using System;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.MessageBlocks
{
    public class BufferMessageBlock : IMessageBlock
    {
        private readonly int _size;

        public BufferMessageBlock(IMessageBufferOptions options)
        {
            _size = options.Size > 1 ? options.Size : throw new ArgumentException("Batch size must be greater than 1");
        }

        public IDataflowBlock CreateBlock<T>() => new BufferBlock<IMessage<T>>(new ExecutionDataflowBlockOptions
        {
            BoundedCapacity = 1,
            EnsureOrdered = true
        });

        public override string ToString() => $"Buffer(Size: {_size})";
    }
}