using System;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.MessageBlocks;

namespace AspNetCore.Kafka.Attributes
{
    public class BufferAttribute : MessageBlockAttribute, IMessageBufferOptions
    {
        public BufferAttribute(Type argumentType) : base(typeof(BufferMessageBlock), argumentType)
        {
        }
        
        public BufferAttribute(int size) : base(typeof(BufferMessageBlock), null, BlockStage.Message)
        {
            Size = size;
        }

        public int Size { get; set; }
    }
}