using System;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.MessageBlocks;

namespace AspNetCore.Kafka.Attributes
{
    public class BatchAttribute : MessageBlockAttribute, IMessageBatchOptions
    {
        public BatchAttribute() : base(typeof(BatchMessageBlock))
        {
        }

        public BatchAttribute(Type argumentType) : base(typeof(BatchMessageBlock), argumentType)
        {
        }

        public int Size { get; set; } = 0;
        
        public int Time { get; set; } = 5;
    }
}