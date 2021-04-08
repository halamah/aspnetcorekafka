using System;
using AspNetCore.Kafka.Attributes;
using AspNetCore.Kafka.Extensions.Abstractions;
using AspNetCore.Kafka.Extensions.Blocks;

namespace AspNetCore.Kafka.Extensions.Attributes
{
    public class MessageBatchAttribute : MessageBlockAttribute, IMessageBatchOptions
    {
        public MessageBatchAttribute() : base(typeof(BatchMessageBlock))
        {
        }

        public MessageBatchAttribute(Type argumentType) : base(typeof(BatchMessageBlock), argumentType)
        {
        }

        public int Size { get; set; } = 0;
        
        public int Time { get; set; } = 5;
    }
}