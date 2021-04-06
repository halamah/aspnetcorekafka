using System;
using AspNetCore.Kafka.Attributes;
using AspNetCore.Kafka.Extensions.Converters;

namespace AspNetCore.Kafka.Extensions.Attributes
{
    public class MessageBatchAttribute : MessageBlockAttribute
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