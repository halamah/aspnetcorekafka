using System;
using AspNetCore.Kafka.Attributes;
using AspNetCore.Kafka.Extensions.Converters;

namespace AspNetCore.Kafka.Extensions.Attributes
{
    public class MessageBufferAttribute : MessageBlockAttribute
    {
        public MessageBufferAttribute() : base(typeof(BufferMessageBlock))
        {
        }

        public MessageBufferAttribute(Type argumentType) : base(typeof(BufferMessageBlock), argumentType)
        {
        }

        public int Size { get; set; } = 0;
    }
}