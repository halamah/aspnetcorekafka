using System;
using AspNetCore.Kafka.Attributes;
using AspNetCore.Kafka.Automation;
using AspNetCore.Kafka.Extensions.Converters;

namespace AspNetCore.Kafka.Extensions.Attributes
{
    public class MessageBatchAttribute : MessageConverterAttribute
    {
        public MessageBatchAttribute() : base(typeof(BatchMessageConverter))
        {
        }

        public MessageBatchAttribute(Type argumentType) : base(typeof(BatchMessageConverter), argumentType)
        {
        }

        protected override object ResolveArgument(IServiceProvider provider)
            => base.ResolveArgument(provider) ?? new MessageConverterArgument<MessageBatchAttribute>(this);

        public int Size { get; set; } = 0;
        
        public int Timeout { get; set; } = 5;
    }
}