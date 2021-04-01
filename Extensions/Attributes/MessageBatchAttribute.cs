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

        protected override object ResolveOptions(IServiceProvider provider)
            => base.ResolveOptions(provider) ?? new MessageConverterArgument<MessageBatchAttribute>(this);

        public long Capacity { get; set; } = 0;
        
        public long Latency { get; set; } = 5;
    }
}