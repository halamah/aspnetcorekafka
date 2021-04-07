using System;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Options;

namespace AspNetCore.Kafka.Attributes
{
    [AttributeUsage(AttributeTargets.Method|AttributeTargets.Class)]
    public class MessageAttribute : Attribute
    {
        public string Topic { get; set; }
        
        public string Id { get; set; }
        
        public TopicOffset Offset { get; set; } = TopicOffset.Unset;
        
        public long Bias { get; set; }

        public int Buffer { get; set; } = 0;

        public TopicFormat Format { get; set; } = TopicFormat.String;

        public override string ToString() =>
            $"Message(Topic: '{Topic}', Format: '{Format}', Id: '{Id}', Offset: '{Offset}', Bias: '{Bias}', Buffer: '{Buffer}')";
    }
}