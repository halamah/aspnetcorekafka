using System;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Options;

namespace AspNetCore.Kafka.Attributes
{
    [AttributeUsage(AttributeTargets.Method|AttributeTargets.Class)]
    public class MessageAttribute : Attribute
    {
        public string Topic { get; set; }
        
        public object Id { get; set; }
        
        public TopicOffset Offset { get; set; } = TopicOffset.Unset;
        
        public long Bias { get; set; }

        public TopicFormat Format { get; set; } = TopicFormat.String;
    }
}