using System;
using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Automation.Attributes
{
    [AttributeUsage(AttributeTargets.Method|AttributeTargets.Class, AllowMultiple = true)]
    public class MessageAttribute : MessagePolicyAttribute
    {
        public string Name { get; set; }
        
        public string Topic { get; set; }
        
        public string Key { get; set; }

        public TopicFormat Format { get; set; }

        public override string ToString() => $"Message(Topic: '{Topic}', Name: '{Name}', Format: '{Format}', Id: '{Key}')";
    }
}