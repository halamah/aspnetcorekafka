using System.Collections.Generic;
using System.Reflection;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Automation
{
    public class SubscriptionDefinition
    {
        public string Topic { get; set; }
        
        public SourceOptions Options { get; set; }
        
        public MethodInfo MethodInfo { get; set; }
        
        public IEnumerable<MessagePolicyAttribute> Policies { get; set; }
    }
}