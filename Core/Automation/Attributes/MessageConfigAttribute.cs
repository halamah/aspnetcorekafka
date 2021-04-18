using System;

namespace AspNetCore.Kafka.Automation.Attributes
{
    public class MessageConfigAttribute : MessageAttribute
    {
        public MessageConfigAttribute(string messageName)
        {
            if (string.IsNullOrWhiteSpace(messageName))
                throw new ArgumentException("Invalid message configuration ");
            
            if (string.Equals(messageName, "Default", StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("Message configuration name must be other than 'Default'");
            
            MessageName = messageName;
        }
        
        public string MessageName { get; set; }
    }
}