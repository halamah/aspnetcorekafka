using System;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Options;

namespace AspNetCore.Kafka.Automation.Attributes
{
    [AttributeUsage(AttributeTargets.Method)]
    public class OffsetAttribute : Attribute
    {
        public OffsetAttribute(TopicOffset offset, long bias = 0)
        {
            Value = new MessageOffset(offset, bias);
        }
        
        public OffsetAttribute(string timeExpression)
        {
            if (DateTimeOffset.TryParse(timeExpression, out var date))
            {
                Value = new MessageOffset(date);
            }
            else if (TimeSpan.TryParse(timeExpression, out var time))
            {
                Value = new MessageOffset(DateTimeOffset.UtcNow + time);
            }
        }
        
        public MessageOffset Value { get; } 
    }
}