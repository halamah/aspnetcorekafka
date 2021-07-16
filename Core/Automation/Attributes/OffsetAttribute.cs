using System;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Options;

namespace AspNetCore.Kafka.Automation.Attributes
{
    [AttributeUsage(AttributeTargets.Method)]
    public class OffsetAttribute : MessagePolicyAttribute
    {
        public OffsetAttribute(TopicOffset offset)
        {
            Value = new MessageOffset(offset, 0);
        }
        
        public OffsetAttribute(long bias)
        {
            Value = new MessageOffset(TopicOffset.End, bias);
        }
        
        public OffsetAttribute(TopicOffset offset, long bias)
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
            else
            {
                throw new ArgumentException("Invalid date offset value: {Value}", timeExpression);
            }
        }
        
        public MessageOffset Value { get; } 
    }
}