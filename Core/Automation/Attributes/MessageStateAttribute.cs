using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Automation.Attributes
{
    public class MessageStateAttribute : MessagePolicyAttribute
    {
        public MessageStateAttribute(MessageState state)
        {
            State = state;
        }
        
        public MessageState State { get; set; }
    }
}