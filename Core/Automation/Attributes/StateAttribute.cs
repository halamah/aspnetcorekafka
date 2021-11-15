using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Automation.Attributes
{
    public class StateAttribute : MessagePolicyAttribute
    {
        public StateAttribute(MessageState state)
        {
            State = state;
        }
        
        public MessageState State { get; set; }
    }
}