namespace AspNetCore.Kafka.Automation.Attributes
{
    public class MessageConfigAttribute : MessagePolicyAttribute
    {
        public MessageConfigAttribute(string configString)
        {
            ConfigString = configString;
        }
        
        public string ConfigString { get; set; }
    }
}