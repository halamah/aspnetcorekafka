namespace AspNetCore.Kafka.Automation.Attributes
{
    public class ConfigAttribute : MessagePolicyAttribute
    {
        public ConfigAttribute(string configString)
        {
            ConfigString = configString;
        }
        
        public string ConfigString { get; set; }
    }
}