using AspNetCore.Kafka.Options;

namespace AspNetCore.Kafka.Data
{
    public class SubscriptionOptions
    {
        public TopicOffset Offset { get; set; }
        
        public long? Bias { get; set; }
        
        public TopicFormat TopicFormat { get; set; }
    }
}