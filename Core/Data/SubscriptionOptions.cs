using AspNetCore.Kafka.Options;

namespace AspNetCore.Kafka.Data
{
    public record SubscriptionOptions
    {
        public TopicFormat Format { get; set; }
        
        public TopicOffset Offset { get; set; }
        
        public long Bias { get; set; }
        
        public int Buffer { get; set; }
    }
}