namespace AspNetCore.Kafka.Data
{
    public record SourceOptions
    {
        public TopicFormat Format { get; set; }
        
        public MessageOffset Offset { get; set; }
    }
}