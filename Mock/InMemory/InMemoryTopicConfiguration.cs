namespace AspNetCore.Kafka.Mock.InMemory
{
    public class InMemoryTopicConfiguration
    {
        public string Name { get; set; }
        
        public string PartitionsCount { get; set; }
    }
}