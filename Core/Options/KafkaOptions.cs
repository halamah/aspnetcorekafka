using System.Collections.Generic;

namespace AspNetCore.Kafka.Options
{
    public enum TopicOffset
    {
        Unset,
        Stored,
        Begin,
        End,
    }

    public class KafkaConfiguration
    {
        public string Group { get; init; }
        
        public TopicOffset Offset { get; init; } = TopicOffset.Stored;
        
        public long Bias { get; init; }

        public Dictionary<string, string> Consumer { get; set; } = new();
        
        public Dictionary<string, string> Producer { get; set; } = new();
    }
        
    public class KafkaOptions
    {
        public string Server { get; init; }

        public KafkaConfiguration Configuration { get; init; } = new();
    }
}