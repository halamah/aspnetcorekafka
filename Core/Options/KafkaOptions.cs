using System;
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
        public string Group { get; set; } = Environment.MachineName;
        
        public TopicOffset Offset { get; set; } = TopicOffset.Stored;
        
        public long Bias { get; set; }

        public Dictionary<string, string> Consumer { get; set; } = new();
        
        public Dictionary<string, string> Producer { get; set; } = new();
    }
        
    public class KafkaOptions
    {
        public string SchemaRegistry { get; set; }
        
        public string Server { get; set; }

        public KafkaConfiguration Configuration { get; init; } = new();
    }
}