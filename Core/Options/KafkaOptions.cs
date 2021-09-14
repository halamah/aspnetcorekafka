using System.Collections.Generic;

namespace AspNetCore.Kafka.Options
{
    public enum TopicOffset
    {
        Stored,
        StoredOrEnd,
        Begin,
        End,
    }

    public class KafkaConfiguration
    {
        public const string GroupId = "group.id";
        public const string BootstrapServers = "bootstrap.servers";
        
        public Dictionary<string, string> Consumer { get; set; } = new();
        
        public Dictionary<string, string> Producer { get; set; } = new();

        public Dictionary<string, string> ClientCommon { get; set; } = new();
    }
        
    public class KafkaOptions
    {
        public string SchemaRegistry { get; set; }
        
        public string Server { get; set; }

        public KafkaConfiguration Configuration { get; set; } = new();
    }
}