using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Options;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageDefinition
    {
        string Topic { get; }
        
        string Id { get; }
        
        TopicOffset Offset { get; }
        
        long Bias { get; }

        int Buffer { get; }

        TopicFormat Format { get; }
    }
}