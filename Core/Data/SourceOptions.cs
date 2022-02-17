using System.Threading;

namespace AspNetCore.Kafka.Data
{
    public record SourceOptions
    {
        public TopicFormat Format { get; set; }

        public MessageOffset Offset { get; set; } = new();
        
        public string Name { get; set; }

        internal CancellationTokenSource CancellationToken { get; init; } = new();
    }
}