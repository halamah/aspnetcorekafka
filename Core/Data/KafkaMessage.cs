using System;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Data
{
    public class KafkaMessage<TContract> : IMessage<TContract>
    {
        private readonly Lazy<bool> _commit;

        public KafkaMessage(Func<bool> commit) => _commit = new Lazy<bool>(commit);

        public TContract Value { get; init; }
        
        public int Partition { get; init; }
        
        public long Offset { get; init; }
        
        public string Key { get; init; }
        
        public string Topic { get; init; }

        public bool Commit() => _commit.Value;  
    }
}