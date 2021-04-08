using System;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Data
{
    public class KafkaMessage<TContract> : IMessage<TContract>
    {
        private bool _suppressCommit;
        private readonly Lazy<bool> _commit;

        public KafkaMessage(Func<bool> commit) => _commit = new Lazy<bool>(commit);

        public TContract Value { get; init; }
        
        public int Partition { get; init; }
        
        public long Offset { get; init; }
        
        public string Key { get; init; }
        
        public string Topic { get; init; }

        public bool SuppressCommit()
        {
            _suppressCommit = true;
            return !_commit.IsValueCreated;
        }

        public bool Commit(bool force = false) => _suppressCommit && !force 
            ? _commit.IsValueCreated && _commit.Value 
            : _commit.Value;  
    }
}