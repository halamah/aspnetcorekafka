using System;
using System.Collections.Generic;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Data
{
    public class KafkaMessage<TContract> : IMessage<TContract>
    {
        private readonly Lazy<bool> _commit;
        private readonly Lazy<bool> _store;

        public KafkaMessage(Func<bool> commit, Func<bool> store)
        {
            _commit = new Lazy<bool>(commit);
            _store = new Lazy<bool>(store);
        }

        public TContract Value { get; init; }

        public string Name { get; init; }
        
        public int Partition { get; init; }
        
        public long Offset { get; init; }
        
        public string Key { get; init; }
        
        public string Topic { get; init; }
        
        public string Group { get; init; }

        public object GetValue() => Value;

        public bool Commit() => _commit.Value;
        
        public bool Store() => _store.Value;

        public IEnumerable<IMessage> Messages => new[] {this};
    }
}