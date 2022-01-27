using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Data
{
    public class KafkaMessageEnumerable<T> : IMessageEnumerable<T>, IEnumerable<T>
    {
        private readonly IEnumerable<IMessage<T>> _collection;
        private readonly Lazy<bool> _commit;
        private readonly Lazy<bool> _store;

        public KafkaMessageEnumerable(IEnumerable<IMessage<T>> collection)
        {
            _collection = collection;
            _commit = new Lazy<bool>(() => ForLatestOffset(x => x.Commit()));
            _store = new Lazy<bool>(() => ForLatestOffset(x => x.Store()));
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator() => _collection.Select(x => x.Value).GetEnumerator();

        public IEnumerator<IMessage<T>> GetEnumerator() => _collection.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _collection.GetEnumerator();

        public bool Commit() => _commit.Value;
        
        public bool Store() => _store.Value;

        public IEnumerable<IMessage> Messages => this;

        private bool ForLatestOffset(Func<IMessage<T>, bool> action)
            => _collection
                .GroupBy(x => x.Partition)
                .Select(x => x.OrderByDescending(m => m.Offset).FirstOrDefault())
                .Where(x => x is not null)
                .Select(action)
                .All(x => x);
    }
}