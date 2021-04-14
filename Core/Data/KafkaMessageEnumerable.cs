using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using AspNetCore.Kafka.Abstractions;
using MoreLinq.Extensions;

namespace AspNetCore.Kafka.Data
{
    public class KafkaMessageEnumerable<T> : IMessageEnumerable<T>, IEnumerable<T>
    {
        private bool _suppressCommit;
        private readonly IEnumerable<IMessage<T>> _collection;
        private readonly Lazy<bool> _commit;

        public KafkaMessageEnumerable(IEnumerable<IMessage<T>> collection)
        {
            _collection = collection;
            _commit = new Lazy<bool>(DoCommit);
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator() => _collection.Select(x => x.Value).GetEnumerator();

        public IEnumerator<IMessage<T>> GetEnumerator() => _collection.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _collection.GetEnumerator();

        public bool SuppressCommit()
        {
            _suppressCommit = true;
            return !_commit.IsValueCreated;
        }

        public bool Commit(bool force = false) => _suppressCommit && !force 
            ? _commit.IsValueCreated && _commit.Value 
            : _commit.Value;   

        private bool DoCommit()
            => _collection
                .OrderByDescending(m => m.Offset)
                .DistinctBy(m => m.Partition)
                .Select(m => m.Commit(true))
                .All(x => x);
    }
}