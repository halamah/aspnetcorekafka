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

        public bool Commit() => _commit.Value;

        public IEnumerable<IMessage> Messages => this;

        private bool DoCommit()
            => _collection
                .OrderByDescending(m => m.Offset)
                .DistinctBy(m => m.Partition)
                .Select(m => m.Commit())
                .All(x => x);
    }
}