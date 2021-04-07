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

        public IMessage SuppressCommit()
        {
            _suppressCommit = true;
            return this;
        }

        public IDisposable GetCommitDisposable() => new CommitDisposable(this);

        public bool Commit(bool force = false) => !force && _suppressCommit || _commit.Value;
    }

    public class CommitDisposable : IDisposable
    {
        private readonly IMessage _message;

        public CommitDisposable(IMessage message) => _message = message;

        public void Dispose() => _message?.Commit();
    }
}