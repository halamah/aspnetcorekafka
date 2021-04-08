using System;
using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageOffset
    {
        bool SuppressCommit();

        IDisposable BeginCommitScope(bool force = false) => Disposable.Create(() => Commit(force));

        bool Commit(bool force = false);
    }
}