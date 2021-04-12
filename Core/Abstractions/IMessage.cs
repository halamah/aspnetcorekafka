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
    
    public interface IMessage : IMessageOffset
    {
        int Partition { get; }
        
        long Offset { get; }
        
        string Key { get; }
        
        string Topic { get; }
    }
    
    public interface IMessage<out T> : IMessage
    {
        T Value { get; }
    }
}