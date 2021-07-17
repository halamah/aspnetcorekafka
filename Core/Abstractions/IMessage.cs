using System.Collections.Generic;

namespace AspNetCore.Kafka.Abstractions
{
    public interface ICommittable
    {
        bool Commit();
        
        IEnumerable<IMessage> Messages { get; }
    }
    
    public interface IMessage : ICommittable
    {
        int Partition { get; }
        
        long Offset { get; }
        
        string Key { get; }
        
        string Topic { get; }

        object GetValue();
    }
    
    public interface IMessage<out T> : IMessage
    {
        T Value { get; }
    }
}