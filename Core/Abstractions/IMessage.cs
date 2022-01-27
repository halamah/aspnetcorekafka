using System.Collections.Generic;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IStorable
    {
        bool Commit();
        
        bool Store();
        
        IEnumerable<IMessage> Messages { get; }
    }
    
    public interface IMessage : IStorable
    {
        string Name { get; }
        
        int Partition { get; }
        
        long Offset { get; }
        
        string Key { get; }
        
        string Topic { get; }
        
        string Group { get; }

        object GetValue();
    }
    
    public interface IMessage<out T> : IMessage
    {
        T Value { get; }
    }
}