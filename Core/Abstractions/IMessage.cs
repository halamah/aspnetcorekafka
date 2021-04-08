using System;

namespace AspNetCore.Kafka.Abstractions
{
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