namespace AspNetCore.Kafka.Abstractions
{
    public interface ICommittable
    {
        bool Commit();
    }
    
    public interface IMessage : ICommittable
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