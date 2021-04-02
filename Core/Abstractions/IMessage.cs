namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessage
    {
        int Partition { get; }
        
        long Offset { get; }
        
        string Key { get; }
        
        string Topic { get; }
        
        void SuppressCommit();

        bool Commit();
    }
    
    public interface IMessage<out T> : IMessage
    {
        T Value { get; }
    }
}