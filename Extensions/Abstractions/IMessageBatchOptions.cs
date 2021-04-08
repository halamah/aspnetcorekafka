namespace AspNetCore.Kafka.Extensions.Abstractions
{
    public interface IMessageBatchOptions
    {
        public int Size { get; }
        
        public int Time { get; }
        
        public bool Commit { get; }
    }
}