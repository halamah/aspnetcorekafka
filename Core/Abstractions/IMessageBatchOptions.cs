namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageBatchOptions
    {
        public int Size { get; }
        
        public int Time { get; }
    }
}