namespace AspNetCore.Kafka.Abstractions
{
    public interface IGroupedMessage<T> : IMessage<T>
    {
        int Group { get; }
    }
}