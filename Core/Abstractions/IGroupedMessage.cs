namespace AspNetCore.Kafka.Abstractions
{
    public interface IGrouped
    {
        int Group { get; }
    }

    public interface IGroupedMessage<T> : IMessage<T>, IGrouped
    {
    }
}