namespace AspNetCore.Kafka.Abstractions
{
    public interface IGroupedMessageEnumerable<T> : IMessageEnumerable<T>, IGrouped
    {
        
    }
}