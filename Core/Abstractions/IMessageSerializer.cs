namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageSerializer<TSerialized>
    {
        TSerialized Serialize<T>(T value);
        
        T Deserialize<T>(TSerialized value);
    }
}