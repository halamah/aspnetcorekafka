namespace AspNetCore.Kafka.Abstractions
{
    public interface IKafkaMessageSerializer<TSerialized>
    {
        TSerialized Serialize<T>(T value);
        
        T Deserialize<T>(TSerialized value);
    }
}