namespace AspNetCore.Kafka.Abstractions
{
    public interface IKafkaMessageJsonSerializer
    {
        string Serialize<T>(T value);
        
        T Deserialize<T>(string value);
    }
}