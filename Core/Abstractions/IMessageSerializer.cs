namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageSerializer
    {
        string Serialize<T>(T value);
        
        T Deserialize<T>(string value);
    }
}