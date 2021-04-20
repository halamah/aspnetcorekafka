namespace AspNetCore.Kafka.Abstractions
{
    public interface IJsonMessageSerializer
    {
        string Serialize<T>(T value);
        
        T Deserialize<T>(string value);
    }
}