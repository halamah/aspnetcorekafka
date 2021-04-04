namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageSerializer
    {
        string Serialize(object value);
        
        string Deserialize(object value);
    }
}