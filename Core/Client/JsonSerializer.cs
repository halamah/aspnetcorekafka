using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Client
{
    public class JsonSerializer : IMessageSerializer
    {
        public string Serialize(object value)
        {
            throw new System.NotImplementedException();
        }

        public string Deserialize(object value)
        {
            throw new System.NotImplementedException();
        }
    }
}