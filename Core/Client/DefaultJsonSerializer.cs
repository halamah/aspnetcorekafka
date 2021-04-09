using System.Text.Json;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Client
{
    public class DefaultJsonSerializer : IMessageSerializer
    {
        public string Serialize<T>(T value) => JsonSerializer.Serialize(value);

        public T Deserialize<T>(string value) => JsonSerializer.Deserialize<T>(value);
    }
}