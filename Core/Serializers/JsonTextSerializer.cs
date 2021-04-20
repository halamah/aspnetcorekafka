using System.Text.Json;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Serializers
{
    public class JsonTextSerializer : IJsonMessageSerializer
    {
        private readonly JsonSerializerOptions _options;

        public JsonTextSerializer(JsonSerializerOptions options = null)
        {
            _options = options;
        }

        public string Serialize<T>(T value) => JsonSerializer.Serialize(value, _options);

        public T Deserialize<T>(string value) => JsonSerializer.Deserialize<T>(value, _options);
    }
}