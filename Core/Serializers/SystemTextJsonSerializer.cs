using System.Text.Json;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Serializers
{
    public class SystemTextJsonSerializer : IKafkaMessageJsonSerializer
    {
        private readonly JsonSerializerOptions _options;

        public SystemTextJsonSerializer(JsonSerializerOptions options = null)
        {
            _options = options ?? new JsonSerializerOptions
            {
                DictionaryKeyPolicy = JsonNamingPolicy.CamelCase,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                PropertyNameCaseInsensitive = true,
                IgnoreNullValues = true,
                IgnoreReadOnlyFields = true,
                IgnoreReadOnlyProperties = true,
            };
        }

        public string Serialize<T>(T value) => JsonSerializer.Serialize(value, _options);

        public T Deserialize<T>(string value) => JsonSerializer.Deserialize<T>(value, _options);
    }
}