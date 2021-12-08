using System.Linq;
using AspNetCore.Kafka.Options;

namespace AspNetCore.Kafka.Utility
{
    public static class EnvironmentExtensions
    {
        public static string ExpandTemplate(this KafkaOptions options, string input)
            => options.Configuration.Placeholders.Aggregate(
                input,
                (value, placeholder) => value?.Replace($"{{{placeholder.Key}}}", placeholder.Value));
    }
}