using System;
using AspNetCore.Kafka.Options;

namespace AspNetCore.Kafka.Utility
{
    public static class EnvironmentExtensions
    {
        public static string ExpandTemplate(this KafkaOptions options, string input)
            => input?.Replace("{env}",
                options?.Environment?.ToUpper() ?? throw new ArgumentNullException(nameof(options.Environment)));
    }
}