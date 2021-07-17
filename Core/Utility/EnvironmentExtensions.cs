using System;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Utility
{
    public static class EnvironmentExtensions
    {
        public static string ExpandTemplate(this IKafkaEnvironment environment, string input)
            => input?.Replace("{env}", environment.EnvironmentName?.ToUpper() ?? Environment.MachineName);
    }
}