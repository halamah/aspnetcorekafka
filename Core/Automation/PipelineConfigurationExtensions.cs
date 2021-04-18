using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using AspNetCore.Kafka.Automation.Attributes;

namespace AspNetCore.Kafka.Automation
{
    internal static class PipelineConfigurationExtensions
    {
        public static IEnumerable<MessageBlockAttribute> ReadConfiguredBlocks(this string configString)
        {
            if (string.IsNullOrWhiteSpace(configString))
                yield break;

            foreach (var (function, arguments) in configString.ReadConfiguredFunctionMap())
            {
                switch (function.ToLower())
                {
                    case "buffer":
                    {
                        if(arguments.Length != 1)
                            throw new ArgumentException($"Buffer block must contain 1 argument with buffer size. {arguments.Length} arguments provided");
                        
                        yield return new BufferAttribute(int.Parse(arguments[0]));
                        break;
                    }
                    case "batch":
                    {
                        if(arguments.Length != 2)
                            throw new ArgumentException($"Batch block must contain 2 arguments with buffer size and time. {arguments.Length} arguments provided");
                        
                        yield return new BatchAttribute(int.Parse(arguments[0]), int.Parse(arguments[1]));
                        break;
                    }
                    case "partitioned":
                    {
                        if(arguments.Length > 1)
                            throw new ArgumentException($"Partitioned block should be configured with 1 or none arguments. {arguments.Length} arguments provided");
                        
                        yield return new PartitionedAttribute(arguments.Any() ? int.Parse(arguments[0]) : -1);
                        break;
                    }
                    case "commit":
                    {
                        if (arguments.Any())
                            throw new ArgumentException($"Commit block has no arguments. {arguments.Length} arguments provided");
                        
                        yield return new CommitAttribute();
                        break;
                    }
                }
            }
        }

        public static T AssignFromConfigString<T>(this T target, string configString)
        {
            if (target is null)
                return target;
            
            if (string.IsNullOrWhiteSpace(configString))
                return target;
            
            ValidateMessageConfigString(configString);

            foreach (var (name, value) in configString.ReadConfiguredPropertyMap())
            {
                var property = target.GetType()
                    .GetProperty(name, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.SetProperty | BindingFlags.IgnoreCase);

                property?.SetValue(target, ChangeType(value, property.PropertyType));
            }
            
            return target;
        }

        private static object ChangeType(string value, Type type)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;

            return type.IsEnum
                ? Enum.Parse(type, value, true)
                : type switch
                {
                    _ when type == typeof(DateTimeOffset) => DateTimeOffset.Parse(value),
                    _ when type == typeof(DateTime) => DateTime.Parse(value),
                    _ when type == typeof(TimeSpan) => TimeSpan.Parse(value),
                    _ => Convert.ChangeType(value, type)
                };
        }

        public static void ValidateMessageConfigString(string config)
        {
            const string pattern = @"^(((([a-z]+)\s*(:|=)\s*[^,;]*)|([a-z]+\s*\(([a-z0-9\s]*,?)*\)))\s*[,;]?\s*)+$";
            if(!Regex.IsMatch(config, pattern, RegexOptions.IgnoreCase))
                throw new ArgumentException($"Invalid configuration [{config}]");
        }
        
        public static Dictionary<string, string> ReadConfiguredPropertyMap(this string configString)
        {
            if (string.IsNullOrWhiteSpace(configString))
                return new();
            
            const string pattern = @"(?<name>[a-z]+)\s*(:|=)\s*(?<value>[^,;\s]+)";
            var matches = Regex.Matches(configString, pattern, RegexOptions.IgnoreCase);
            
            if(!matches.Any())
                return new();
            
            return matches.ToDictionary(x => x.Groups["name"].Value, x => x.Groups["value"].Value);
        }
        
        public static Dictionary<string, string[]> ReadConfiguredFunctionMap(this string configString)
        {
            if (string.IsNullOrWhiteSpace(configString))
                return new();
            
            const string pattern = @"(?<name>[a-z]+)\s*\((\s*(?<value>[a-z0-9]+)\s*,?\s*)*\)";
            var matches = Regex.Matches(configString, pattern, RegexOptions.IgnoreCase);
            
            if(!matches.Any())
                return new();
            
            return matches.ToDictionary(x => x.Groups["name"].Value,
                x => x.Groups["value"].Captures.Select(c => c.Value).ToArray());
        }
    }
}