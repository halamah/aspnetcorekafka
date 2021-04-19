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

            var map = new Dictionary<string, Type>
            {
                ["buffer"] = typeof(BufferAttribute),
                ["batch"] = typeof(BatchAttribute),
                ["parallel"] = typeof(ParallelAttribute),
                ["commit"] = typeof(CommitAttribute),
            };

            foreach (var (blockName, arguments) in configString.ReadConfiguredFunctionMap())
            {
                var type = map.GetValueOrDefault(blockName.ToLower());

                if (type is null)
                    throw new ArgumentException($"Invalid block name '{blockName}'");

                MessageBlockAttribute instance = null; 
                
                foreach (var constructorInfo in type.GetConstructors(BindingFlags.Instance | BindingFlags.Public))
                {
                    try
                    {
                        if(arguments.Length != constructorInfo.GetParameters().Length)
                            throw new Exception("Invalid block arguments");
                        
                        var constructorArguments = arguments
                            .Zip(constructorInfo.GetParameters())
                            .Select(x => ChangeType(x.First, x.Second.ParameterType))
                            .ToArray();

                        instance = (MessageBlockAttribute) constructorInfo.Invoke(constructorArguments);
                    }
                    catch (Exception e)
                    {
                        continue;
                    }
                    
                    break;
                }

                if (instance is null)
                    throw new ArgumentException($"Invalid arguments for block [{blockName}({string.Join(",", arguments)})]");
                
                yield return instance;
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