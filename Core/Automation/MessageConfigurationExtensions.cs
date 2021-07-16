using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Utility;

namespace AspNetCore.Kafka.Automation
{
    internal static class MessageConfigurationExtensions
    {
        public static IEnumerable<MessagePolicyAttribute> ReadConfiguredPolicies(this ConfigurationString config)
        {
            if (config.IsEmpty)
                yield break;

            var knownProperties = new HashSet<string>()
            {
                "offset", "bias", "dateoffset", "topic", "format", "state"
            };

            var knownFunctions = new Dictionary<string, Type>
            {
                ["buffer"] = typeof(BufferAttribute),
                ["batch"] = typeof(BatchAttribute),
                ["parallel"] = typeof(ParallelAttribute),
                ["commit"] = typeof(CommitAttribute),
                ["offset"] = typeof(OffsetAttribute),
                ["state"] = typeof(MessageStateAttribute),
                
                ["options"] = typeof(OptionsAttribute),
            };
            
            foreach (var (name, _) in config.Properties)
                if(!knownProperties.Contains(name.ToLower()))
                    throw new ArgumentException($"Invalid message property '{name}'");
            
            foreach (var (name, _) in config.Functions)
                if(!knownFunctions.Keys.Contains(name.ToLower()))
                    throw new ArgumentException($"Invalid message policy '{name}'");

            var all = config.Functions
                .Concat(config.Properties.ToDictionary(x => x.Key, x => new[] {x.Value}))
                .ToDictionary(x => x.Key, x => x.Value);
                
            foreach (var (policyName, arguments) in all)
            {
                var type = knownFunctions.GetValueOrDefault(policyName.ToLower());

                if (type is null)
                    continue;

                if (type == typeof(OptionsAttribute))
                {
                    var flags = arguments.Select(ConfigurationString.ChangeType<Option>).ToArray();
                    yield return new OptionsAttribute(flags);
                    continue;
                }

                MessagePolicyAttribute instance = null; 
                
                foreach (var constructorInfo in type.GetConstructors(BindingFlags.Instance | BindingFlags.Public))
                {
                    try
                    {
                        if(arguments.Length != constructorInfo.GetParameters().Length)
                            throw new Exception("Invalid policy arguments");
                        
                        var constructorArguments = arguments
                            .Zip(constructorInfo.GetParameters())
                            .Select(x => ConfigurationString.ChangeType(x.First, x.Second.ParameterType))
                            .ToArray();

                        instance = (MessagePolicyAttribute) constructorInfo.Invoke(constructorArguments);
                    }
                    catch (Exception)
                    {
                        continue;
                    }
                    
                    break;
                }

                if (instance is null)
                    throw new ArgumentException($"Invalid arguments for policy [{policyName}({string.Join(",", arguments)})]");
                
                yield return instance;
            }
        }

        public static T AssignFromConfig<T>(this T target, ConfigurationString config)
        {
            if (target is null)
                return target;
            
            if (!config.Properties.Any())
                return target;
            
            foreach (var (name, value) in config.Properties)
            {
                var property = target.GetType()
                    .GetProperty(name, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.SetProperty | BindingFlags.IgnoreCase);

                property?.SetValue(target, ConfigurationString.ChangeType(value, property.PropertyType));
            }
            
            return target;
        }
    }
}