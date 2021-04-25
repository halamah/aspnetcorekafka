using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Utility;

namespace AspNetCore.Kafka.Automation.Pipeline
{
    internal static class PipelineConfigurationExtensions
    {
        public static IEnumerable<MessagePolicyAttribute> ReadConfiguredPolicies(this InlineConfigurationValues config)
        {
            if (!config.Functions.Any())
                yield break;

            var knownProperties = new HashSet<string>()
            {
                "offset", "bias", "dateoffset", "topic", "format"
            };

            foreach (var (name, _) in config.Properties)
                if(!knownProperties.Contains(name.ToLower()))
                    throw new ArgumentException($"Invalid message property '{name}'");

            var knownFunctions = new Dictionary<string, Type>
            {
                ["buffer"] = typeof(BufferAttribute),
                ["batch"] = typeof(BatchAttribute),
                ["parallel"] = typeof(ParallelAttribute),
                ["commit"] = typeof(CommitAttribute),
                ["failure"] = typeof(FailuresAttribute),
                ["offset"] = typeof(OffsetAttribute),
                ["retryonfailure"] = typeof(RetryOnFailureAttribute),
                ["skipfailure"] = typeof(SkipFailureAttribute),
            };

            foreach (var (blockName, arguments) in config.Functions)
            {
                var type = knownFunctions.GetValueOrDefault(blockName.ToLower());

                if (type is null)
                    throw new ArgumentException($"Invalid message policy '{blockName}'");

                MessagePolicyAttribute instance = null; 
                
                foreach (var constructorInfo in type.GetConstructors(BindingFlags.Instance | BindingFlags.Public))
                {
                    try
                    {
                        if(arguments.Length != constructorInfo.GetParameters().Length)
                            throw new Exception("Invalid block arguments");
                        
                        var constructorArguments = arguments
                            .Zip(constructorInfo.GetParameters())
                            .Select(x => x.First.ChangeType(x.Second.ParameterType))
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
                    throw new ArgumentException($"Invalid arguments for block [{blockName}({string.Join(",", arguments)})]");
                
                yield return instance;
            }
        }

        public static T AssignFromConfig<T>(this T target, InlineConfigurationValues config)
        {
            if (target is null)
                return target;
            
            if (config.Properties.Any())
                return target;
            
            foreach (var (name, value) in config.Properties)
            {
                var property = target.GetType()
                    .GetProperty(name, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.SetProperty | BindingFlags.IgnoreCase);

                property?.SetValue(target, value.ChangeType(property.PropertyType));
            }
            
            return target;
        }
    }
}