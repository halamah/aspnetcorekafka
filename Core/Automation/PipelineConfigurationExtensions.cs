using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Utility;

namespace AspNetCore.Kafka.Automation
{
    internal static class PipelineConfigurationExtensions
    {
        public static IEnumerable<MessagePolicyAttribute> ReadConfiguredBlocks(this string configString)
        {
            if (string.IsNullOrWhiteSpace(configString))
                yield break;

            var map = new Dictionary<string, Type>
            {
                ["buffer"] = typeof(BufferAttribute),
                ["batch"] = typeof(BatchAttribute),
                ["parallel"] = typeof(ParallelAttribute),
                ["commit"] = typeof(CommitAttribute),
                ["failure"] = typeof(FailuresAttribute),
            };

            foreach (var (blockName, arguments) in configString.ReadConfiguredFunctions())
            {
                var type = map.GetValueOrDefault(blockName.ToLower());

                if (type is null)
                    throw new ArgumentException($"Invalid block name '{blockName}'");

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