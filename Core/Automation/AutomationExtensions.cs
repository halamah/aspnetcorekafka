using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Utility;
using Microsoft.Extensions.Configuration;

[assembly: InternalsVisibleTo("Tests")]
namespace AspNetCore.Kafka.Automation
{
    internal static class AutomationExtensions
    {
        private const string KafkaMessageConfigurationPath = "Kafka:Message";

        private const BindingFlags Invokable
            = BindingFlags.Instance
              | BindingFlags.NonPublic
              | BindingFlags.Public
              | BindingFlags.InvokeMethod;
        
        public static IReadOnlyCollection<Type> GetMessageHandlerTypes(this IEnumerable<Assembly> fromAssemblies)
        {
            return fromAssemblies   
                .SelectMany(x => x.GetTypes())
                .Where(x => x.IsNonAbstractClass())
                .Where(x => x.IsMessageHandlerType())
                .ToList();
        }
        
        public static IReadOnlyCollection<MethodInfo> GetMessageHandlerMethods(this IEnumerable<Type> fromTypes)
        {
            return fromTypes
                .SelectMany(x => x.GetMethods(Invokable))
                .Where(x => x.IsMessageHandlerMethod())
                .ToList();
        }

        public static bool IsMessageHandlerMethod(this MethodInfo methodInfo)
        {
            var type = methodInfo.DeclaringType;

            return methodInfo.GetCustomAttributes<MessageAttribute>(true).Any() ||
                   type!.GetInterfaces()
                       .Where(x => x.IsGenericType)
                       .Where(x => x.GetGenericTypeDefinition() == typeof(IMessageHandler<>))
                       .SelectMany(x => type.GetInterfaceMap(x).TargetMethods)
                       .Any(x => x == methodInfo);
        }

        public static bool IsNonAbstractClass(this Type type) 
            => type.IsClass && !type.IsAbstract && !type.IsInterface;

        public static bool IsMessageHandlerType(this Type type)
            => type.GetCustomAttribute<MessageHandlerAttribute>() is not null ||
               type.IsAssignableTo(typeof(IMessageHandler));

        public static IEnumerable<SubscriptionDefinition> GetSubscriptionDefinitions(
                this MethodInfo methodInfo,
                IConfiguration configuration)
        {
            var name = methodInfo.GetCustomAttribute<MessageAttribute>()?.Name;
            var contractType = methodInfo.GetContractType();
            var messages = methodInfo.GetCustomAttributes<MessageAttribute>().ToList();
            var defaultConfig = ConfigurationString.Parse(configuration.GetValue<string>($"{KafkaMessageConfigurationPath}:Default"));
            var messageConfig = !string.IsNullOrWhiteSpace(name)
                ? ConfigurationString.Parse(configuration.GetValue<string>($"{KafkaMessageConfigurationPath}:{name}"))
                : ConfigurationString.Empty;
            var configString = methodInfo.GetCustomAttribute<MessageConfigAttribute>() is var messageConfigAttribute and not null
                ? ConfigurationString.Parse(messageConfigAttribute.ConfigString)
                : ConfigurationString.Empty;
            
            if(!messages.Any())
                messages.Add(new MessageAttribute());

            foreach (var attribute in messages)
            {
                var definitions = new[]
                    {
                        new MessageAttribute()
                            .AssignFromConfig(defaultConfig)
                            .AssignFromConfig(messageConfig)
                            .AssignFromConfig(configString),
                        TopicDefinition.FromType(contractType),
                        attribute,
                    }
                    .Where(x => x is not null)
                    .ToArray();

                var policies = defaultConfig
                    .ReadConfiguredPolicies()
                    .Concat(messageConfig.ReadConfiguredPolicies())
                    .Concat(configString.ReadConfiguredPolicies())
                    .Concat(methodInfo.GetCustomAttributes<MessagePolicyAttribute>())
                    .GroupBy(x => x.GetType())
                    .Select(x => x.Last())
                    .ToList();
                
                var offsets = new[]
                    {
                        new MessageOffset()
                            .AssignFromConfig(defaultConfig)
                            .AssignFromConfig(messageConfig)
                            .AssignFromConfig(configString),
                        policies.Select(x => x as OffsetAttribute).LastOrDefault(x => x is not null)?.Value,
                        methodInfo.GetCustomAttribute<OffsetAttribute>()?.Value
                    }
                    .Where(x => x is not null).ToArray();
                
                var topic = definitions.Select(x => x.Topic).LastOrDefault(x => !string.IsNullOrWhiteSpace(x));
                var format = definitions.Select(x => x.Format).LastOrDefault(x => x != TopicFormat.Unset);

                var options = new SourceOptions
                {
                    Offset = new MessageOffset
                    {
                        Offset = offsets.Select(x => x.Offset).LastOrDefault(x => x is not null),
                        Bias = offsets.Select(x => x.Bias).LastOrDefault(x => x is not null),
                        DateOffset = offsets.Select(x => x.DateOffset).LastOrDefault(x => x is not null),
                    },
                    Format = format,
                };

                yield return new SubscriptionDefinition
                {
                    Name = name,
                    Topic = topic,
                    Options = options,
                    MethodInfo = methodInfo,
                    Policies = policies
                };
            }
        }

        public static Type GetContractType(this MethodInfo method)
        {
            var arg = method.GetParameters().Single().ParameterType;
            return GetContractType(arg);
        }
        
        private static Type GetContractType(Type type)
        {
            if (!type.IsGenericType)
                return type.IsArray ? type.GetElementType() : type;

            if (type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            {
                var itemType = type.GenericTypeArguments.Single();
                return GetContractType(itemType);
            }

            var message = type.GetInterfaces().Concat(new[] {type})
                .Where(x => x.IsGenericType)
                .Select(x => new[] {x}.Concat(x.GetGenericArguments()))
                .SelectMany(x => x)
                .FirstOrDefault(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IMessage<>));
                
            return message?.GetGenericArguments().FirstOrDefault();
        }

        public static Delegate CreateDelegate(this MethodInfo methodInfo, object target)
        {
            var types = methodInfo.GetParameters().Select(p => p.ParameterType).Concat(new[] { methodInfo.ReturnType });

            return methodInfo.IsStatic
                ? Delegate.CreateDelegate(Expression.GetFuncType(types.ToArray()), methodInfo) 
                : Delegate.CreateDelegate(Expression.GetFuncType(types.ToArray()), target, methodInfo);
        }
    }
}