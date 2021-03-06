using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
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
               typeof(IMessageHandler).IsAssignableFrom(type);

        public static IEnumerable<SubscriptionDefinition> GetSubscriptionDefinitions(
                this MethodInfo methodInfo,
                IConfiguration configuration)
        {
            var contractType = methodInfo.GetContractType();
            var definition = TopicDefinition.FromType(contractType);
            var messages = methodInfo.GetCustomAttributes<MessageAttribute>().ToList();
            var defaultConfig = ConfigurationString.Parse(configuration.GetValue<string>($"{KafkaMessageConfigurationPath}:Default"));
            var configString = methodInfo.GetCustomAttribute<ConfigAttribute>() is var messageConfigAttribute and not null
                ? ConfigurationString.Parse(messageConfigAttribute.ConfigString)
                : ConfigurationString.Empty;
            
            if(!messages.Any())
                messages.Add(new MessageAttribute());

            foreach (var attribute in messages)
            {
                var name = attribute.Name ?? definition.Name;
                var messageConfig = !string.IsNullOrWhiteSpace(name)
                    ? ConfigurationString.Parse(configuration.GetValue<string>($"{KafkaMessageConfigurationPath}:{name}"))
                    : ConfigurationString.Empty;
                
                var definitions = new[]
                    {
                        new MessageAttribute()
                            .AssignFromConfig(defaultConfig)
                            .AssignFromConfig(messageConfig)
                            .AssignFromConfig(configString),
                        definition,
                        attribute,
                    }
                    .Where(x => x is not null)
                    .ToArray();

                var defaultPolicies = defaultConfig.ReadConfiguredPolicies();
                var configMessagePolicies = messageConfig.ReadConfiguredPolicies();
                var configPolicies = configString.ReadConfiguredPolicies();
                var messagePolicies = methodInfo.GetCustomAttributes<MessagePolicyAttribute>();

                var policies = defaultConfig
                    .ReadConfiguredPolicies()
                    .Concat(defaultPolicies)
                    .Concat(configMessagePolicies)
                    .Concat(configPolicies)
                    .Concat(messagePolicies)
                    .GroupBy(x => x.GetType())
                    .Select(x => x.Last())
                    .ToList();

                var topic = definitions.Select(x => x.Topic).LastOrDefault(x => !string.IsNullOrWhiteSpace(x));
                var format = definitions.Select(x => x.Format).LastOrDefault(x => x != TopicFormat.Unset);

                var options = new SourceOptions
                {
                    Offset = policies
                        .Where(x => x is OffsetAttribute)
                        .Cast<OffsetAttribute>()
                        .Select(x => x.Value)
                        .LastOrDefault(x => x.Offset is not null || x.DateOffset is not null),
                    Format = format,
                    Name = name,
                };

                yield return new SubscriptionDefinition
                {
                    Topic = topic,
                    Options = options,
                    MethodInfo = methodInfo,
                    Policies = policies
                };
            }
        }

        public static bool HasCancellationToken(this MethodInfo method)
            => method.GetParameters().Any(x => x.ParameterType == typeof(CancellationToken));
        
        public static Type GetMessageParameterType(this MethodInfo method)
            => method.GetParameters().First(x => x.ParameterType != typeof(CancellationToken)).ParameterType;
        
        public static Type GetContractType(this MethodInfo method)
            => GetContractType(method.GetMessageParameterType());
        
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
                .SelectMany(x => new[] {x}.Concat(x.GetGenericArguments()))
                .FirstOrDefault(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IMessage<>));
                
            return message?.GetGenericArguments().FirstOrDefault() ?? type;
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