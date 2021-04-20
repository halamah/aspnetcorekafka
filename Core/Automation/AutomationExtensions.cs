using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Options;
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
                   methodInfo.GetCustomAttributes<MessageConfigAttribute>(true).Any() ||
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
                IConfiguration config)
        {
            var contractType = methodInfo.GetContractType();
            var messages = methodInfo.GetCustomAttributes<MessageAttribute>().ToList();
            
            if(!messages.Any())
                messages.Add(new MessageAttribute());

            foreach (var attribute in messages)
            {
                var pipeline = attribute as MessageConfigAttribute;
                
                var configString = pipeline is not null
                    ? config.GetValue<string>($"{KafkaMessageConfigurationPath}:{pipeline.MessageName}")
                    : null;

                var defaultConfigString = config.GetValue<string>($"{KafkaMessageConfigurationPath}:Default");

                if (pipeline is not null && string.IsNullOrEmpty(configString))
                    throw new ArgumentException($"Invalid pipeline configuration. MessageName: {pipeline.MessageName}");
                    
                var definitions = new[]
                    {
                        attribute?
                            .AssignFromConfigString(defaultConfigString)
                            .AssignFromConfigString(configString),
                        TopicDefinition.FromType(contractType)
                    }
                    .Where(x => x is not null)
                    .ToArray();

                var topic = definitions.Select(x => x.Topic).FirstOrDefault(x => !string.IsNullOrWhiteSpace(x));

                var options = new SourceOptions
                {
                    Offset = (methodInfo.GetCustomAttribute<OffsetAttribute>()?.Value ?? new MessageOffset())
                        .AssignFromConfigString(defaultConfigString)
                        .AssignFromConfigString(configString),
                    Format = definitions.Select(x => x.Format).FirstOrDefault(x => x != TopicFormat.Unset)
                };

                yield return new SubscriptionDefinition
                {
                    Topic = topic,
                    Options = options,
                    MethodInfo = methodInfo,
                    Blocks = methodInfo.GetCustomAttributes<MessageBlockAttribute>()
                        .Concat(defaultConfigString.ReadConfiguredBlocks())
                        .Concat(configString.ReadConfiguredBlocks())
                        .GroupBy(x => x.GetType())
                        .Select(x => x.Last())
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