using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Attributes;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.VisualBasic;
using Microsoft.VisualBasic.CompilerServices;

[assembly: InternalsVisibleTo("Tests")]
namespace AspNetCore.Kafka.Automation
{
    internal static class AutomationExtensions
    {
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
            return methodInfo.GetCustomAttribute<MessageAttribute>() != null
                   || methodInfo.IsFromChildInterfaceOf(typeof(IMessageHandler));
        }

        private static bool IsFromChildInterfaceOf(this MethodInfo methodInfo, Type interfaceType)
        {
            var declaringType = methodInfo.DeclaringType;
            if (declaringType == null) return false;

            return declaringType
                .GetInterfaces()
                .Where(i => i.GetInterfaces().Contains(interfaceType))
                .Any(methodInfo.IsFromInterface);
        }

        private static bool IsFromInterface(this MethodInfo methodInfo, Type interfaceType)
        {
            if (interfaceType == null) return false;

            var interfaceMethodInfo = interfaceType.GetMethod(methodInfo.Name);
            if (interfaceMethodInfo?.DeclaringType == null) return false;

            var interfaceMethodParams = interfaceMethodInfo.GetParameters().Select(p => p.ParameterType)
                .ToArray();

            var map = methodInfo.DeclaringType?.GetInterfaceMap(interfaceMethodInfo.DeclaringType);
            return map?.TargetType.GetMethod(interfaceMethodInfo.Name, interfaceMethodParams) != null;
        }

        public static bool IsNonAbstractClass(this Type type) 
            => type.IsClass && !type.IsAbstract && !type.IsInterface;

        public static bool IsMessageHandlerType(this Type type)
        {
            return type.GetCustomAttribute<MessageHandlerAttribute>() != null
                   || type.GetInterfaces().Any(i
                       => i == typeof(IMessageHandler)
                          || i.IsAssignableFrom(typeof(IMessageHandler)));
        }
        
        public static (string Topic, SubscriptionOptions Options) GetSubscriptionOptions(this MethodInfo methodInfo)
        {
            var contractType = ActionMessageBlock.GetContractType(methodInfo);
            var definitions = new[]
                {
                    methodInfo.GetCustomAttribute<MessageAttribute>(),
                    TopicDefinition.FromType(contractType)
                }
                .Where(x => x is not null)
                .ToArray();

            var options = new SubscriptionOptions
            {
                Offset = definitions.Select(x => x.Offset).FirstOrDefault(x => x != TopicOffset.Unset),
                Bias = definitions.Select(x => x.Bias).FirstOrDefault(),
                Format = definitions.Select(x => x.Format).FirstOrDefault(x => x != TopicFormat.Unset),
                Buffer = definitions.Select(x => x.Buffer).FirstOrDefault(),
            };

            var topic = definitions.Select(x => x.Topic).FirstOrDefault(x => !string.IsNullOrWhiteSpace(x));

            return (topic, options);
        }
        
        public static object ResolveBlock(this MethodInfo method, IServiceProvider provider)
        {
            var blockInfo = method.GetCustomAttribute<MessageBlockAttribute>() ??
                            new MessageBlockAttribute(typeof(ActionMessageBlock));

            var argument = blockInfo.ArgumentType is not null
                ? provider.GetService(blockInfo.ArgumentType)
                  ?? Versioned.CallByName(
                      provider.GetRequiredService(typeof(IOptions<>).MakeGenericType(blockInfo.ArgumentType)), "Value",
                      CallType.Get)
                : blockInfo;

            argument ??= new InvalidOperationException($"Null options provided for {method}");

            try
            {
                return ActivatorUtilities.CreateInstance(provider, blockInfo.BlockType, argument);
            }
            catch (InvalidOperationException)
            {
                return ActivatorUtilities.CreateInstance(provider, blockInfo.BlockType);
            }
        }
    }
}