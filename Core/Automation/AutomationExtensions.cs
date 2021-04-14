using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Attributes;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Options;

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
            var type = methodInfo.DeclaringType;

            return methodInfo.GetCustomAttributes<MessageAttribute>().Any() ||
                   type!.GetInterfaces()
                       .Where(x => x.GetCustomAttribute<MessageHandlerAttribute>() is not null)
                       .SelectMany(x => type.GetInterfaceMap(x).TargetMethods)
                       .Any(x => x == methodInfo);
        }

        public static bool IsNonAbstractClass(this Type type) 
            => type.IsClass && !type.IsAbstract && !type.IsInterface;

        public static bool IsMessageHandlerType(this Type type)
            => type.GetCustomAttribute<MessageHandlerAttribute>() is not null ||
               type.IsAssignableTo(typeof(IMessageHandler));
        
        public static 
            IEnumerable<(string Topic, SubscriptionOptions Options, MethodInfo MethodInfo)>
            GetSubscriptionDefinitions(this MethodInfo methodInfo)
        {
            var contractType = methodInfo.GetContractType();

            foreach (var attribute in methodInfo.GetCustomAttributes<MessageAttribute>())
            {
                var definitions = new[] {attribute, TopicDefinition.FromType(contractType)}
                    .Where(x => x is not null)
                    .ToArray();

                var options = new SubscriptionOptions
                {
                    DateOffset = definitions
                        .Select(x =>
                            string.IsNullOrEmpty(x.DateOffset)
                                ? (DateTimeOffset?) null
                                : DateTimeOffset.Parse(x.DateOffset)).FirstOrDefault(x => x is not null),
                    NegativeTimeOffset = definitions.Select(x => TimeSpan.FromMinutes(x.RelativeOffsetMinutes)).Max(),
                    Offset = definitions.Select(x => x.Offset).FirstOrDefault(x => x != TopicOffset.Unset),
                    Bias = definitions.Select(x => x.Bias).FirstOrDefault(),
                    Format = definitions.Select(x => x.Format).FirstOrDefault(x => x != TopicFormat.Unset),
                };

                var topic = definitions.Select(x => x.Topic).FirstOrDefault(x => !string.IsNullOrWhiteSpace(x));

                yield return (topic, options, methodInfo);
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