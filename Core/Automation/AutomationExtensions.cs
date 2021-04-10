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
            var type = methodInfo.DeclaringType;

            return methodInfo.GetCustomAttribute<MessageAttribute>() is not null ||
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
        
        public static (string Topic, SubscriptionOptions Options) GetSubscriptionOptions(this MethodInfo methodInfo)
        {
            var contractType = methodInfo.GetContractType();
            
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
        
        public static Type GetContractType(this MethodInfo method)
        {
            return method
                .GetParameters()
                .SelectMany(x => x.ParameterType.GetInterfaces().Concat(new[] {x.ParameterType}))
                .Where(x => x.IsGenericType)
                .Select(x => new[] {x}.Concat(x.GetGenericArguments()))
                .SelectMany(x => x)
                .FirstOrDefault(x => x.GetGenericTypeDefinition() == typeof(IMessage<>))?
                .GetGenericArguments()
                .FirstOrDefault();
        }
        
        public static Delegate CreateDelegate(this MethodInfo methodInfo, object target)
        {
            var types = methodInfo.GetParameters().Select(p => p.ParameterType).Concat(new[] { methodInfo.ReturnType });

            return methodInfo.IsStatic
                ? Delegate.CreateDelegate(Expression.GetFuncType(types.ToArray()), methodInfo) 
                : Delegate.CreateDelegate(Expression.GetFuncType(types.ToArray()), target, methodInfo.Name);
        }
    }
}