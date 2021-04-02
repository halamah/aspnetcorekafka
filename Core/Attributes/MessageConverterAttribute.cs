using System;
using System.Reflection;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace AspNetCore.Kafka.Attributes
{
    [AttributeUsage(AttributeTargets.Method)]
    public class MessageConverterAttribute : Attribute
    {
        public MessageConverterAttribute(Type converterType) : this(converterType, null)
        {
        }
        
        public MessageConverterAttribute(Type converterType, Type argumentType)
        {
            if (!converterType.IsAssignableTo(typeof(IMessageConverter)))
                throw new ArgumentException("Message converter should inherit from IMessageConverter");
                
            ConverterType = converterType;
            ArgumentType = argumentType;
        }

        public virtual object CreateInstance(IServiceProvider provider, MethodInfo methodInfo)
        {
            var argument = ResolveOptions(provider);

            return argument is not null
                ? ActivatorUtilities.CreateInstance(provider, ConverterType, methodInfo, argument)
                : ActivatorUtilities.CreateInstance(provider, ConverterType, methodInfo);
        }

        protected virtual object ResolveOptions(IServiceProvider provider)
        {
            object argument = null;

            if (ArgumentType is not null)
            {
                var actualArgument = provider.GetService(typeof(IOptions<>).MakeGenericType(ArgumentType));

                argument = actualArgument is not null
                    ? actualArgument.GetType().GetProperty("Value")?.GetValue(actualArgument) 
                    : provider.GetRequiredService(ArgumentType);
            }
            
            return argument is null
                ? null
                : Activator.CreateInstance(
                    typeof(MessageConverterArgument<>).MakeGenericType(argument.GetType()), argument);
        }

        public Type ConverterType { get; }
        
        public Type ArgumentType { get; }
    }
}