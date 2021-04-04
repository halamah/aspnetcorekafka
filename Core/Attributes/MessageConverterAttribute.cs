using System;
using System.Reflection;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.VisualBasic;
using Microsoft.VisualBasic.CompilerServices;

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

        public object CreateInstance(IServiceProvider provider, object instance, MethodInfo methodInfo)
        {
            var argument = ResolveArgument(provider);

            return argument is not null
                ? ActivatorUtilities.CreateInstance(provider, ConverterType, instance, methodInfo, argument)
                : ActivatorUtilities.CreateInstance(provider, ConverterType, instance, methodInfo);
        }

        protected virtual object ResolveArgument(IServiceProvider provider)
        {
            object argument = null;

            if (ArgumentType is not null)
            {
                var actualArgument = provider.GetService(typeof(IOptions<>).MakeGenericType(ArgumentType));

                argument = actualArgument is not null
                    ? Versioned.CallByName(actualArgument, "Value", CallType.Get) 
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