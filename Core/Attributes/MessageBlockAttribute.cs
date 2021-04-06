using System;

namespace AspNetCore.Kafka.Attributes
{
    [AttributeUsage(AttributeTargets.Method)]
    public class MessageBlockAttribute : Attribute
    {
        public MessageBlockAttribute(Type converterType, Type argumentType = null)
        {
            ConverterType = converterType;
            ArgumentType = argumentType;
        }
        
        public Type ConverterType { get; }
        
        public Type ArgumentType { get; }
    }
}