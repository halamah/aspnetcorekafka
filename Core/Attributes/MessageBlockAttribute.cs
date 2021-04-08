using System;

namespace AspNetCore.Kafka.Attributes
{
    [AttributeUsage(AttributeTargets.Method)]
    public class MessageBlockAttribute : Attribute
    {
        public MessageBlockAttribute(Type blockType, Type argumentType = null)
        {
            BlockType = blockType;
            ArgumentType = argumentType;
        }
        
        public Type BlockType { get; }
        
        public Type ArgumentType { get; }
    }
}