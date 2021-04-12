using System;
using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Attributes
{
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    public class MessageBlockAttribute : Attribute
    {
        public MessageBlockAttribute(Type type, Type argumentType = null, BlockStage stage = BlockStage.Transform)
        {
            Type = type;
            ArgumentType = argumentType;
            Stage = stage;
        }
        
        public Type Type { get; }
        
        public Type ArgumentType { get; }

        public BlockStage Stage { get; }
    }
}