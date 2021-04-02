using System;
using System.Linq;
using AspNetCore.Kafka.Attributes;
using Microsoft.VisualBasic;
using Microsoft.VisualBasic.CompilerServices;

namespace AspNetCore.Kafka.Data
{
    public static class TopicTypeDefinition
    {
        public static MessageAttribute FromType<T>()
            => typeof(T)
                .GetCustomAttributes(typeof(MessageAttribute), true)
                .Cast<MessageAttribute>()
                .FirstOrDefault();
        
        public static MessageAttribute FromType(Type type)
            => type
                .GetCustomAttributes(typeof(MessageAttribute), true)
                .Cast<MessageAttribute>()
                .FirstOrDefault();

        public static string GetMessageKey(this MessageAttribute message, object value)
            => message?
                .Id?
                .ToString()?
                .Split('.')
                .Aggregate(value, (from, name) => Versioned.CallByName(from, name, CallType.Get))?
                .ToString();
    }
}