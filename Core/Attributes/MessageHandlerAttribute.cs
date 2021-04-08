using System;

namespace AspNetCore.Kafka.Attributes
{
    [AttributeUsage(AttributeTargets.Class)]
    public class MessageHandlerAttribute : Attribute
    {
    }
}