using System;

namespace AspNetCore.Kafka.Attributes
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Interface)]
    public class MessageHandlerAttribute : Attribute
    {
    }
}