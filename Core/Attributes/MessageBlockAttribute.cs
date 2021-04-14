using System;

namespace AspNetCore.Kafka.Attributes
{
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    public class MessageBlockAttribute : Attribute
    {
    }
}