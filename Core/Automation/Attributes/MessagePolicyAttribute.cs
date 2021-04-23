using System;

namespace AspNetCore.Kafka.Automation.Attributes
{
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    public abstract class MessagePolicyAttribute : Attribute
    {
    }
}