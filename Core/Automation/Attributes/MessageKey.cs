using System;

namespace AspNetCore.Kafka.Automation.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public class MessageKey : Attribute
    {
    }
}