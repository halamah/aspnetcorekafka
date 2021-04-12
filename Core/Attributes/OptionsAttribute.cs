using System;

namespace AspNetCore.Kafka.Attributes
{
    public class OptionsAttribute : Attribute
    {
        public OptionsAttribute(Type type)
        {
            Type = type;
        }

        public Type Type { get; set; }
    }
}