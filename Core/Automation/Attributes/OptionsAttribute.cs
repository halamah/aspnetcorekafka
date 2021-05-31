using System;
using System.Linq;

namespace AspNetCore.Kafka.Automation.Attributes
{
    [Flags]
    public enum Option
    {
        None = 0,
        SkipNullMessages = 0x0001,
        RetryFailure = 0x0002,
        SkipFailure = 0x0004,
    }
    
    public class OptionsAttribute : MessagePolicyAttribute
    {
        public Option Flags { get; }

        public OptionsAttribute(Option flags) => Flags = flags;
        
        public OptionsAttribute(params Option[] flags) => Flags = (Option) flags.Aggregate(0, (p, c) => p | (int) c);
    }
    
    public static class HandlerFlagsExtensions
    {
        public static bool IsSet(this Option flags, Option value) => (flags & value) != 0;
    }
}