using System;

namespace AspNetCore.Kafka.Automation.Attributes
{
    public class RetryOptions
    {
        public static RetryOptions Infinite(int delay = 0) => new() { Count = -1, Delay = delay };
        
        public static RetryOptions WithCount(int count, int delay = 0) => new() { Count = count, Delay = delay };
            
        public int Count { get; set; } = -1;

        public int Delay { get; set; }
    }
    
    [AttributeUsage(AttributeTargets.Method)]
    public class RetryAttribute : MessagePolicyAttribute
    {
        public RetryAttribute() { }

        public RetryAttribute(int count, int delay)
        {
            Count = count;
            Delay = delay;
        }
        
        public RetryAttribute(int count) => Count = count;
            
        public int Count { set => Options.Count = value; }

        public int Delay
        {
            set
            {
                if (value < 0)
                    throw new ArgumentException("Retry delay value must be positive or zero");
                        
                Options.Delay = value;
            }
        }

        public RetryOptions Options { get; } = new();
    }
}