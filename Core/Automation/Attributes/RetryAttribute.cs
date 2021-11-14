using System;

namespace AspNetCore.Kafka.Automation.Attributes
{
    public class RetryOptions
    {
        public static RetryOptions Infinite(int delay = 0) => new() { Retries = -1, Delay = delay };
        
        public static RetryOptions WithCount(int count, int delay = 0) => new() { Retries = count, Delay = delay };
            
        public int Retries { get; set; } = -1;

        public int Delay { get; set; }
    }
    
    [AttributeUsage(AttributeTargets.Method)]
    public class RetryAttribute : MessagePolicyAttribute
    {
        public RetryAttribute() { }

        public RetryAttribute(int retries, int delay)
        {
            Retries = retries;
            Delay = delay;
        }
        
        public RetryAttribute(int retries) => Retries = retries;
            
        public int Retries { set => Options.Retries = value; }

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