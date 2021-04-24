using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Automation.Attributes
{
    public class FailuresAttribute : MessagePolicyAttribute
    {
        public Failure Behavior { get; }

        public FailuresAttribute(Failure behavior)
        {
            Behavior = behavior;
        }
    }
    
    public class SkipFailureAttribute : FailuresAttribute
    {
        public SkipFailureAttribute() : base(Failure.Skip) { }
    }
    
    public class RetryOnFailureAttribute : FailuresAttribute
    {
        public RetryOnFailureAttribute() : base(Failure.Retry) { }
    }
}