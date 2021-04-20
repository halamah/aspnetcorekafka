using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Automation.Attributes
{
    public class FailureAttribute : MessageBlockAttribute
    {
        public Failure Behavior { get; }

        public FailureAttribute(Failure behavior)
        {
            Behavior = behavior;
        }
    }
}