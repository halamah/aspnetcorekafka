using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Attributes;

namespace ReferencedAssemblySample
{
    [Message(Topic = "event.payments.deposit.changed1-{env}")]
    public class AnotherMessage
    {
        public string Id { get; set; }

        public string TransactionType { get; set; }
    }
    
    public class AnotherHandler : IMessageHandler<AnotherMessage>
    {
        public Task HandleAsync(AnotherMessage message)
        {
            return Task.CompletedTask;
        }
    }
}