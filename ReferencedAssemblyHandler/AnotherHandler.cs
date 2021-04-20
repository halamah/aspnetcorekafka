using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Attributes;

namespace ReferencedAssemblySample
{
    [Message(Topic = "AnotherAssemblyTopic")]
    public class AnotherMessage
    {
        
    }
    
    public class AnotherHandler : IMessageHandler<AnotherMessage>
    {
        public Task HandleAsync(AnotherMessage message)
        {
            return Task.CompletedTask;
        }
    }
}