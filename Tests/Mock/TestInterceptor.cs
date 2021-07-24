using System.Collections.Generic;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;

namespace Tests.Mock
{
    public class TestInterceptor : IMessageInterceptor
    {
        public List<KafkaInterception> ProduceInterceptions { get; } = new();
        
        public List<KafkaInterception> ConsumeInterceptions { get; } = new();

        public Task ConsumeAsync(KafkaInterception interception)
        {
            ConsumeInterceptions.Add(interception);
            return Task.CompletedTask;
        }

        public Task ProduceAsync(KafkaInterception interception)
        {
            ProduceInterceptions.Add(interception);
            return Task.CompletedTask;
        }

        public void Bounce()
        {
            ProduceInterceptions.Clear();
            ConsumeInterceptions.Clear();
        }
    }
}