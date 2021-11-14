using System.Collections.Generic;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;

namespace Tests.Mock
{
    public class TestInterceptor : IMessageInterceptor
    {
        public List<KafkaInterception> Produced { get; } = new();
        
        public List<KafkaInterception> Consumed { get; } = new();

        public Task ConsumeAsync(KafkaInterception interception)
        {
            Consumed.Add(interception);
            return Task.CompletedTask;
        }

        public Task ProduceAsync(KafkaInterception interception)
        {
            Produced.Add(interception);
            return Task.CompletedTask;
        }

        public void Bounce()
        {
            Produced.Clear();
            Consumed.Clear();
        }
    }
}