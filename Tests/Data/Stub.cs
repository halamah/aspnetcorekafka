using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AspNetCore.Kafka;
using AspNetCore.Kafka.Abstractions;

namespace Tests.Data
{
    public class Stub
    {
        private int _id;

        public ConcurrentBag<IEnumerable<StubMessage>> ConsumedBatches { get; } = new();

        public ConcurrentBag<StubMessage> Consumed { get; } = new();

        private HashSet<StubMessage> GenerateMessages(int count)
            => Enumerable.Range(0, count).Select(x => new StubMessage
                {
                    Index = ++_id
                })
                .ToHashSet();

        public async Task<HashSet<StubMessage>> Produce(IKafkaProducer producer, int count, string topic = null)
        {
            var messages = GenerateMessages(count);
            await Task.WhenAll(messages.Select(x => producer.ProduceAsync(topic, x)));
            return messages;
        }

        public Task ConsumeMessage(IMessage<StubMessage> message)
        {
            Consumed.Add(message.Value);
            return Task.CompletedTask;
        }
        
        public Task ConsumeBatch(IMessageEnumerable<StubMessage> messages)
        {
            ConsumedBatches.Add(messages.Select(x => x.Value));

            foreach (var message in messages)
            {
                Consumed.Add(message.Value);
            }

            return Task.CompletedTask;
        }
    }
}