using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Client.Consumer;
using AspNetCore.Kafka.Client.Consumer.Pipeline;
using AspNetCore.Kafka.Mock.Abstractions;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Tests.Data;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class ProduceConsumeTests : TestServerFixture
    {
        public ProduceConsumeTests(ITestOutputHelper log) : base(log)
        {
        }
        
        [Fact]
        public async Task ProduceConsumer()
        {
            const int bufferSize = 20;
            var messages = Enumerable.Range(0, 103)
                .Select(x => new StubMessage {Index = x, Id = Guid.NewGuid()})
                .ToHashSet();
            
            var consumed = new HashSet<StubMessage>();
            var signal = new ManualResetEvent(false);
            
            var consumer = Services.GetRequiredService<IKafkaConsumer>();
            var producer = Services.GetRequiredService<IKafkaProducer>();
            var broker = Services.GetRequiredService<IKafkaMemoryBroker>();

            await Task.WhenAll(messages.Select(x => producer.ProduceAsync("test", x)));

            consumer.Pipeline<StubMessage>()
                .Buffer(bufferSize)
                .Action(async x =>
                {
                    signal.WaitOne();
                    
                    Log($"Received Index = {x.Value.Index} Id = {x.Value.Id} Offset = {x.Offset}");
                    consumed.Add(x.Value);
                })
                .Subscribe("test");
            
            await Task.Delay(2000);

            broker.ConsumeCount.Should().Be(bufferSize + 2);
            signal.Set();
            
            await Task.Delay(1000);
            
            broker.ConsumeCount.Should().Be(messages.Count);
            broker.ProduceCount.Should().Be(messages.Count);
            
            consumed.Count.Should().Be(messages.Count);
            consumed.Should().BeEquivalentTo(messages);
        }
    }
}