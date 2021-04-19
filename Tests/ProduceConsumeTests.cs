using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Client.Consumer.Pipeline;
using AspNetCore.Kafka.Mock.Abstractions;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Tests.Data;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class TestMessageHandler : IMessageHandler
    {
        [Message(Topic = "test")]
        public Task Handler(StubMessage message)
        {
            return Task.CompletedTask;
        }
    }
    
    public class ProduceConsumeTests : TestServerFixture
    {
        public ProduceConsumeTests(ITestOutputHelper log) : base(log)
        {
        }

        [Fact]
        public async Task ConsumeByDeclaration()
        {
            var subscriptions = Services
                .GetService<ISubscriptionService>()!
                .SubscribeFromAssembliesAsync(new[] {typeof(TestMessageHandler).Assembly},
                    x => x == typeof(TestMessageHandler));
        }

        [Fact]
        public async Task ProduceConsume()
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

            consumer.Message<StubMessage>()
                .Buffer(bufferSize)
                .Action(x =>
                {
                    signal.WaitOne();
                    
                    Log($"Received Index = {x.Value.Index} Id = {x.Value.Id} Offset = {x.Offset}");
                    consumed.Add(x.Value);
                    
                    return Task.CompletedTask;
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