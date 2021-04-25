using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Automation.Pipeline;
using AspNetCore.Kafka.Data;
using FluentAssertions;
using Tests.Data;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class TestMessageHandler : IMessageHandler
    {
        public HashSet<StubMessage> Consumed { get; } = new();

        [Message(Topic = "test")]
        public async Task Handler(StubMessage message)
        {
            if(message.Id == Guid.Empty)
                await Task.Delay(15000);
            
            Consumed.Add(message);
        }
    }
    
    public class ProduceConsumeTests : TestServerFixture
    {
        public ProduceConsumeTests(ITestOutputHelper log) : base(log)
        { }

        [Fact]
        public async Task ConsumeByDeclaration()
        {
            var topic = Broker.GetTopic("test");
            var stub = new Stub();
            
            var produced = await stub.Produce(Producer, 471, topic.Name);
            
            var subscriptions = await Manager
                .SubscribeFromAssembliesAsync(new[] {typeof(TestMessageHandler).Assembly},
                    x => x == typeof(TestMessageHandler));

            subscriptions.Count().Should().Be(1);

            await topic.WhenConsumedAll();
            await Task.Delay(100);
            await Consumer.Complete();
            
            topic.ConsumedCount.Should().Be(produced.Count);
            topic.ProducedCount.Should().Be(produced.Count);
            
            var handler = (TestMessageHandler) Manager.GetServiceOrCreateInstance(typeof(TestMessageHandler));
            handler!.Consumed.Should().BeEquivalentTo(produced);
        }
        
        [Fact]
        public async Task UnsubscribeNoPipeline()
        {
            var signal = new ManualResetEvent(false);
            const int messageDelay = 5000;
            
            await Producer.ProduceAsync(nameof(UnsubscribeNoPipeline), new StubMessage());
            
            var sw = Stopwatch.StartNew();
            
            Consumer.Subscribe<StubMessage>(nameof(UnsubscribeNoPipeline), x =>
            {
                signal.Set();
                return Task.Delay(messageDelay);
            });

            signal.WaitOne(1000).Should().Be(true);

            await Consumer.Complete(10000);
            
            sw.ElapsedMilliseconds.Should().BeGreaterOrEqualTo(messageDelay);
        }
        
        [Fact]
        public async Task SkipRetryPolicy()
        {
            var topic = Broker.GetTopic(nameof(SkipRetryPolicy));
            var stub = new Stub();

            var produced = await stub.Produce(Producer, 4, topic.Name);
            
            Consumer
                .Message<StubMessage>()
                .Action(x => throw new Exception(), Failure.Skip)
                .Action(stub.ConsumeMessage)
                .Subscribe(topic.Name);

            await topic.WhenConsumedAll();
            await Task.Delay(100);
            await Consumer.Complete();

            topic.ConsumedCount.Should().Be(produced.Count);
            stub.Consumed.Should().BeEquivalentTo(produced);
        }
        
        [Fact]
        public async Task RetryPolicy()
        {
            var topic = Broker.GetTopic(nameof(SkipRetryPolicy));
            var stub = new Stub();

            await stub.Produce(Producer, 4, topic.Name);
            
            Consumer
                .Message<StubMessage>()
                .Action(x => throw new Exception())
                .Action(stub.ConsumeMessage)
                .Subscribe(topic.Name);

            await Task.Delay(2000);

            topic.ConsumedCount.Should().BeInRange(1, 2);
            stub.Consumed.Should().BeEmpty();
        }

        [Fact]
        public async Task ProduceConsume()
        {
            var topic = Broker.GetTopic(nameof(ProduceConsume));
            const int bufferSize = 20;

            var consumed = new HashSet<StubMessage>();
            var stub = new Stub();
            
            var produced = await stub.Produce(Producer, 311, topic.Name);

            Consumer
                .Message<StubMessage>()
                .Buffer(bufferSize)
                .Action(x => consumed.Add(x.Value))
                .Subscribe(topic.Name);

            await topic.WhenConsumedAll();
            await Task.Delay(100);
            await Consumer.Complete();
            
            topic.ConsumedCount.Should().Be(produced.Count);
            topic.ProducedCount.Should().Be(produced.Count);
            
            consumed.Count.Should().Be(produced.Count);
            consumed.Should().BeEquivalentTo(produced);
        }
    }
}