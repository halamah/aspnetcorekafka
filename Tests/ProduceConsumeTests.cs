using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Client.Consumer.Pipeline;
using AspNetCore.Kafka.Mock.Abstractions;
using FluentAssertions;
using Tests.Data;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    internal static class TestData
    {
        public const string Topic = "test";
        
        public static readonly HashSet<StubMessage> Messages = Enumerable.Range(0, 103)
            .Select(x => new StubMessage {Index = x, Id = Guid.NewGuid()})
            .ToHashSet();

        public static Task ProduceAll(IKafkaProducer producer, string topic = null) =>
            Task.WhenAll(Messages.Select(x => producer.ProduceAsync(topic ?? Topic, x)));
    }

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
            var service = GetRequiredService<ISubscriptionService>();
            var broker = GetRequiredService<IKafkaMemoryBroker>();
            var producer = GetRequiredService<IKafkaProducer>();
            
            await TestData.ProduceAll(producer);
            
            var subscriptions = await service
                .SubscribeFromAssembliesAsync(new[] {typeof(TestMessageHandler).Assembly},
                    x => x == typeof(TestMessageHandler));

            subscriptions.Count().Should().Be(1);

            await Task.Delay(2000);
            
            broker.ConsumeCount.Should().Be(TestData.Messages.Count);
            broker.ProduceCount.Should().Be(TestData.Messages.Count);

            var handler = (TestMessageHandler) service.GetServiceOrCreateInstance(typeof(TestMessageHandler));
            handler!.Consumed.Should().BeEquivalentTo(TestData.Messages);
        }
        
        //todo:[Fact]
        public async Task UnsubscribePipeline()
        {
            var service = GetRequiredService<ISubscriptionService>();
            var broker = GetRequiredService<IKafkaMemoryBroker>();
            var producer = GetRequiredService<IKafkaProducer>();
            var consumer = GetRequiredService<IKafkaConsumer>();
            var signal = new ManualResetEvent(false);
            
            await TestData.ProduceAll(producer);

            await producer.ProduceAsync(nameof(UnsubscribePipeline), new StubMessage());

            consumer
                .Message<StubMessage>()
                .Buffer(100)
                .Action(x =>
                {
                    signal.Set();
                    return Task.Delay(2000);
                })
                .Subscribe(nameof(UnsubscribePipeline));

            signal.WaitOne(5000);
            var sw = Stopwatch.StartNew();
            
            WaitHandle.WaitAll(await service.UnsubscribeAllAsync());
            sw.ElapsedMilliseconds.Should().BeGreaterOrEqualTo(2000);
        }
        
        [Fact]
        public async Task UnsubscribeBasic()
        {
            var service = GetRequiredService<ISubscriptionService>();
            var producer = GetRequiredService<IKafkaProducer>();
            var consumer = GetRequiredService<IKafkaConsumer>();
            var signal = new ManualResetEvent(false);
            
            await TestData.ProduceAll(producer);

            await producer.ProduceAsync(nameof(UnsubscribeBasic), new StubMessage());
            
            consumer.Subscribe<StubMessage>(nameof(UnsubscribeBasic), x =>
            {
                signal.Set();
                return Task.Delay(2000);
            });

            signal.WaitOne(5000);
            var sw = Stopwatch.StartNew();
            
            WaitHandle.WaitAll(await service.UnsubscribeAllAsync());
            sw.ElapsedMilliseconds.Should().BeGreaterOrEqualTo(2000);
        }

        [Fact]
        public async Task ProduceConsume()
        {
            const int bufferSize = 20;

            var consumed = new HashSet<StubMessage>();
            var signal = new ManualResetEvent(false);
            
            var consumer = GetRequiredService<IKafkaConsumer>();
            var broker = GetRequiredService<IKafkaMemoryBroker>();

            await TestData.ProduceAll(GetRequiredService<IKafkaProducer>());

            consumer.Message<StubMessage>()
                .Buffer(bufferSize)
                .Action(x =>
                {
                    signal.WaitOne(5000);
                    
                    Log($"Received Index = {x.Value.Index} Id = {x.Value.Id} Offset = {x.Offset}");
                    consumed.Add(x.Value);
                    
                    return Task.CompletedTask;
                })
                .Subscribe("test");
            
            await Task.Delay(2000);

            broker.ConsumeCount.Should().Be(bufferSize + 2);
            signal.Set();
            
            await Task.Delay(1000);
            
            broker.ConsumeCount.Should().Be(TestData.Messages.Count);
            broker.ProduceCount.Should().Be(TestData.Messages.Count);
            
            consumed.Count.Should().Be(TestData.Messages.Count);
            consumed.Should().BeEquivalentTo(TestData.Messages);
        }
    }
}