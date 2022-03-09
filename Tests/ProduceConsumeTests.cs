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
using AspNetCore.Kafka.Mock.Abstractions;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Tests.Data;
using Tests.Mock;
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
    
    public class ProduceConsumeTests : IClassFixture<TestServer>
    {
        private readonly TestServer _server;

        public ProduceConsumeTests(TestServer server, ITestOutputHelper output)
        {
            _server = server.SetOutput(output);
        }

        [Fact]
        public Task Produce_No_Topic_Definition()
        {
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            
            producer.Awaiting(x => x.ProduceAsync(" ", new object())).Should().ThrowAsync<ArgumentException>();
            producer.Awaiting(x => x.ProduceAsync(new object())).Should().ThrowAsync<ArgumentException>();
            producer.Awaiting(x => x.ProduceAsync("test", new object())).Should().ThrowAsync<ArgumentException>();
            
            return Task.CompletedTask;
        }

        [Fact]
        public async Task ConsumeByDeclaration()
        {
            var broker = _server.Services.GetRequiredService<IKafkaMemoryBroker>();
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            var manager = _server.Services.GetRequiredService<ISubscriptionManager>();
            
            var topic = broker.GetTopic("test");
            var stub = new Stub();
            
            var produced = await stub.Produce(producer, 471, topic.Name);
            
            var subscriptions = await manager
                .SubscribeFromAssembliesAsync(new[] {typeof(TestMessageHandler).Assembly},
                    x => x == typeof(TestMessageHandler));

            subscriptions.Count().Should().Be(1);

            await topic.WhenConsumedAll();
            await Task.Delay(100);

            foreach (var subscription in subscriptions)
                await subscription.UnsubscribeAsync();

            topic.Consumed.Count().Should().Be(produced.Count);
            topic.Produced.Count().Should().Be(produced.Count);
            
            //var handler = (TestMessageHandler) Manager.GetServiceOrCreateInstance(typeof(TestMessageHandler));
            //handler!.Consumed.Should().BeEquivalentTo(produced);
        }
        
        [Fact]
        public async Task UnsubscribeNoPipeline()
        {
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            var consumer = _server.Services.GetRequiredService<IKafkaConsumer>();
            
            var signal = new ManualResetEvent(false);
            const int messageDelay = 5000;
            
            await producer.ProduceAsync(nameof(UnsubscribeNoPipeline), new StubMessage());
            
            var sw = Stopwatch.StartNew();
            
            var subscription = consumer.Subscribe<StubMessage>(nameof(UnsubscribeNoPipeline), x =>
            {
                signal.Set();
                return Task.Delay(messageDelay);
            });

            signal.WaitOne(1000).Should().Be(true);

            await subscription.UnsubscribeAsync();
            
            sw.ElapsedMilliseconds.Should().BeGreaterOrEqualTo(messageDelay);
        }
        
        [Fact]
        public async Task SkipRetryPolicy()
        {
            var broker = _server.Services.GetRequiredService<IKafkaMemoryBroker>();
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            var consumer = _server.Services.GetRequiredService<IKafkaConsumer>();
            
            var topic = broker.GetTopic(nameof(SkipRetryPolicy));
            var stub = new Stub();

            var produced = await stub.Produce(producer, 4, topic.Name);
            
            var subscription = consumer
                .Message<StubMessage>()
                .Action(x => throw new Exception())
                .Action(stub.ConsumeMessage)
                .Subscribe(topic.Name);

            await topic.WhenConsumedAll();
            await Task.Delay(100);
            await subscription.UnsubscribeAsync();

            topic.Consumed.Count().Should().Be(produced.Count);
            stub.Consumed.Should().BeEquivalentTo(produced);
        }
        
        [Fact]
        public async Task RetryPolicy()
        {
            var broker = _server.Services.GetRequiredService<IKafkaMemoryBroker>();
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            var consumer = _server.Services.GetRequiredService<IKafkaConsumer>();
            var interceptor = (TestInterceptor) _server.Services.GetRequiredService<IMessageInterceptor>();
            
            var topic = broker.GetTopic(nameof(RetryPolicy));
            var stub = new Stub();

            await stub.Produce(producer, 4, topic.Name);

            interceptor.Bounce();
            
            var subscription = consumer
                .Message<StubMessage>()
                .Action(_ => throw new Exception(), RetryOptions.Infinite(50))
                .Action(stub.ConsumeMessage)
                .Subscribe(topic.Name);

            await Task.Delay(2000);
            await subscription.UnsubscribeAsync();

            interceptor.Consumed.Should().HaveCount(1);
            topic.Consumed.Count().Should().Be(2);
            stub.Consumed.Should().BeEmpty();
        }
        
        [Fact]
        public async Task IgnoreNullMessage()
        {
            var broker = _server.Services.GetRequiredService<IKafkaMemoryBroker>();
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            var consumer = _server.Services.GetRequiredService<IKafkaConsumer>();
            
            var topic = broker.GetTopic(nameof(IgnoreNullMessage));
            var stub = new Stub();

            await producer.ProduceAsync<StubMessage>(topic.Name, null);
            
            var subscription = consumer
                .Message<StubMessage>()
                .Where(x => x is not null)
                .Action(stub.ConsumeMessage)
                .Subscribe(topic.Name);

            await topic.WhenConsumedAll();
            await Task.Delay(100);
            await subscription.UnsubscribeAsync();
            
            topic.Consumed.Count().Should().Be(1);
            stub.Consumed.Should().BeEmpty();
        }
        
        [Fact]
        public async Task IgnoreNullMessageInBatch()
        {
            var broker = _server.Services.GetRequiredService<IKafkaMemoryBroker>();
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            var consumer = _server.Services.GetRequiredService<IKafkaConsumer>();
            
            const int batchSize = 10;
            var topic = broker.GetTopic(nameof(IgnoreNullMessageInBatch));
            var stub = new Stub();
            
            topic.Consumed.Should().BeEmpty();

            for (var i = 0; i < batchSize; i++)
                await producer.ProduceAsync<StubMessage>(topic.Name, null);
            
            var subscription = consumer
                .Message<StubMessage>()
                .Where(x => x is not null)
                .Batch(batchSize, 100)
                .Action(stub.ConsumeBatch)
                .Subscribe(topic.Name);

            await topic.WhenConsumedAll();
            await Task.Delay(100);
            await subscription.UnsubscribeAsync();
            
            topic.Consumed.Count().Should().Be(batchSize);
            stub.Consumed.Should().BeEmpty();
        }

        [Fact]
        public async Task ProduceConsume()
        {
            var broker = _server.Services.GetRequiredService<IKafkaMemoryBroker>();
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            var consumer = _server.Services.GetRequiredService<IKafkaConsumer>();
            
            var topic = broker.GetTopic(nameof(ProduceConsume));
            const int bufferSize = 20;

            var consumed = new HashSet<StubMessage>();
            var stub = new Stub();
            
            var produced = await stub.Produce(producer, 311, topic.Name);

            var subscription = consumer
                .Message<StubMessage>()
                .Buffer(bufferSize)
                .Action(x => consumed.Add(x.Value))
                .Subscribe(topic.Name);

            await topic.WhenConsumedAll();
            await Task.Delay(100);
            await subscription.UnsubscribeAsync();
            
            topic.Consumed.Count().Should().Be(produced.Count);
            topic.Produced.Count().Should().Be(produced.Count);
            
            consumed.Count().Should().Be(produced.Count);
            consumed.Should().BeEquivalentTo(produced);
        }
        
        [Fact]
        public async Task Offset()
        {
            var broker = _server.Services.GetRequiredService<IKafkaMemoryBroker>();
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            var consumer = _server.Services.GetRequiredService<IKafkaConsumer>();
            
            var topic = broker.GetTopic(nameof(Offset));
            const int bufferSize = 20;

            var consumed = new HashSet<StubMessage>();
            var stub = new Stub();
            
            var produced = await stub.Produce(producer, 100, topic.Name);

            var subscription = consumer
                .Message<StubMessage>()
                .Buffer(bufferSize)
                .Action(x => consumed.Add(x.Value))
                .Subscribe(topic.Name);

            await topic.WhenConsumedAll();
            await Task.Delay(100);
            await subscription.UnsubscribeAsync();
            
            topic.Consumed.Count().Should().Be(produced.Count);
            topic.Produced.Count().Should().Be(produced.Count);
            
            consumed.Count().Should().Be(produced.Count);
            consumed.Should().BeEquivalentTo(produced);
        }

        [Fact]
        public async Task Dynamic()
        {
            var broker = _server.Services.GetRequiredService<IKafkaMemoryBroker>();
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            var consumer = _server.Services.GetRequiredService<IKafkaConsumer>();
            var topic = Guid.NewGuid().ToString();
            var stub = new Stub();
            
            var produced = await stub.Produce(producer, 100, topic);
            
            var subscription = consumer
                .Message<dynamic>()
                .Action(x =>
                {
                    _server.Output.WriteLine(x?.Value.id.ToString());
                    return Task.CompletedTask;
                })
                .Subscribe(topic);
            
            await Task.Delay(10000);
            await subscription.UnsubscribeAsync();
        }
    }
}