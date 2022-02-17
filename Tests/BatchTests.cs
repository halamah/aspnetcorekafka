using System;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Pipeline;
using AspNetCore.Kafka.Mock.Abstractions;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Tests.Data;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class BatchTests : IClassFixture<TestServer>
    {
        private readonly TestServer _server;

        public BatchTests(TestServer server, ITestOutputHelper output)
        {
            _server = server.SetOutput(output);
        }

        [Fact]
        public async Task BatchSeries()
        {
            var broker = _server.Services.GetRequiredService<IKafkaMemoryBroker>();
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            var consumer = _server.Services.GetRequiredService<IKafkaConsumer>();
            
            var topic = broker.GetTopic(nameof(BatchSeries));
            const int batchSize = 5;
            const int batchCount = 30;
            const int batchTime = 500;

            var stub = new Stub();
            
            await stub.Produce(producer, batchCount * batchSize, topic.Name);

            var subscription = consumer
                .Message<StubMessage>()
                .Batch(batchSize, TimeSpan.FromMilliseconds(batchTime))
                .Action(stub.ConsumeBatch)
                .Subscribe(topic.Name);
            
            await stub.Produce(producer, 1, topic.Name);

            await topic.WhenConsumedAll();
            await Task.Delay(100);
            await subscription.UnsubscribeAsync();
            
            stub.Consumed.Count.Should().Be(batchCount * batchSize);
            stub.ConsumedBatches.Count.Should().Be(batchCount);
        }

        [Fact]
        public async Task RandomBatches()
        {
            var broker = _server.Services.GetRequiredService<IKafkaMemoryBroker>();
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            var consumer = _server.Services.GetRequiredService<IKafkaConsumer>();
            
            var topic = broker.GetTopic(nameof(RandomBatches));
            const int batchSize = 10;
            const int batchTime = 500;
            
            var stub = new Stub();
            
            var count = await Generator.Run(
                () => stub.Produce(producer, 1, topic.Name),
                TimeSpan.FromSeconds(5), 
                TimeSpan.FromMilliseconds(200));
            
            var subscription = consumer
                .Message<StubMessage>()
                .Batch(batchSize, TimeSpan.FromMilliseconds(batchTime))
                .Action(stub.ConsumeBatch)
                .Subscribe(topic.Name);

            await topic.WhenConsumedAll();
            await Task.Delay(1000);
            await subscription.UnsubscribeAsync();
            
            _server.Output.WriteLine($"Generated {count} calls");

            stub.Consumed.Count.Should().Be(count);
        }
    }
}