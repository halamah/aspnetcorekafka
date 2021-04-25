using System;
using System.Threading.Tasks;
using AspNetCore.Kafka.Automation.Pipeline;
using FluentAssertions;
using Tests.Data;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class BatchTests : TestServerFixture
    {
        public BatchTests(ITestOutputHelper log) : base(log)
        {
        }

        [Fact]
        public async Task BatchSeries()
        {
            var topic = Broker.GetTopic(nameof(BatchSeries));
            const int batchSize = 5;
            const int batchCount = 30;
            const int batchTime = 500;

            var stub = new Stub();
            
            await stub.Produce(Producer, batchCount * batchSize, topic.Name);

            Consumer
                .Message<StubMessage>()
                .Batch(batchSize, TimeSpan.FromMilliseconds(batchTime))
                .Action(stub.ConsumeBatch)
                .Subscribe(topic.Name);
            
            await stub.Produce(Producer, 1, topic.Name);

            await topic.WhenConsumedAll();
            await Task.Delay(100);
            await Consumer.Complete();
            
            stub.Consumed.Count.Should().Be(batchCount * batchSize + 1);
            stub.ConsumedBatches.Count.Should().Be(batchCount + 1);
        }

        [Fact]
        public async Task RandomBatches()
        {
            var topic = Broker.GetTopic(nameof(RandomBatches));
            const int batchSize = 10;
            const int batchTime = 500;
            
            var stub = new Stub();
            
            var count = await Generator.Run(
                () => stub.Produce(Producer, 1, topic.Name),
                TimeSpan.FromSeconds(5), 
                TimeSpan.FromMilliseconds(200));
            
            Consumer
                .Message<StubMessage>()
                .Batch(batchSize, TimeSpan.FromMilliseconds(batchTime))
                .Action(stub.ConsumeBatch)
                .Subscribe(topic.Name);

            await topic.WhenConsumedAll();
            await Task.Delay(1000);
            await Consumer.Complete();
            
            Log($"Generated {count} calls");

            stub.Consumed.Count.Should().Be(count);
        }
    }
}