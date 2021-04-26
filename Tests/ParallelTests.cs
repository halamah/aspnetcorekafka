using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Pipeline;
using AspNetCore.Kafka.Mock.Abstractions;
using FluentAssertions;
using Tests.Data;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class ParallelTests : TestServerFixture
    {
        public ParallelTests(ITestOutputHelper log) : base(log)
        {
        }

        [Fact]
        public async Task Parallel()
        {
            var topic = Broker.GetTopic(nameof(Parallel));
            var stub = new Stub();
            const int batchSize = 5;
            const int batchTime = 500;

            topic.PartitionsCount = 5;
            
            var produced = await stub.Produce(Producer, topic.PartitionsCount * 2, topic.Name);
            
            Consumer
                .Message<StubMessage>()
                .AsParallel()
                .Batch(batchSize, TimeSpan.FromMilliseconds(batchTime))
                .Action(async messages =>
                {
                    Log($"Received Batch = {messages.Count()}, Partition = {messages.First().Partition}");
                    await Task.Delay(500);
                })
                .Action(stub.ConsumeBatch)
                .Subscribe(topic.Name);

            await topic.WhenConsumedAll();
            await Task.Delay(100);
            await Consumer.Complete();

            stub.Consumed.Count.Should().Be(produced.Count);
        }
        
        [Fact]
        public async Task Unsubscribe()
        {
            const int messageDelay = 5000;
            var signal = new ManualResetEvent(false);

            await Producer.ProduceAsync(nameof(Unsubscribe), new StubMessage());
            
            var sw = Stopwatch.StartNew();
            
            Consumer
                .Message<StubMessage>()
                .Buffer(100)
                .AsParallel()
                .Action(async x =>
                {
                    signal.Set();
                    await Task.Delay(messageDelay);
                })
                .Subscribe(nameof(Unsubscribe));

            signal.WaitOne(1000).Should().Be(true);

            await Consumer.Complete(10000);
            
            sw.ElapsedMilliseconds.Should().BeInRange(messageDelay, messageDelay * 15 / 10);
        }
    }
}