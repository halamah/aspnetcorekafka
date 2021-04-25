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
            const int batchSize = 5;
            const int batchCount = 30;
            const int batchTime = 500;
            const string topic = "test";
            
            var consumer = GetRequiredService<IKafkaConsumer>();
            var producer = GetRequiredService<IKafkaProducer>();
            var broker = GetRequiredService<IKafkaMemoryBroker>();
            
            broker.SetTopicPartitions(topic, 5);
            
            consumer
                .Message<StubMessage>()
                .AsParallel()
                .Batch(batchSize, TimeSpan.FromMilliseconds(batchTime))
                .Action(async messages =>
                {
                    Log($"Received Batch = {messages.Count()}, Partition = {messages.First().Partition}");
                    await Task.Delay(2000);
                })
                .Subscribe(topic);

            await Task.WhenAll(Enumerable.Range(0, batchCount * batchSize)
                .Select(_ =>
                    producer.ProduceAsync(topic, new StubMessage {Id = Guid.NewGuid()}, Guid.NewGuid().ToString())));

            await Task.Delay(5000);
        }
        
        [Fact]
        public async Task Unsubscribe()
        {
            var producer = GetRequiredService<IKafkaProducer>();
            var consumer = GetRequiredService<IKafkaConsumer>();
            var signal = new ManualResetEvent(false);
            const int messageDelay = 5000;
            
            await TestData.ProduceAll(producer);

            await producer.ProduceAsync(nameof(Unsubscribe), new StubMessage());
            
            var sw = Stopwatch.StartNew();
            
            consumer
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

            await consumer.Complete(10000);
            
            sw.ElapsedMilliseconds.Should().BeInRange(messageDelay, messageDelay * 15 / 10);
        }
    }
}