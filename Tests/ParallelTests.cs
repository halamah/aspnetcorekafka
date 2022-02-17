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
using Microsoft.Extensions.DependencyInjection;
using Tests.Data;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class ParallelTests : IClassFixture<TestServer>
    {
        private readonly TestServer _server;

        public ParallelTests(TestServer server, ITestOutputHelper output)
        {
            _server = server.SetOutput(output);
        }

        [Fact]
        public async Task Parallel()
        {
            var broker = _server.Services.GetRequiredService<IKafkaMemoryBroker>();
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            var consumer = _server.Services.GetRequiredService<IKafkaConsumer>();
            
            var topic = broker.GetTopic(nameof(Parallel));
            var stub = new Stub();
            const int batchSize = 5;
            const int batchTime = 500;

            topic.PartitionsCount = 5;
            
            var produced = await stub.Produce(producer, topic.PartitionsCount * 2, topic.Name);
            
            var subscription = consumer
                .Message<StubMessage>()
                .AsParallel()
                .Batch(batchSize, TimeSpan.FromMilliseconds(batchTime))
                .Action(stub.ConsumeBatch)
                .Action(async messages =>
                {
                    _server.Output.WriteLine($"Received Batch = {messages.Count()}, Partition = {messages.First().Partition}");
                    await Task.Delay(batchTime);
                })
                .Subscribe(topic.Name);

            await topic.WhenConsumedAll();
            await Task.Delay(batchTime * 2);
            await subscription.UnsubscribeAsync();

            stub.ConsumedBatches.Count.Should().BeGreaterThan(1);
            stub.Consumed.Count.Should().Be(produced.Count);
        }
        
        [Fact]
        public async Task Unsubscribe()
        {
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            var consumer = _server.Services.GetRequiredService<IKafkaConsumer>();
            
            const int messageDelay = 5000;
            var signal = new ManualResetEvent(false);

            await producer.ProduceAsync(nameof(Unsubscribe), new StubMessage());
            
            var sw = Stopwatch.StartNew();
            
            var subscription = consumer
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

            await subscription.UnsubscribeAsync();
            
            sw.ElapsedMilliseconds.Should().BeInRange(messageDelay, messageDelay * 15 / 10);
        }
    }
}