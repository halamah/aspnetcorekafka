using System;
using System.Linq;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Client.Consumer;
using AspNetCore.Kafka.Client.Consumer.Pipeline;
using AspNetCore.Kafka.Mock.Abstractions;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
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
            const int batchSize = 5;
            const int batchCount = 30;
            const int batchTime = 500;
            const string topic = "test";
            var sink = Sink<StubMessage>.Create(x => Log("Received"));
            
            var consumer = Services.GetRequiredService<IKafkaConsumer>();
            var producer = Services.GetRequiredService<IKafkaProducer>();
            var broker = Services.GetRequiredService<IKafkaMemoryBroker>();
            
            consumer
                .Pipeline<StubMessage>()
                .Batch(batchSize, TimeSpan.FromMilliseconds(batchTime))
                .Action(async messages =>
                {
                    await sink.Batch(messages);
                    Log($"Received Batch = {messages.Count()}");
                })
                .Subscribe(topic);
            
            await Task.WhenAll(Enumerable.Range(0, batchCount * batchSize)
                .Select(_ => producer.ProduceAsync(topic, Sink<StubMessage>.NewMessage)));
            
            await producer.ProduceAsync(topic, Sink<StubMessage>.NewMessage);

            await Task.Delay(batchTime * 2);
            
            await sink.Received(batchCount)
                .Batch(Arg.Is<IMessageEnumerable<StubMessage>>(x => x.Count() == batchSize));

            await sink.Received(1).Batch(Arg.Is<IMessageEnumerable<StubMessage>>(x => x.Count() == 1));
            
            await sink.DidNotReceive()
                .Batch(Arg.Is<IMessageEnumerable<StubMessage>>(x => x.Count() != batchSize && x.Count() != 1));
        }

        [Fact]
        public async Task RandomBatches()
        {
            const string topic = "test";
            const int batchSize = 10;
            const int batchTime = 500;
            
            var consumer = Services.GetRequiredService<IKafkaConsumer>();
            var producer = Services.GetRequiredService<IKafkaProducer>();
            
            var sink = Sink<StubMessage>.Create();
            
            consumer
                .Pipeline<StubMessage>()
                .Batch(batchSize, TimeSpan.FromMilliseconds(batchTime))
                .Action(async messages =>
                {
                    await sink.Batch(messages);
                    Log($"Received Batch = {messages.Count()}");
                })
                .Subscribe(topic);
            
            var count = await Generator.Run(
                Logger, 
                () => producer.ProduceAsync(topic, Sink<StubMessage>.NewMessage),
                TimeSpan.FromSeconds(5), 
                TimeSpan.FromMilliseconds(200));
            
            await Task.Delay(batchTime * 2);
            
            Log($"Generated {count} calls");

            sink.TotalMessages().Should().Be(count);
        }
    }
}