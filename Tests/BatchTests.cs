using System;
using System.Linq;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Client.Consumer;
using AspNetCore.Kafka.Mock.Abstractions;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Tests.Data;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class BatchOptions
    {
        public BatchOptions(int size = 0, int time = 0, bool commit = false)
        {
            Size = size;
            Time = time;
            Commit = commit;
        }

        public int Size { get; }
        public int Time { get; }
        public bool Commit { get; }
    }
    
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
            const string topic = "test";
            var sink = Sink<StubMessage>.Create(Logger, x => Log("Received"));
            
            var consumer = Services.GetRequiredService<IKafkaConsumer>();
            var producer = Services.GetRequiredService<IKafkaProducer>();
            var broker = Services.GetRequiredService<IKafkaMemoryBroker>();
            
            consumer.Pipeline<StubMessage>(topic)
                .Batch(batchSize, TimeSpan.FromMilliseconds(100))
                .Action(async messages =>
                {
                    await sink.Batch(messages);
                    Log($"Received Batch = {messages.Count()}");
                })
                .Subscribe();
            
            await Task.WhenAll(Enumerable.Range(0, batchCount * batchSize)
                .Select(_ => producer.ProduceAsync(topic, Sink<StubMessage>.NewMessage)));
            
            await producer.ProduceAsync(topic, Sink<StubMessage>.NewMessage);

            await sink.Received(batchCount)
                .Batch(Arg.Is<IMessageEnumerable<StubMessage>>(x => x.Count() == batchSize));
            
            await sink.DidNotReceive()
                .Batch(Arg.Is<IMessageEnumerable<StubMessage>>(x => x.Count() != batchSize));
            
            sink.ClearReceivedCalls();
            
            await Task.Delay(70);
            
            await sink.DidNotReceiveWithAnyArgs().Batch(null);
            
            await Task.Delay(70);
            
            await sink.Received(1).Batch(Arg.Is<IMessageEnumerable<StubMessage>>(x => x.Count() == 1));
            
            await sink.DidNotReceive().Batch(Arg.Is<IMessageEnumerable<StubMessage>>(x => x.Count() != 1));
        }

        [Fact]
        public async Task RandomBatches()
        {
            const string topic = "test";
            const int batchSize = 10;
            
            var consumer = Services.GetRequiredService<IKafkaConsumer>();
            var producer = Services.GetRequiredService<IKafkaProducer>();
            
            var sink = Sink<StubMessage>.Create(Logger);
            
            consumer.Pipeline<StubMessage>(topic)
                .Batch(batchSize, TimeSpan.FromMilliseconds(1000))
                .Action(async messages =>
                {
                    await sink.Batch(messages);
                    Log($"Received Batch = {messages.Count()}");
                })
                .Subscribe();
            
            var count = await Generator.Run(
                Logger, 
                () => producer.ProduceAsync(topic, Sink<StubMessage>.NewMessage),
                TimeSpan.FromSeconds(5), 
                TimeSpan.FromMilliseconds(200));
            
            await Task.Delay(1000);
            
            Log($"Generated {count} calls");

            sink.TotalMessages().Should().Be(count);
        }
    }
}