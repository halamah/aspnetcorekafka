using System;
using System.Linq;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Client.Consumer.Pipeline;
using AspNetCore.Kafka.Mock.Abstractions;
using Microsoft.Extensions.DependencyInjection;
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
            
            var consumer = Services.GetRequiredService<IKafkaConsumer>();
            var producer = Services.GetRequiredService<IKafkaProducer>();
            var broker = Services.GetRequiredService<IKafkaMemoryBroker>();
            
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
    }
}