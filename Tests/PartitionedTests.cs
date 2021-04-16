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
    public class PartitionedTests : TestServerFixture
    {
        public PartitionedTests(ITestOutputHelper log) : base(log)
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
                //.Partitioned()
                .Batch(batchSize, TimeSpan.FromMilliseconds(batchTime))
                .Action(async messages =>
                {
                    await sink.Batch(messages);
                    Log($"Received Batch = {messages.Count()}");
                })
                .Subscribe(topic);
            
        }
    }
}