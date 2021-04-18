using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Client.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Tests.Data;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class ParallelTests : TestServerFixture
    {
        private readonly ITestOutputHelper _log;

        public ParallelTests(ITestOutputHelper log) : base(log)
        {
            _log = log;
        }

        [Fact]
        public async Task ProcessInOrder()
        {
            const int degreeOfParallelism = 5;
            const int delayMs = 10;
            const string topic = "test";
            
            var consumer = Services.GetRequiredService<IKafkaConsumer>();
            var producer = Services.GetRequiredService<IKafkaProducer>();
            
            var messages = Enumerable.Range(0, 103)
                .Select(x => new StubMessage {Index = x, Id = Guid.NewGuid()})
                .ToHashSet();

            var partitions = new ConcurrentDictionary<string, ConcurrentQueue<int>>();
            
            consumer.Pipeline<StubMessage>(topic)
                .Buffer(10)
                .GroupBy(by => by
                    .ByPartition().And
                    .ByKey(), degreeOfParallelism)
                .Action(async message =>
                {
                    await Task.Delay(delayMs);
                    partitions.GetOrAdd(message.Key, new ConcurrentQueue<int>()).Enqueue(message.Value.Index);
                    _log.WriteLine($"Received Message({message.Offset}) = {message.Key} " +
                                   $"- Thread = {Thread.CurrentThread.ManagedThreadId} " +
                                   $"- Time = {DateTimeOffset.Now.ToUnixTimeMilliseconds()}");
                })
                .Subscribe();
            
            await Task.WhenAll(messages.Select((x, i) => producer
                .ProduceAsync(topic, x, ((i + 1) % degreeOfParallelism).ToString())));
            
            await producer.ProduceAsync(topic, Sink<StubMessage>.NewMessage);

            await Task.Delay(2000);

            Assert.All(partitions, partition =>
            {
                Assert.Equal(partition.Value, partition.Value.OrderBy(_ => _));
            });
        }
    }
}