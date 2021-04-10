using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Extensions.Abstractions;
using AspNetCore.Kafka.Extensions.Blocks;
using FluentAssertions;
using NSubstitute;
using Tests.Data;
using Tests.Mock;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class BatchOptions : IMessageBatchOptions
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
            var sink = Sink<StubMessage>.Create(Logger, x => Log("Received"));
            var converter = BatchBlock(batchSize, 100);
            var handler = converter.Create<StubMessage>(sink.Batch);
            
            await Task.WhenAll(Enumerable.Range(0, batchCount * batchSize)
                .Select(_ => handler(Sink<StubMessage>.NewMessage)));
            
            await handler(Sink<StubMessage>.NewMessage);

            await sink.Received(batchCount)
                .Batch(Arg.Is<IEnumerable<IMessage<StubMessage>>>(x => x.Count() == batchSize));
            
            await sink.DidNotReceive()
                .Batch(Arg.Is<IEnumerable<IMessage<StubMessage>>>(x => x.Count() != batchSize));
            
            sink.ClearReceivedCalls();
            
            await Task.Delay(70);
            
            await sink.DidNotReceiveWithAnyArgs().Batch(null);
            
            await Task.Delay(70);
            
            await sink.Received(1).Batch(Arg.Is<IEnumerable<IMessage<StubMessage>>>(x => x.Count() == 1));
            
            await sink.DidNotReceive().Batch(Arg.Is<IEnumerable<IMessage<StubMessage>>>(x => x.Count() != 1));
        }

        [Fact]
        public async Task RandomBatches()
        {
            var sink = Sink<StubMessage>.Create(Logger);
            var converter = BatchBlock(10, 100);
            var handler = converter.Create<StubMessage>(sink.Batch);
            
            var count = await Generator.Run(Logger, () => handler(Sink<StubMessage>.NewMessage),
                TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(200));
            
            await Task.Delay(1000);
            
            Log($"Generated {count} calls");

            sink.TotalMessages().Should().Be(count);
        }
        
        private BatchMessageBlock BatchBlock(int size, int timout) => new(
            new TestLogger<BatchMessageBlock>(Logger),
            new BatchOptions(size, timout));
    }
}