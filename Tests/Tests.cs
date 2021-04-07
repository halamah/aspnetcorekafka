using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Extensions.Blocks;
using AspNetCore.Kafka.Options;
using FluentAssertions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Tests.Data;
using Tests.Mock;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public record BatchOptions(int Size, int Time);
    
    public class Tests
    {
        private ITestOutputHelper _log;

        public Tests(ITestOutputHelper log)
        {
            _log = log;
        }

        [Fact]
        public async Task BatchSeries()
        {
            const int batchSize = 5;
            const int batchCount = 30;
            var sink = Sink<SampleMessage>.Create(_log, x => _log.WriteLine("Received"));
            var converter = Converter(sink, batchSize, 100);
            var handler = converter.Create<SampleMessage>(sink.Batch);
            
            await Task.WhenAll(Enumerable.Range(0, batchCount * batchSize)
                .Select(_ => handler(Sink<SampleMessage>.NewMessage)));
            
            await handler(Sink<SampleMessage>.NewMessage);

            await sink.Received(batchCount)
                .Batch(Arg.Is<IEnumerable<IMessage<SampleMessage>>>(x => x.Count() == batchSize));
            
            await sink.DidNotReceive()
                .Batch(Arg.Is<IEnumerable<IMessage<SampleMessage>>>(x => x.Count() != batchSize));
            
            sink.ClearReceivedCalls();
            
            await Task.Delay(70);
            
            await sink.DidNotReceiveWithAnyArgs().Batch(null);
            
            await Task.Delay(70);
            
            await sink.Received(1).Batch(Arg.Is<IEnumerable<IMessage<SampleMessage>>>(x => x.Count() == 1));
            
            await sink.DidNotReceive().Batch(Arg.Is<IEnumerable<IMessage<SampleMessage>>>(x => x.Count() != 1));
        }

        [Fact]
        public async Task BatchGenerator()
        {
            var sink = Sink<SampleMessage>.Create(_log);
            var converter = Converter(sink, 10, 100);
            var handler = converter.Create<SampleMessage>(sink.Batch);
            
            var count = await Generator.Run(_log, () => handler(Sink<SampleMessage>.NewMessage),
                TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(200));
            
            await Task.Delay(1000);
            
            _log.WriteLine($"Generated {count} calls");

            sink.TotalMessages().Should().Be(count);
        }
        
        private BatchMessageBlock Converter<T>(ISink<T> sink, int size, int timout) => new(
            new TestLogger<BatchMessageBlock>(_log),
            new OptionsWrapper<KafkaOptions>(null), 
            new BatchOptions(size, timout));
    }
}