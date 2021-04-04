using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation;
using AspNetCore.Kafka.Extensions.Converters;
using AspNetCore.Kafka.Options;
using FluentAssertions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Tests.Data;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Converters
{
    public class BatchConverterTests
    {
        private ITestOutputHelper _logger;

        public BatchConverterTests(ITestOutputHelper logger)
        {
            _logger = logger;
        }

        [Fact]
        public async Task BatchSeries()
        {
            const int batchSize = 5;
            const int batchCount = 30;
            var sink = Sink.Create<SampleMessage>(_logger);
            var converter = Converter(sink, batchSize, 100);

            await Task.WhenAll(Enumerable.Range(0, batchCount * batchSize)
                .Select(_ => converter.HandleAsync(Sink.NewMessage)));
            
            await converter.HandleAsync(Sink.NewMessage);

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
            var sink = Sink.Create<SampleMessage>(_logger);
            var converter = Converter(sink, 10, 100);

            var count = await Generator.Run(_logger, () => converter.HandleAsync(Sink.NewMessage),
                TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(200));
            
            await Task.Delay(1500);
            
            _logger.WriteLine($"Generated {count} calls");

            sink.TotalMessages().Should().Be(count);
        }
        
        private BatchMessageConverter Converter<T>(ISink<T> sink, int size, int timout) => new(
            sink,
            sink.BatchMethodInfo,
            new OptionsWrapper<KafkaOptions>(null), 
            new MessageConverterArgument<object>(new
            {
                Size = size,
                Timeout = timout
            }));
    }
}