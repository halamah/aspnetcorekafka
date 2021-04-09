using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Tests.Data;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class ProduceConsumeTests : TestServerFixture
    {
        public ProduceConsumeTests(ITestOutputHelper log) : base(log)
        {
        }
        
        [Fact]
        public async Task ProduceConsumer()
        {
            var messages = Enumerable.Range(0, 103).Select(x => new SampleMessage {Index = x, Id = Guid.NewGuid()}).ToHashSet();
            var consumed = new HashSet<SampleMessage>();
            
            var consumer = Services.GetRequiredService<IKafkaConsumer>();
            var producer = Services.GetRequiredService<IKafkaProducer>();

            await Task.WhenAll(messages.Select(x => producer.ProduceAsync("test", null, x)));
            
            consumer.Subscribe<SampleMessage>("test", async x =>
                {
                    Log($"Received Index = {x.Value.Index} Id = {x.Value.Id} Offset = {x.Offset}");
                    consumed.Add(x.Value);
                },
                new SubscriptionOptions
                {

                });

            await Task.Delay(5000);
            
            consumed.Count.Should().Be(messages.Count);
            consumed.Should().BeEquivalentTo(messages);
        }
    }
}