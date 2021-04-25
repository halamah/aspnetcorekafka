using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka;
using AspNetCore.Kafka.Automation.Pipeline;
using FluentAssertions;
using Tests.Data;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class PipelineTests : TestServerFixture
    {
        public PipelineTests(ITestOutputHelper log) : base(log)
        { }

        [Fact]
        public async Task Unsubscribe()
        {
            var signal = new ManualResetEvent(false);
            const int messageDelay = 5000;
            
            await Producer.ProduceAsync(nameof(Unsubscribe), new StubMessage());
            
            var sw = Stopwatch.StartNew();
            
            Consumer
                .Message<StubMessage>()
                .Buffer(100)
                .Action(async x =>
                {
                    signal.Set();
                    await Task.Delay(messageDelay);
                })
                .Subscribe(nameof(Unsubscribe));

            signal.WaitOne(1000).Should().Be(true);

            await Consumer.Complete(10000);
            sw.ElapsedMilliseconds.Should().BeGreaterOrEqualTo(messageDelay);
        }
        
        [Fact]
        public async Task SubscribeSimplePipeline()
        {
            const string topic = nameof(SubscribeSimplePipeline);
            var stub = new Stub();
            
            var produced = await stub.Produce(Producer, 333, topic);
            
            Consumer.Message<StubMessage>().Action(stub.ConsumeMessage).Subscribe(topic);

            await Broker.GetTopic(topic).WhenConsumedAll();

            Broker.GetTopic(topic).ConsumedCount.Should().Be(produced.Count);
            Broker.GetTopic(topic).ProducedCount.Should().Be(produced.Count);
            
            stub.Consumed.Should().BeEquivalentTo(produced); 
        }
    }
}