using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Pipeline;
using AspNetCore.Kafka.Mock.Abstractions;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Tests.Data;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class PipelineTests : IClassFixture<TestServer>
    {
        private readonly TestServer _server;

        public PipelineTests(TestServer server, ITestOutputHelper output)
        {
            _server = server.SetOutput(output);
        }

        [Fact]
        public async Task Unsubscribe()
        {
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            var consumer = _server.Services.GetRequiredService<IKafkaConsumer>();
            
            var signal = new ManualResetEvent(false);
            const int messageDelay = 5000;
            
            await producer.ProduceAsync(nameof(Unsubscribe), new StubMessage());
            
            var sw = Stopwatch.StartNew();
            
            var subscription = consumer
                .Message<StubMessage>()
                .Buffer(100)
                .Action(async x =>
                {
                    signal.Set();
                    await Task.Delay(messageDelay);
                })
                .Subscribe(nameof(Unsubscribe));

            signal.WaitOne(1000).Should().Be(true);

            await subscription.UnsubscribeAsync();
            
            sw.ElapsedMilliseconds.Should().BeGreaterOrEqualTo(messageDelay - 100);
        }
        
        [Fact]
        public async Task SubscribeSimplePipeline()
        {
            var broker = _server.Services.GetRequiredService<IKafkaMemoryBroker>();
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            var consumer = _server.Services.GetRequiredService<IKafkaConsumer>();
            
            const string topic = nameof(SubscribeSimplePipeline);
            var stub = new Stub();
            
            var produced = await stub.Produce(producer, 333, topic);
            
            var subscription = consumer.Message<StubMessage>().Action(stub.ConsumeMessage).Subscribe(topic);

            await broker.GetTopic(topic).WhenConsumedAll();
            await Task.Delay(200);
            await subscription.UnsubscribeAsync();

            broker.GetTopic(topic).Consumed.Count().Should().Be(produced.Count);
            broker.GetTopic(topic).Produced.Count().Should().Be(produced.Count);
            
            stub.Consumed.Should().BeEquivalentTo(produced); 
        }
    }
}