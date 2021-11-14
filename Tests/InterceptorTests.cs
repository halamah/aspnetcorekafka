using System.Linq;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Pipeline;
using AspNetCore.Kafka.Mock.Abstractions;
using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Tests.Data;
using Tests.Mock;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class InterceptorTests : IClassFixture<TestServer>
    {
        private readonly TestServer _server;

        public InterceptorTests(TestServer server, ITestOutputHelper output)
        {
            _server = server.SetOutput(output);
        }

        [Fact]
        public async Task Intercept()
        {
            var broker = _server.Services.GetRequiredService<IKafkaMemoryBroker>();
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            var consumer = _server.Services.GetRequiredService<IKafkaConsumer>();
            var interceptor = _server.Services.GetServices<IMessageInterceptor>().Cast<TestInterceptor>().First();
            
            var topic = broker.GetTopic(nameof(Intercept));
            
            var stub = new Stub();
            const int count = 23;

            _server.Interceptor.Bounce();

            await stub.Produce(producer, count, topic.Name);

            consumer.Message<StubMessage>().Action(_ => Task.CompletedTask).Subscribe(topic.Name);

            await topic.WhenConsumedAll();
            await Task.Delay(100);
            await consumer.Complete();

            interceptor.Consumed.Count.Should().Be(count);
            interceptor.Produced.Count.Should().Be(count);
        }
    }
}