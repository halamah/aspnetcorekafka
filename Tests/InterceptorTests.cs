using System.Threading.Tasks;
using AspNetCore.Kafka.Automation.Pipeline;
using FluentAssertions;
using Tests.Data;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class InterceptorTests : TestServerFixture
    {
        public InterceptorTests(ITestOutputHelper log) : base(log)
        { }

        [Fact]
        public async Task Intercept()
        {
            var topic = Broker.GetTopic(nameof(Intercept));
            var stub = new Stub();
            const int count = 23;

            Interceptor.Bounce();

            await stub.Produce(Producer, count, topic.Name);

            Consumer.Message<StubMessage>().Action(_ => Task.CompletedTask).Subscribe(topic.Name);

            await topic.WhenConsumedAll();
            await Task.Delay(100);
            await Consumer.Complete();

            Interceptor.ConsumeInterceptions.Count.Should().Be(count);
            Interceptor.ProduceInterceptions.Count.Should().Be(count);
        }
    }
}