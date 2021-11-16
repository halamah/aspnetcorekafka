using System.Threading.Tasks;
using AspNetCore.Kafka;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Mock.Abstractions;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class MemoryBrokerTests : IClassFixture<TestServer>
    {
        private readonly TestServer _server;

        [Message(Topic = "notification-{env}")]
        record Notification
        {
            public string Type { get; set; }
            public string Value { get; set; }
        }
        
        public MemoryBrokerTests(TestServer server, ITestOutputHelper output)
        {
            _server = server.SetOutput(output);
        }

        [Fact]
        public async Task GetTopic()
        {
            var broker = _server.Services.GetRequiredService<IKafkaMemoryBroker>().Bounce();
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            var environment = _server.Services.GetRequiredService<IHostEnvironment>();

            await producer.ProduceAsync(new Notification { Value = "777" });
            await producer.ProduceAsync(new Notification { Value = "666" });

            broker.GetTopic<Notification>().Produced.Should().HaveCount(2);
            broker.GetTopic("notification-{env}").Produced.Should().HaveCount(2);
            broker.GetTopic($"notification-{environment.EnvironmentName.ToUpper()}").Produced.Should().HaveCount(2);
            
            broker.GetTopic<Notification>(x => x.Value == "777").Produced.Should().HaveCount(1);
            broker.GetTopic<Notification>(x => x.Value == "666").Produced.Should().HaveCount(1);
            broker.GetTopic<Notification>(x => x.Value == "000").Produced.Should().BeEmpty();
        }

        [Fact]
        public async Task ReuseTopic()
        {
            var broker = _server.Services.GetRequiredService<IKafkaMemoryBroker>().Bounce();
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();

            var topic1 = broker.GetTopic<Notification>(x => x.Type == "1");
            var topic2 = broker.GetTopic<Notification>(x => x.Type == "2");
            
            await producer.ProduceAsync(new Notification { Type = "1", Value = "111" });
            await producer.ProduceAsync(new Notification { Type = "2", Value = "222" });

            topic1.Produced.Should().ContainSingle(x => x.Value.Value == "111");
            topic2.Produced.Should().ContainSingle(x => x.Value.Value == "222");

            broker.Bounce();
            
            topic1.Produced.Should().BeEmpty();
            topic2.Produced.Should().BeEmpty();
            
            await producer.ProduceAsync(new Notification { Type = "1", Value = "000" });
            
            topic1.Produced.Should().ContainSingle(x => x.Value.Value == "000");
            topic2.Produced.Should().BeEmpty();
        }
    }
}