using System.Threading.Tasks;
using AspNetCore.Kafka;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Mock.Abstractions;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
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
            public string Value { get; set; }
        }
        
        public MemoryBrokerTests(TestServer server, ITestOutputHelper output)
        {
            _server = server.SetOutput(output);
        }

        [Fact]
        public async Task GetTopic()
        {
            var broker = _server.Services.GetRequiredService<IKafkaMemoryBroker>();
            var producer = _server.Services.GetRequiredService<IKafkaProducer>();
            var consumer = _server.Services.GetRequiredService<IKafkaConsumer>();
            var environmentName = _server.Services.GetRequiredService<IKafkaEnvironment>().EnvironmentName;
            
            await producer.ProduceAsync(new Notification { Value = "777" });
            await producer.ProduceAsync(new Notification { Value = "666" });

            broker.GetTopic<Notification>().Produced.Should().HaveCount(2);
            broker.GetTopic("notification-{env}").Produced.Should().HaveCount(2);
            broker.GetTopic($"notification-{environmentName.ToUpper()}").Produced.Should().HaveCount(2);
            
            broker.GetTopic<Notification>(x => x.Value == "777").Produced.Should().HaveCount(1);
            broker.GetTopic<Notification>(x => x.Value == "666").Produced.Should().HaveCount(1);
            broker.GetTopic<Notification>(x => x.Value == "000").Produced.Should().BeEmpty();
        }
    }
}