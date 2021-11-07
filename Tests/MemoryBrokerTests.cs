using System;
using System.Linq;
using System.Threading.Tasks;
using AspNetCore.Kafka;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Attributes;
using FluentAssertions;
using Tests.Data;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class MemoryBrokerTests : TestServerFixture
    {
        [Message(Topic = "notification-{env}")]
        record Notification
        {
            public string Value { get; set; }
        }
        
        public MemoryBrokerTests(ITestOutputHelper log) : base(log)
        { }

        [Fact]
        public async Task GetTopic()
        {
            await Producer.ProduceAsync(new Notification { Value = "777" });
            await Producer.ProduceAsync(new Notification { Value = "666" });

            var environmentName = GetRequiredService<IKafkaEnvironment>().EnvironmentName;

            Broker.GetTopic<Notification>().Produced.Should().HaveCount(2);
            Broker.GetTopic("notification-{env}").Produced.Should().HaveCount(2);
            Broker.GetTopic($"notification-{environmentName.ToUpper()}").Produced.Should().HaveCount(2);
            
            Broker.GetTopic<Notification>(x => x.Value == "777").Produced.Should().HaveCount(1);
            Broker.GetTopic<Notification>(x => x.Value == "666").Produced.Should().HaveCount(1);
            Broker.GetTopic<Notification>(x => x.Value == "000").Produced.Should().BeEmpty();
        }
    }
}