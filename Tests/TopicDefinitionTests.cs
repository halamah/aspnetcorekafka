using System;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Data;
using FluentAssertions;
using Xunit;

namespace Tests
{
    public class TopicDefinitionTests
    {
        [Fact]
        public void MessageKeyDefinition()
        {
            const string strKey = "777";
            const int ordKey = 777;

            new StubMessage1 {Id = strKey}.GetMessageKey().Should().Be(strKey);
            new StubMessage2 {Id = ordKey}.GetMessageKey().Should().Be(strKey);
            new StubMessage5().GetMessageKey().Should().Be(null);
            new StubMessage6().GetMessageKey().Should().Be(null);

            new StubMessage3 {Id = strKey}.Invoking(x => x.GetMessageKey()).Should().Throw<MissingMemberException>();
            new StubMessage4 {Id = strKey}.Invoking(x => x.GetMessageKey()).Should().Throw<ArgumentException>();
        }

        class StubMessage1
        {
            [MessageKey]
            public string Id { get; set; }
        }
        
        [Message(Key = "Id")]
        class StubMessage2
        {
            public int Id { get; set; }
        }
        
        [Message(Key = "Nonexistent")]
        class StubMessage3
        {
            public string Id { get; set; }
        }
        
        [Message(Key = "Id")]
        class StubMessage4
        {
            public string Id { get; set; }
            
            [MessageKey]
            public string Key { get; set; }
        }
        
        [Message(Key = "Id")]
        class StubMessage5
        {
            public string Id { get; set; }
        }
        
        [Message(Key = "Id")]
        class StubMessage6
        {
            public int? Id { get; set; }
        }
    }
}