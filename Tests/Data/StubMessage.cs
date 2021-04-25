using System;
using AspNetCore.Kafka.Automation.Attributes;

namespace Tests.Data
{
    [Message(Topic = "StubMessage")]
    public record StubMessage
    {
        [MessageKey]
        public Guid Id { get; set; } = Guid.NewGuid();
        
        public int Index { get; set; }
    }
}