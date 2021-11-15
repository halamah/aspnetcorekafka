using System;
using System.Text.Json.Serialization;
using AspNetCore.Kafka.Automation.Attributes;

namespace Sample
{
    [Message(Topic = "test.topic-uat")]
    public record TestMessage
    {
        public int Index { get; set; }
        
        public Guid Id { get; set; }
        
        public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
        
        [JsonConverter(typeof(JsonStringEnumConverter))]
        public SampleType Type { get; set; }
    }
}