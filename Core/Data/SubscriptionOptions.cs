using System;
using System.Collections.Generic;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Options;

namespace AspNetCore.Kafka.Data
{
    public record SubscriptionOptions
    {
        public TopicFormat Format { get; set; }
        
        public DateTimeOffset? DateOffset { get; set; }
        
        public TimeSpan NegativeTimeOffset { get; set; } = TimeSpan.Zero;
        
        public TopicOffset Offset { get; set; }
        
        public long Bias { get; set; }
    }
}