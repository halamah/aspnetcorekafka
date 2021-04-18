using System;
using AspNetCore.Kafka.Options;

namespace AspNetCore.Kafka.Data
{
    public record MessageOffset
    {
        public MessageOffset()
        {
        }
        
        public MessageOffset(TopicOffset offset, long bias = 0)
        {
            Offset = offset;
            Bias = bias;
        }
        
        public MessageOffset(DateTimeOffset dateOffset)
        {
            DateOffset = dateOffset;
        }

        public TopicOffset? Offset { get; set; }
        
        public long? Bias { get; set; }
        
        public DateTimeOffset? DateOffset { get; set; }
    }
}