using System;
using System.Collections.Generic;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Data
{
    public class InterceptedMessage
    {
        public InterceptedMessage()
        {
        }
        
        public InterceptedMessage(IMessage message)
        {
            Key = message.Key;
            Value = message.GetValue();
            Topic = message.Topic;
            Name = message.Name;
            Offset = message.Offset;
            Partition = message.Partition;
            Group = message.Group;
        }

        public string Name { get; set; }

        public object Key { get; set; }
        
        public object Value { get; set; }
        
        public string Topic { get; set; }
        
        public string Group { get; set; }
        
        public long Offset { get; set; }
        
        public int Partition { get; set; }
    }

    public class InterceptionMetrics
    {
        public TimeSpan ProcessingTime { get; set; }
    }

    public class KafkaInterception
    {
        public Exception Exception { get; set; }
        
        public bool IsBatched { get; set; }
        
        public IEnumerable<InterceptedMessage> Messages { get; set; }
        
        public InterceptionMetrics Metrics { get; set; }
    }
}