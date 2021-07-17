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
        }
        
        public object Key { get; set; }
        
        public object Value { get; set; }
        
        public string Topic { get; set; }
    }

    public class KafkaInterception
    {
        public Exception Exception { get; set; }
        
        public IEnumerable<InterceptedMessage> Messages { get; set; }
    }
}