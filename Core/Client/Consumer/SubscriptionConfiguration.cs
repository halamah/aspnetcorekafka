using System;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Options;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Client.Consumer
{
    public class SubscriptionConfiguration
    {
        public ILogger Logger { get; init; }
        
        public string Topic { get; set; }

        public string Group { get; set; }
        
        public IServiceScope Scope { get; set; }
        
        public IMessageSerializer Serializer { get; set; }
        
        public TopicFormat TopicFormat { get; set; }
        
        public Action<IClient, LogMessage> LogHandler { get; set; }
        
        public TopicOffset Offset { get; set; }
        
        public long Bias { get; set; }
        
        public int Buffer { get; set; }
    }
}