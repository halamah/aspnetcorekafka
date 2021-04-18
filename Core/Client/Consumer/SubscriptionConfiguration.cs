using System;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
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
        
        public SourceOptions Options { get; set; }
        
        public IServiceScope Scope { get; set; }
        
        public IMessageSerializer Serializer { get; set; }
        
        public Action<IClient, LogMessage> LogHandler { get; set; }
    }
}