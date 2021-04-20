using System;
using AspNetCore.Kafka.Data;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;

namespace AspNetCore.Kafka.Client.Consumer
{
    public class SubscriptionConfiguration
    {
        public string Topic { get; init; }

        public string Group { get; init; }
        
        public SourceOptions Options { get; init; }
        
        public IServiceScope Scope { get; init; }

        public Action<IClient, LogMessage> LogHandler { get; init; }
    }
}