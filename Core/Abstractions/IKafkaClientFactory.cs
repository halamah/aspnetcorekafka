using System;
using AspNetCore.Kafka.Client.Consumer;
using AspNetCore.Kafka.Options;
using Confluent.Kafka;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IKafkaClientFactory
    {
        IProducer<TKey, TValue> CreateProducer<TKey, TValue>(KafkaOptions options, Action<IClient, LogMessage> logHandler);
        
        IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(KafkaOptions options, SubscriptionConfiguration config);
    }
}