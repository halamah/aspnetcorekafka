using System;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Client.Consumer;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Options;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.DependencyInjection;

namespace AspNetCore.Kafka.Client
{
    public class DefaultKafkaClientFactory : IKafkaClientFactory
    {
        public IProducer<TKey, TValue> CreateProducer<TKey, TValue>(
            KafkaOptions options, 
            Action<IClient, LogMessage> logHandler)
        {
            return new ProducerBuilder<TKey, TValue>(new ProducerConfig(options.Configuration?.Producer ?? new())
                {
                    BootstrapServers = options.Server,
                })
                .SetLogHandler(logHandler)
                .Build();
        }

        public IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(
            KafkaOptions options,
            SubscriptionConfiguration subscription)
        {
            var builder =
                new ConsumerBuilder<TKey, TValue>(options.Configuration.Consumer)
                    .SetLogHandler(subscription.LogHandler);

            if (subscription.TopicFormat == TopicFormat.Avro)
            {
                var schema = subscription.Scope.ServiceProvider.GetRequiredService<ISchemaRegistryClient>();
                var avroDeserializer = new AvroDeserializer<TValue>(schema, new SchemaRegistryConfig());
                builder = builder.SetValueDeserializer(avroDeserializer.AsSyncOverAsync());
            }

            builder.SetPartitionsAssignedHandler((c, p) =>
                PartitionsAssigner.Handler(subscription.Logger, subscription, c, p));

            return builder.Build();
        }
    }
}