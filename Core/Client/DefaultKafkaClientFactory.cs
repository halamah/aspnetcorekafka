using System;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Options;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Client
{
    public class DefaultKafkaClientFactory : IKafkaClientFactory
    {
        private readonly ILogger _log;

        public DefaultKafkaClientFactory(ILogger<DefaultKafkaClientFactory> log)
        {
            _log = log;
        }

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
            var config = new ConsumerConfig(options.Configuration?.Consumer ?? new())
            {
                BootstrapServers = options.Server,
                GroupId = options.Configuration?.Group ?? Environment.MachineName,
            };
            
            var builder = new ConsumerBuilder<TKey, TValue>(config).SetLogHandler(subscription.LogHandler);

            if (subscription.Options.Format == TopicFormat.Avro)
            {
                var schema = subscription.Scope.ServiceProvider.GetRequiredService<ISchemaRegistryClient>();
                var avroDeserializer = new AvroDeserializer<TValue>(schema, new SchemaRegistryConfig());
                builder = builder.SetValueDeserializer(avroDeserializer.AsSyncOverAsync());
            }

            builder.SetPartitionsAssignedHandler((c, p) => PartitionsAssigner.Handler(_log, subscription, c, p));

            return builder.Build();
        }
    }
}