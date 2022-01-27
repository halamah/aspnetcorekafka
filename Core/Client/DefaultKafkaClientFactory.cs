using System;
using System.Collections.Generic;
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

        private static readonly Dictionary<SyslogLevel, LogLevel> LogMap = new()
        {
            {SyslogLevel.Emergency, LogLevel.Critical},
            {SyslogLevel.Alert, LogLevel.Critical},
            {SyslogLevel.Critical, LogLevel.Critical},
            {SyslogLevel.Error, LogLevel.Error},
            {SyslogLevel.Warning, LogLevel.Warning},
            {SyslogLevel.Notice, LogLevel.Information},
            {SyslogLevel.Info, LogLevel.Information},
            {SyslogLevel.Debug, LogLevel.Debug},
        };

        public DefaultKafkaClientFactory(ILogger<DefaultKafkaClientFactory> log)
        {
            _log = log;
        }

        public IProducer<TKey, TValue> CreateProducer<TKey, TValue>(KafkaOptions options)
        {
            var config = options.Configuration.ClientCommon.Merge(options.Configuration.Producer);
            
            return new ProducerBuilder<TKey, TValue>(new ProducerConfig(config)
                {
                    BootstrapServers = options.Server ?? config.GetValueOrDefault(KafkaConfiguration.BootstrapServers)
                })
                .SetLogHandler(LogHandler)
                .Build();
        }

        public IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(
            KafkaOptions options,
            Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>> revokeHandler,
            SubscriptionConfiguration subscription)
        {
            var config = options.Configuration.ClientCommon.Merge(options.Configuration.Consumer);

            var builder = new ConsumerBuilder<TKey, TValue>(new ConsumerConfig(config)
                {
                    BootstrapServers = options.Server ?? config.GetValueOrDefault(KafkaConfiguration.BootstrapServers)
                })
                .SetLogHandler(LogHandler);

            if (subscription.Options.Format == TopicFormat.Avro)
            {
                var schema = subscription.Scope.ServiceProvider.GetRequiredService<ISchemaRegistryClient>();
                var avroDeserializer = new AvroDeserializer<TValue>(schema, new SchemaRegistryConfig());
                builder = builder.SetValueDeserializer(avroDeserializer.AsSyncOverAsync());
            }

            var offset = subscription.Options.Offset;

            if (offset.Bias is not null || offset.Offset is not null || offset.DateOffset is not null)
            {
                builder.SetPartitionsRevokedHandler(revokeHandler);
                builder.SetPartitionsAssignedHandler((c, p) => PartitionsAssigner.Handler(_log, subscription, c, p));
            }

            return builder.Build();
        }
        
        private void LogHandler(IClient client, LogMessage message)
        {
            using var _ = _log.BeginScope(new {ClientName = client.Name});
            _log.Log(LogMap[message.Level], $"{message.Facility}: {message.Message}");
        }
    }
}