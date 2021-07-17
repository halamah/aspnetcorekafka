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
            return new ProducerBuilder<TKey, TValue>(new ProducerConfig(options.Configuration?.Producer ?? new())
                {
                    BootstrapServers = options.Server,
                })
                .SetLogHandler(LogHandler)
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
            
            var builder = new ConsumerBuilder<TKey, TValue>(config).SetLogHandler(LogHandler);

            if (subscription.Options.Format == TopicFormat.Avro)
            {
                var schema = subscription.Scope.ServiceProvider.GetRequiredService<ISchemaRegistryClient>();
                var avroDeserializer = new AvroDeserializer<TValue>(schema, new SchemaRegistryConfig());
                builder = builder.SetValueDeserializer(avroDeserializer.AsSyncOverAsync());
            }

            builder.SetPartitionsAssignedHandler((c, p) => PartitionsAssigner.Handler(_log, subscription, c, p));

            return builder.Build();
        }
        
        private void LogHandler(IClient client, LogMessage message)
        {
            using var _ = _log.BeginScope(new {ClientName = client.Name});
            _log.Log(LogMap[message.Level], $"{message.Facility}: {message.Message}");
        }
    }
}