using System;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Options;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Core.Consumer
{
    internal class SubscriptionConfiguration
    {
        public ILogger Logger { get; init; }
        
        public string Topic { get; set; }
        
        public string Group { get; set; }
        
        public IServiceScope Scope { get; set; }
        
        public TopicFormat TopicFormat { get; set; }
        
        public Action<IClient, LogMessage> LogHandler { get; set; }
        
        public TopicOffset Offset { get; set; }
        
        public long Bias { get; set; }
    }
    
    internal class SubscriptionBuilder<TKey, TValue, TContract> where TContract : class
    {
        private readonly KafkaOptions _options;

        public SubscriptionBuilder(KafkaOptions options)
        {
            _options = options;
        }

        public SubscriptionWorker<TKey, TValue, TContract> Build(SubscriptionConfiguration subscription)
        {
            var group = subscription.Group ?? _options.Configuration.Group;
            
            var config = new ConsumerConfig(_options.Configuration.Consumer)
            {
                BootstrapServers = _options.Server,
                GroupId = group,
            };
            
            if (subscription.TopicFormat == TopicFormat.Avro)
            {
                var schema = subscription.Scope.ServiceProvider.GetRequiredService<ISchemaRegistryClient>();
                var avroDeserializer = new AvroDeserializer<TValue>(schema, new SchemaRegistryConfig());
                var builder = new ConsumerBuilder<TKey, TValue>(config)
                    .SetValueDeserializer(avroDeserializer.AsSyncOverAsync())
                    .SetLogHandler(subscription.LogHandler);

                builder.SetPartitionsAssignedHandler((c, p) =>
                    PartitionsAssigner.Handler(subscription.Logger, subscription.Offset, subscription.Bias, c, p));
                        
                var consumer = builder.Build();
                consumer.Subscribe(subscription.Topic);

                return new SubscriptionWorker<TKey, TValue, TContract>(
                    subscription.Scope,
                    subscription.Logger,
                    consumer,
                    _options.IsManualCommit(),
                    subscription.Topic);
            }
            else
            {
                var builder = new ConsumerBuilder<TKey, TValue>(config).SetLogHandler(subscription.LogHandler);

                builder.SetPartitionsAssignedHandler((c, p) =>
                    PartitionsAssigner.Handler(subscription.Logger, subscription.Offset, subscription.Bias, c, p));

                var consumer = builder.Build();
                consumer.Subscribe(subscription.Topic);

                return new SubscriptionWorker<TKey, TValue, TContract>(
                    subscription.Scope,
                    subscription.Logger,
                    consumer,
                    _options.IsManualCommit(),
                    subscription.Topic);
            }
        }
    }
}