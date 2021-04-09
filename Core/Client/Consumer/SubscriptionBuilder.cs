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

namespace AspNetCore.Kafka.Client.Consumer
{
    internal class SubscriptionConfiguration
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
    
    internal class SubscriptionBuilder<TKey, TValue, TContract> where TContract : class
    {
        private readonly KafkaOptions _options;
        private readonly IKafkaClientFactory _clientFactory;

        public SubscriptionBuilder(KafkaOptions options, IKafkaClientFactory clientFactory)
        {
            _options = options;
            _clientFactory = clientFactory;
        }

        public MessageReaderTask<TKey, TValue, TContract> Build(SubscriptionConfiguration subscription)
        {
            var group = subscription.Group ?? _options.Configuration?.Group;
            
            if(string.IsNullOrEmpty(_options.Server))
                throw new ArgumentException("Kafka connection string is not defined");
            
            var config = new ConsumerConfig(_options.Configuration?.Consumer ?? new())
            {
                BootstrapServers = _options.Server,
                GroupId = group,
            };
            
            IConsumer<TKey, TValue> DefaultConsumer() {
                var builder = new ConsumerBuilder<TKey, TValue>(config).SetLogHandler(subscription.LogHandler);

                if (subscription.TopicFormat == TopicFormat.Avro)
                {
                    var schema = subscription.Scope.ServiceProvider.GetRequiredService<ISchemaRegistryClient>();
                    var avroDeserializer = new AvroDeserializer<TValue>(schema, new SchemaRegistryConfig());
                    builder = builder.SetValueDeserializer(avroDeserializer.AsSyncOverAsync());
                }

                builder.SetPartitionsAssignedHandler((c, p) =>
                    PartitionsAssigner.Handler(subscription.Logger, subscription.Offset, subscription.Bias, c, p));

                return builder.Build();
            }

            var consumer = _clientFactory?.CreateConsumer<TKey, TValue>(subscription.Topic) ?? DefaultConsumer();

            consumer.Subscribe(subscription.Topic);

            return new MessageReaderTask<TKey, TValue, TContract>(
                subscription.Scope.ServiceProvider.GetServices<IMessageInterceptor>(),
                subscription.Serializer,
                subscription.Logger,
                consumer,
                subscription.Topic,
                subscription.Buffer);
        }
    }
}