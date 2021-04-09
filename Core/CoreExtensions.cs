using System;
using System.Collections.Generic;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation;
using AspNetCore.Kafka.Avro;
using AspNetCore.Kafka.Client;
using AspNetCore.Kafka.Client.Consumer;
using AspNetCore.Kafka.Options;
using Confluent.SchemaRegistry;
using Mapster;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace AspNetCore.Kafka
{
    public static class CoreExtensions
    {
        private const string ConnectionName = "Kafka";
        private const string SchemaRegistryConnection = "SchemaRegistry";

        public static KafkaServiceConfiguration AddKafka(this IServiceCollection services, IConfiguration config)
        {
            var options = config.GetKafkaOptions();

            var builder = new KafkaServiceConfiguration(services);
            
            services
                .AddSingleton(x => CreateSchemaRegistry(x.GetRequiredService<IOptions<KafkaOptions>>()))
                .AddSingleton<IKafkaProducer, KafkaProducer>()
                .AddSingleton<IKafkaConsumer, KafkaConsumer>()
                .AddSingleton<IKafkaClientFactory, DefaultKafkaClientFactory>()
                .AddTransient<IMessageSerializer, DefaultJsonSerializer>()
                .AddSingleton(builder)
                .AddHostedService<ConsumerHostedService>()
                .AddOptions<KafkaOptions>().Configure(x => options.Adapt(x));

            return builder;
        }

        private static ISchemaRegistryClient CreateSchemaRegistry(IOptions<KafkaOptions> options)
        {
            if (string.IsNullOrEmpty(options.Value?.SchemaRegistry))
                throw new ArgumentException("Missing SchemaRegistry connection string");

            AvroLogicalTypes.Register();

            return new CachedSchemaRegistryClient(new SchemaRegistryConfig {Url = options.Value.SchemaRegistry});
        }

        public static KafkaOptions GetKafkaOptions(this IConfiguration config) => new()
        {
            SchemaRegistry = config.GetConnectionString(SchemaRegistryConnection),
            Server = config.GetConnectionString(ConnectionName),
            Configuration = config.GetSection(ConnectionName).Get<KafkaConfiguration>() ?? new()
        };

        public static bool IsManualCommit(this KafkaOptions options) =>
            bool.TryParse(options?.Configuration?.Consumer?.GetValueOrDefault("enable.auto.commit"), out var x) && !x;
    }
}