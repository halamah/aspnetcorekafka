using System;
using System.Collections.Generic;
using System.Linq;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation;
using AspNetCore.Kafka.Avro;
using AspNetCore.Kafka.Client;
using AspNetCore.Kafka.Options;
using AspNetCore.Kafka.Serializers;
using Avro.Generic;
using Confluent.SchemaRegistry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace AspNetCore.Kafka
{
    public static class CoreExtensions
    {
        private const string ConnectionName = "Kafka";
        private const string SchemaRegistryConnection = "SchemaRegistry";
        private const string EnvironmentPlaceholder = "env";

        public static KafkaConfigurationBuilder AddKafka(this IServiceCollection services)
            => services.AddKafka(new ConfigurationBuilder().Build());
        
        public static KafkaConfigurationBuilder AddKafka(this IServiceCollection services, IConfiguration config)
        {
            var options = config.GetKafkaOptions();
            var builder = new KafkaConfigurationBuilder(services);

            if (!options.Configuration.Placeholders.ContainsKey(EnvironmentPlaceholder))
            {
                var provider = services.BuildServiceProvider();
                var environment = provider.GetService<IHostEnvironment>();
                options.Configuration.Placeholders.Add(EnvironmentPlaceholder, environment?.EnvironmentName);
            }

            services
                .AddSingleton(x => CreateSchemaRegistry(x.GetRequiredService<IOptions<KafkaOptions>>()))
                .AddSingleton<IKafkaProducer, KafkaProducer>()
                .AddSingleton<IKafkaConsumer, KafkaConsumer>()
                .AddSingleton<IKafkaClientFactory, DefaultKafkaClientFactory>()
                .AddSingleton<ISubscriptionManager, SubscriptionManager>()
                .AddSingleton(builder)
                .AddOptions<KafkaOptions>().Configure(x =>
                {
                    x.Configuration = options.Configuration;
                    x.Server = options.Server;
                    x.SchemaRegistry = options.SchemaRegistry;
                });

            services.TryAddTransient<IKafkaMessageSerializer<string>>(_ => new SystemTextSerializer());
            services.TryAddTransient<IKafkaMessageSerializer<GenericRecord>>(_ => new SimpleAvroSerializer());

            return builder;
        }

        private static ISchemaRegistryClient CreateSchemaRegistry(IOptions<KafkaOptions> options)
        {
            if (string.IsNullOrEmpty(options.Value?.SchemaRegistry))
                throw new ArgumentException("Missing SchemaRegistry connection string");

            AvroLogicalTypes.Register();

            return new CachedSchemaRegistryClient(new SchemaRegistryConfig {Url = options.Value.SchemaRegistry});
        }

        public static KafkaOptions GetKafkaOptions(this IConfiguration config)
        {
            var common = config.GetSection(ConnectionName)
                .GetChildren()
                .Where(x => !string.IsNullOrWhiteSpace(x.Value))
                .ToDictionary(x => x.Key.ToLower(), x => x.Value);

            var configuration = config.GetSection(ConnectionName).Get<KafkaConfiguration>() ?? new();
            
            configuration.ClientCommon = common;
            
            return new()
            {
                SchemaRegistry = config.GetConnectionString(SchemaRegistryConnection),
                Server = config.GetConnectionString(ConnectionName),
                Configuration = configuration
            };
        }

        public static bool IsManualCommit(this KafkaOptions options) =>
            bool.TryParse(options?.Configuration?.Consumer?.GetValueOrDefault("enable.auto.commit"), out var x) && !x;
    }
}