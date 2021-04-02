using System.Collections.Generic;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation;
using AspNetCore.Kafka.Avro;
using AspNetCore.Kafka.Core;
using AspNetCore.Kafka.Options;
using Confluent.SchemaRegistry;
using Mapster;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace AspNetCore.Kafka
{
    public static class CoreExtensions
    {
        private const string ConnectionName = "Kafka";
        private const string SchemaRegistryConnection = "SchemaRegistry";

        public static KafkaServiceConfiguration AddKafka(this IServiceCollection services, IConfiguration config)
        {
            var options = config.GetKafkaOptions();
                            
            if (config.GetConnectionString(SchemaRegistryConnection) is var schemaRegistryUrl and not null)
            {
                AvroLogicalTypes.Register();
                
                services.AddSingleton<ISchemaRegistryClient>(new CachedSchemaRegistryClient(new SchemaRegistryConfig
                {
                    Url = schemaRegistryUrl
                }));
            }
            
            var builder = new KafkaServiceConfiguration(services);
            
            services
                .AddSingleton<IKafkaProducer, KafkaProducer>()
                .AddSingleton<IKafkaConsumer, KafkaConsumer>()
                .AddSingleton(builder)
                .AddHostedService<ConsumerHostedService>()
                .AddOptions<KafkaOptions>().Configure(x => options.Adapt(x));

            return builder;
        }

        public static KafkaOptions GetKafkaOptions(this IConfiguration config) => new()
        {
            Server = config.GetConnectionString(ConnectionName),
            Configuration = config.GetSection(ConnectionName).Get<KafkaConfiguration>()
        };

        public static JsonSerializerSettings JsonSerializerSettings => new()
        {
            DateTimeZoneHandling = DateTimeZoneHandling.Utc,
            ContractResolver = new CamelCasePropertyNamesContractResolver()
        };
        
        public static bool IsManualCommit(this KafkaOptions options) =>
            bool.TryParse(options?.Configuration?.Consumer?.GetValueOrDefault("enable.auto.commit"), out var x) && !x;
    }
}