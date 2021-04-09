using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Mock.Abstractions;
using AspNetCore.Kafka.Mock.InMemory;
using AspNetCore.Kafka.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AspNetCore.Kafka.Mock
{
    public static class MockExtensions
    {
        public static KafkaServiceConfiguration UseInMemoryBroker(this KafkaServiceConfiguration kafkaConfig)
        {
            var descriptor =
                new ServiceDescriptor(
                    typeof(IKafkaClientFactory),
                    typeof(KafkaClientInMemoryFactory),
                    ServiceLifetime.Singleton);
            
            kafkaConfig.Services
                .Replace(descriptor)
                .AddSingleton<IKafkaMemoryBroker, KafkaMemoryBroker>()
                .AddOptions<KafkaOptions>().Configure(x =>
                {
                    x.Server = "memory";
                });

            return kafkaConfig;
        }
    }
}