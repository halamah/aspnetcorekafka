using System.Collections.Generic;
using System.Linq;
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
                    x => x.GetRequiredService<IKafkaMemoryBroker>(),
                    ServiceLifetime.Singleton);

            kafkaConfig.Services
                .AddSingleton<IKafkaMemoryBroker, KafkaMemoryBroker>()
                .Replace(descriptor)
                .AddOptions<KafkaOptions>().Configure(x => x.Server = "memory");

            return kafkaConfig;
        }
    }
}