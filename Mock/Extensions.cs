using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Options;
using Microsoft.Extensions.DependencyInjection;

namespace AspNetCore.Kafka.Mock
{
    public static class Extensions
    {
        public static KafkaServiceConfiguration UseInMemoryBroker(this KafkaServiceConfiguration kafkaConfig)
        {
            kafkaConfig.Services
                .AddSingleton<IKafkaClientFactory, KafkaClientInMemoryFactory>()
                .AddOptions<KafkaOptions>().Configure(x =>
                {
                    x.Server = "memory";
                });

            return kafkaConfig;
        }
    }
}