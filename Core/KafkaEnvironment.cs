using System;
using AspNetCore.Kafka.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace AspNetCore.Kafka
{
    public class ConfiguredKafkaEnvironment : IKafkaEnvironment
    {
        public string EnvironmentName { get; }
        
        public ConfiguredKafkaEnvironment(string environmentName)
        {
            EnvironmentName = environmentName;
        }
    }
    
    public class DefaultKafkaEnvironment : IKafkaEnvironment
    {
        public string EnvironmentName { get; }
        
        public DefaultKafkaEnvironment(IServiceProvider provider)
        {
            var environment = provider.GetService<IHostEnvironment>();

            EnvironmentName = environment?.EnvironmentName ??
                              provider.GetRequiredService<IConfiguration>()["ASPNETCORE_ENVIRONMENT"];
        }
    }
}