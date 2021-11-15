using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AspNetCore.Kafka.Automation
{
    public record KafkaHandlerTypes(IReadOnlyCollection<Type> Types);
        
    public static class ConfigurationBuilderExtensions
    {
        public static ConfigurationBuilder SubscribeFromAssembly(
            this ConfigurationBuilder builder, 
            ServiceLifetime lifetime, 
            params Assembly[] assemblies)
        {
            var types = assemblies
                .Concat(new[] { Assembly.GetEntryAssembly() })
                .Distinct()
                .GetMessageHandlerTypes();

            foreach (var type in types)
                builder.Services.TryAdd(new ServiceDescriptor(type, type, lifetime));    
                
            builder.Services
                .AddSingleton(new KafkaHandlerTypes(types))
                .AddHostedService<ConsumerHostedService>();

            return builder;
        }
    }
}