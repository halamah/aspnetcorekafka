using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using AspNetCore.Kafka.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AspNetCore.Kafka.Automation
{
    public static class SubscriptionBuilderExtensions
    {
        public static ConfigurationBuilder Subscribe(this ConfigurationBuilder builder, Action<SubscriptionBuilder> subscriber)
        {
            var sink = new SubscriptionBuilder(builder.Services);

            subscriber(sink);

            if (sink.Handlers.Any())
            {
                builder.Services
                    .AddHostedService(x =>
                        ActivatorUtilities.CreateInstance<ConsumerHostedService>(x, sink.Handlers));
            }

            return builder;
        }
    }

    public class SubscriptionBuilder
    {
        private readonly IServiceCollection _services;
        
        internal HashSet<Type> Handlers { get; } = new();

        public SubscriptionBuilder(IServiceCollection services) => _services = services;

        public SubscriptionBuilder AddTransient<THandler>() where THandler : IMessageHandler
            => FromType(typeof(THandler), ServiceLifetime.Transient);
        
        public SubscriptionBuilder AddScoped<THandler>() where THandler : IMessageHandler
            => FromType(typeof(THandler), ServiceLifetime.Scoped);
        
        public SubscriptionBuilder AddSingleton<THandler>() where THandler : IMessageHandler
            => FromType(typeof(THandler), ServiceLifetime.Singleton);

        public SubscriptionBuilder AddAssembly(
            ServiceLifetime lifetime = ServiceLifetime.Transient,
            params Assembly[] assemblies)
        {
            var types = assemblies
                .Concat(new[] { Assembly.GetEntryAssembly() })
                .Distinct()
                .GetMessageHandlerTypes();

            foreach (var type in types)
                FromType(type, lifetime);

            return this;
        }

        private SubscriptionBuilder FromType(Type type, ServiceLifetime lifetime)
        {
            if (!Handlers.Contains(type))
            {
                Handlers.Add(type);
                _services.TryAdd(new ServiceDescriptor(type, type, lifetime));
            }

            return this;
        }
    }
}