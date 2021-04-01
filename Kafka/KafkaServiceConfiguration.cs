using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Core;
using Microsoft.Extensions.DependencyInjection;

namespace AspNetCore.Kafka
{
    public class KafkaServiceConfiguration
    {
        private readonly IServiceCollection _services;

        public KafkaServiceConfiguration(IServiceCollection services)
        {
            _services = services;
        }

        public HashSet<Assembly> Assemblies { get; } = new();
        
        public KafkaServiceConfiguration AddAssembly(Assembly assembly)
        {
            Assemblies.Add(assembly);
            return this;
        }
        
        public KafkaServiceConfiguration AddConsumerInterceptor<T>() where T : class, IMessageInterceptor
        {
            _services.AddSingleton<T>();
            return this;
        }
        
        public KafkaServiceConfiguration AddConsumerInterceptor(Type interceptorType)
        {
            if (!interceptorType.IsAssignableTo(typeof(IMessageInterceptor)))
                throw new ArgumentException($"Invalid interceptor type {interceptorType}");
            
            _services.AddSingleton(interceptorType);
            return this;
        }
        
        public KafkaServiceConfiguration AddConsumerInterceptor(Func<IServiceProvider, IMessageInterceptor> func)
        {
            _services.AddSingleton(func);
            return this;
        }
        
        public KafkaServiceConfiguration AddConsumerInterceptor(IMessageInterceptor interceptor)
        {
            _services.AddSingleton(interceptor);
            return this;
        }
    }
}