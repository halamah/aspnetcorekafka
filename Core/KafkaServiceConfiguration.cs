using System;
using System.Collections.Generic;
using System.Reflection;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Options;
using Microsoft.Extensions.DependencyInjection;

namespace AspNetCore.Kafka
{
    public class KafkaServiceConfiguration
    {
        private readonly IServiceCollection _services;
        
        public HashSet<Assembly> Assemblies { get; } = new();
        
        public KafkaServiceConfiguration(IServiceCollection services)
        {
            _services = services;
        }

        public KafkaServiceConfiguration Configure(Action<KafkaOptions> action)
        {
            _services.AddOptions<KafkaOptions>().Configure(action);
            return this;
        }
        
        public KafkaServiceConfiguration AddAssembly(Assembly assembly)
        {
            Assemblies.Add(assembly);
            return this;
        }

        #region Interceptors

        public KafkaServiceConfiguration AddInterceptor<T>() where T : class, IMessageInterceptor
        {
            _services.AddSingleton<IMessageInterceptor, T>();
            return this;
        }
        
        public KafkaServiceConfiguration AddInterceptor(Type interceptorType)
        {
            if (!interceptorType.IsAssignableTo(typeof(IMessageInterceptor)))
                throw new ArgumentException($"Invalid interceptor type {interceptorType}");
            
            _services.AddSingleton(typeof(IMessageInterceptor), interceptorType);
            return this;
        }

        public KafkaServiceConfiguration AddInterceptor(IMessageInterceptor interceptor)
        {
            _services.AddSingleton(interceptor);
            return this;
        }
        
        #endregion
    }
}