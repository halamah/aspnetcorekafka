using System;
using System.Collections.Generic;
using System.Reflection;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Options;
using Microsoft.Extensions.DependencyInjection;
using MoreLinq;

namespace AspNetCore.Kafka
{
    public class KafkaServiceConfiguration
    {
        public IServiceCollection Services { get; }
        
        public HashSet<Assembly> Assemblies { get; } = new();
        
        public HashSet<Assembly> TypeFilter { get; } = new();
        
        public KafkaServiceConfiguration(IServiceCollection services)
        {
            Services = services;
        }

        public KafkaServiceConfiguration Configure(Action<KafkaOptions> action)
        {
            Services.AddOptions<KafkaOptions>().Configure(action);
            return this;
        }
        
        public KafkaServiceConfiguration AddAssemblies(params Assembly[] assemblies)
        {
            Assemblies.Add(Assembly.GetEntryAssembly());
            
            assemblies.ForEach(x => Assemblies.Add(x));
            
            return this;
        }

        #region Interceptors

        public KafkaServiceConfiguration AddInterceptor<T>() where T : class, IMessageInterceptor
        {
            Services.AddSingleton<IMessageInterceptor, T>();
            return this;
        }
        
        public KafkaServiceConfiguration AddInterceptor(Type interceptorType)
        {
            if (!interceptorType.IsAssignableTo(typeof(IMessageInterceptor)))
                throw new ArgumentException($"Invalid interceptor type {interceptorType}");
            
            Services.AddSingleton(typeof(IMessageInterceptor), interceptorType);
            return this;
        }

        public KafkaServiceConfiguration AddInterceptor(IMessageInterceptor interceptor)
        {
            Services.AddSingleton(interceptor);
            return this;
        }
        
        #endregion
    }
}