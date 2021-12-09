using System;
using System.Runtime.CompilerServices;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Options;
using Avro.Generic;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

[assembly: InternalsVisibleTo("AspNetCore.Kafka.Mock")]
namespace AspNetCore.Kafka
{
    public class KafkaConfigurationBuilder
    {
        internal IServiceCollection Services { get; }
        
        public KafkaConfigurationBuilder(IServiceCollection services) => Services = services;

        public KafkaConfigurationBuilder ConfigureSerializer<T>(Func<IServiceProvider, IKafkaMessageSerializer<T>> serializer)
        {
            if (typeof(T) != typeof(string) && typeof(T) != typeof(GenericRecord))
                throw new ArgumentException("Serializer may be implemented for String or GenericRecord types only");
            
            Services.Replace(new ServiceDescriptor(typeof(IKafkaMessageSerializer<T>), serializer, ServiceLifetime.Transient));
            return this;
        }
        
        public KafkaConfigurationBuilder ConfigureSerializer<T>(IKafkaMessageSerializer<T> serializer)
        {
            if (typeof(T) != typeof(string) && typeof(T) != typeof(GenericRecord))
                throw new ArgumentException("Serializer may be implemented for String or GenericRecord types only");
            
            Services.Replace(new ServiceDescriptor(typeof(IKafkaMessageSerializer<T>), _ => serializer, ServiceLifetime.Transient));
            return this;
        }
        
        public KafkaConfigurationBuilder Configure(Action<KafkaOptions> action)
        {
            Services.AddOptions<KafkaOptions>().Configure(action);
            return this;
        }

        #region Interceptors

        public KafkaConfigurationBuilder AddInterceptor<T>() where T : class, IMessageInterceptor
        {
            Services.AddSingleton<IMessageInterceptor, T>();
            return this;
        }
        
        public KafkaConfigurationBuilder AddInterceptor(Type interceptorType)
        {
            if (!interceptorType.IsAssignableTo(typeof(IMessageInterceptor)))
                throw new ArgumentException($"Invalid interceptor type {interceptorType}");
            
            Services.AddSingleton(typeof(IMessageInterceptor), interceptorType);
            return this;
        }

        public KafkaConfigurationBuilder AddInterceptor(IMessageInterceptor interceptor)
        {
            Services.AddSingleton(interceptor);
            return this;
        }
        
        #endregion
    }
}