using System;
using System.Runtime.CompilerServices;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

[assembly: InternalsVisibleTo("AspNetCore.Kafka.Mock")]
namespace AspNetCore.Kafka
{
    public class KafkaConfigurationBuilder
    {
        internal IServiceCollection Services { get; }
        
        public KafkaConfigurationBuilder(IServiceCollection services) => Services = services;

        public KafkaConfigurationBuilder ConfigureJsonSerializer(Func<IServiceProvider, IKafkaMessageJsonSerializer> serializer)
        {
            Services.Replace(new ServiceDescriptor(typeof(IKafkaMessageJsonSerializer), serializer, ServiceLifetime.Transient));
            return this;
        }
        
        public KafkaConfigurationBuilder ConfigureJsonSerializer(IKafkaMessageJsonSerializer serializer)
        {
            Services.Replace(new ServiceDescriptor(typeof(IKafkaMessageJsonSerializer), _ => serializer, ServiceLifetime.Transient));
            return this;
        }
        
        public KafkaConfigurationBuilder ConfigureAvroSerializer(Func<IServiceProvider, IKafkaMessageAvroSerializer> serializer)
        {
            Services.Replace(new ServiceDescriptor(typeof(IKafkaMessageAvroSerializer), serializer, ServiceLifetime.Transient));
            return this;
        }
        
        public KafkaConfigurationBuilder ConfigureAvroSerializer(IKafkaMessageAvroSerializer serializer)
        {
            Services.Replace(new ServiceDescriptor(typeof(IKafkaMessageAvroSerializer), _ => serializer, ServiceLifetime.Transient));
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