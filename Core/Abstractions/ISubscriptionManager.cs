using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;

namespace AspNetCore.Kafka.Abstractions
{
    public interface ISubscriptionManager
    {
        Task<IEnumerable<IMessageSubscription>> SubscribeFromAssembliesAsync();
        
        Task<IEnumerable<IMessageSubscription>> SubscribeFromAssembliesAsync(IEnumerable<Assembly> assemblies, Func<Type, bool> filter = null);
        
        Task<IEnumerable<IMessageSubscription>> SubscribeFromTypesAsync(IEnumerable<Type> types, Func<Type, bool> filter = null);

        object GetServiceOrCreateInstance(Type type);
    }
}