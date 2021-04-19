using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;

namespace AspNetCore.Kafka.Abstractions
{
    public interface ISubscriptionService
    {
        IReadOnlyCollection<IMessageSubscription> Subscriptions { get; }
        
        Task<IEnumerable<IMessageSubscription>> SubscribeConfiguredAssembliesAsync();
        
        Task<IEnumerable<IMessageSubscription>> SubscribeFromAssembliesAsync(IEnumerable<Assembly> assemblies, Func<Type, bool> filter = null);

        void Register(IMessageSubscription subscription);
        
        Task UnsubscribeAllAsync();
    }
}