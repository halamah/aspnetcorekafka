using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MoreLinq;

namespace AspNetCore.Kafka.Automation
{
    public class ConsumerHostedService : IHostedService
    {
        private record SubscriptionInfo(string Topic, Type Type, SubscriptionOptions Options, ITargetBlock<IMessage> Block);
        
        private const BindingFlags NonPublicInstance = BindingFlags.Instance | BindingFlags.NonPublic;
            
        private readonly IServiceScopeFactory _factory;
        private readonly KafkaServiceConfiguration _serviceConfiguration;
        private readonly IKafkaConsumer _kafka;
        private readonly ILogger<ConsumerHostedService> _log;
        private readonly List<IMessageSubscription> _subscriptions = new();
        
        public ConsumerHostedService(
            IServiceScopeFactory factory,
            KafkaServiceConfiguration serviceConfiguration,
            IKafkaConsumer kafka, 
            ILogger<ConsumerHostedService> log)
        {
            _factory = factory;
            _serviceConfiguration = serviceConfiguration;
            _kafka = kafka;
            _log = log;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            static bool Exec(Action a) { a(); return true; }
            
            using var scope = _factory.CreateScope();
            var provider = scope.ServiceProvider;
            var instances = new ConcurrentDictionary<Type, object>();

            var assemblies = new[]
                {
                    Assembly.GetEntryAssembly(), 
                    Assembly.GetExecutingAssembly()
                }
                .Concat(_serviceConfiguration.Assemblies)
                .ToImmutableHashSet();
            
            var methods = assemblies
                .GetMessageHandlerTypes()
                .GetMessageHandlerMethods();

            var topics = methods.Select(x => new
                {
                    Method = x,
                    Subscription = x.GetSubscriptionOptions(),
                    MessageType = x.GetContractType() ?? throw new ArgumentException($"Unsupported handler type {x}")
                })
                .GroupBy(x => (Name: x.Subscription.Topic, x.MessageType));

            var subscribers = from keyValue in topics
                let topic = keyValue.Key
                let subscriberType = typeof(AggregatedSubscriber<>).MakeGenericType(topic.MessageType)
                select (ISubscriber) ActivatorUtilities.CreateInstance(
                    provider,
                    subscriberType,
                    instances,
                    keyValue.Select(x => x.Method));

            _subscriptions.AddRange(subscribers.Select(x => x.Subscribe()).ToList());
                
            _log.LogInformation("Created {Count} Kafka subscription(s)", _subscriptions.Count);

            return Task.CompletedTask;
        }

        private IMessageSubscription Subscribe(string topic, List<SubscriptionInfo> subscription)
        {
            var contractType = subscription.Select(x => x.Type).First();
            
            return (IMessageSubscription) GetType()
                    .GetMethod(nameof(SubscribeAggregate), NonPublicInstance)!
                    .MakeGenericMethod(contractType)
                    .Invoke(this, new object[]
                    {
                        topic,
                        subscription.Select(x => x.Options).First(),
                        subscription.Select(x => x.Block).ToList()
                    });
        }
        
        private IMessageSubscription SubscribeAggregate<T>(string topic, SubscriptionOptions options, IEnumerable<ITargetBlock<IMessage>> chains) where T : class =>
            _kafka.Subscribe<T>(topic, x => AggregateAsync(chains, x), options);

        private static Task AggregateAsync<T>(IEnumerable<ITargetBlock<IMessage>> chains, IMessage<T> message)
            => Task.WhenAll(chains.Select(x => x.SendAsync(message)));

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _subscriptions.ForEach(x => x.Unsubscribe());
            return Task.CompletedTask;
        }
    }
}