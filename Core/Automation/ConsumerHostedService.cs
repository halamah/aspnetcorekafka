using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
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
        private record SubscriptionInfo(string Topic, Type Type, SubscriptionOptions Options, Delegate Handler, string BlockInfo);
        
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

            var subscriptions = from method in methods
                let type = method.DeclaringType
                let contractType = ActionMessageBlock.GetContractType(method) ?? throw new ArgumentException($"Unsupported handler type {type}.{method}")
                let messageType = typeof(IMessage<>).MakeGenericType(contractType)
                let _ = Exec(() => _log.LogInformation("Found message handler {Class}.{Method}({MessageType})", type!.Name, method.Name, contractType))
                let block = method.ResolveBlock(provider)
                let subscription = method.GetSubscriptionOptions()
                let instance = instances.GetOrAdd(type, ActivatorUtilities.GetServiceOrCreateInstance(provider, type!))
                let create = block.GetType().GetMethod("Create") ?? throw new ArgumentException("Block method 'Create' not found")
                let handler = (Delegate) create.MakeGenericMethod(contractType).Invoke(block, new object[] { ActionMessageBlock.CreateDelegate(instance, method) })
                select new SubscriptionInfo(subscription.Topic, contractType, subscription.Options, handler, block.ToString());

            var aggregated = subscriptions
                .GroupBy(x => x.Topic)
                .ToDictionary(x => x.Key,
                    x => x.Select(s =>
                            s with
                            {
                                Options = s.Options with
                                {
                                    Buffer = x.Select(o => o.Options.Buffer).Max(),
                                }
                            })
                        .ToList());

            aggregated.SelectMany(x => x.Value).ForEach(x =>
                _log.LogInformation(
                    "Message(Topic: {Topic}, Format: '{Format}', Offset: '{Offset}', Bias: '{Bias}', Buffer: '{Buffer}', Block: '{Block}'",
                    x.Topic, x.Options.Format, x.Options.Offset, x.Options.Bias, x.Options, x.BlockInfo));

            _subscriptions.AddRange(aggregated.Select(x => Subscribe(x.Key, x.Value)));

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
                        subscription.Select(x => x.Handler).ToList()
                    });
        }
        
        private IMessageSubscription SubscribeAggregate<T>(string topic, SubscriptionOptions options, IEnumerable<Delegate> aggregate) where T : class =>
            _kafka.Subscribe<T>(topic, x => AggregateAsync(aggregate, x), options);

        private static Task AggregateAsync<T>(IEnumerable<Delegate> aggregate, IMessage<T> message)
            => Task.WhenAll(aggregate.Select(x => ((Func<IMessage<T>, Task>) x)(message)));

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _subscriptions.ForEach(x => x.Unsubscribe());
            return Task.CompletedTask;
        }
    }
}