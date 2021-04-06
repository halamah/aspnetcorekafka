using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Attributes;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Automation
{
    public class ConsumerHostedService : IHostedService
    {
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
            var handlers = new ConcurrentDictionary<string, List<Delegate>>();
            var subscribers = new ConcurrentDictionary<string, IMessageSubscription>();
            static bool fun(Action a) { a(); return true; }

            var collected = from x in 
                    new[] {Assembly.GetEntryAssembly(), Assembly.GetExecutingAssembly()}.ToHashSet().Concat(_serviceConfiguration.Assemblies)
                    .SelectMany(x => x.GetTypes())
                    .Where(x => x.IsClass && !x.IsAbstract && !x.IsInterface)
                    .Where(x => x.GetCustomAttribute<MessageAttribute>() != null || x.Name.EndsWith("MessageHandler"))
                    .SelectMany(x => x.GetMethods(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.InvokeMethod))
                where x.GetCustomAttribute<MessageAttribute>() != null
                let scope = _factory.CreateScope()
                let provider = scope.ServiceProvider
                let method = x
                let type = x.DeclaringType
                let contractType = ActionMessageBlock.GetContractType(method) ?? throw new ArgumentException($"Unsupported handler type {type}.{method}")
                let messageType = typeof(IMessage<>).MakeGenericType(contractType)
                let _ = fun(() => _log.LogInformation("Found message handler {Class}.{Method}", type!.Name, method.Name))
                let converterInfo = x.GetCustomAttribute<MessageBlockAttribute>() ?? new MessageBlockAttribute(typeof(ActionMessageBlock))
                let argument = converterInfo!.ArgumentType is not null ? provider.GetRequiredService(converterInfo!.ArgumentType) : converterInfo
                let converter = ActivatorUtilities.CreateInstance(provider, converterInfo!.ConverterType, argument)
                let definition = x.GetCustomAttribute<MessageAttribute>()
                let typeDefinition = TopicDefinition.FromType(contractType)
                let topic = definition?.Topic ?? typeDefinition?.Topic ?? throw new ArgumentException($"Missing topic name for {type!.Name}.{method.Name}")
                let __ = fun(() => _log.LogInformation("Topic {Topic} converter: {Info}", topic, converter))
                let format = definition?.Format ?? typeDefinition?.Format ?? TopicFormat.String
                let offset = definition?.Offset ?? typeDefinition?.Offset ?? TopicOffset.Stored
                let bias = definition?.Bias ?? typeDefinition?.Bias ?? 0
                let aggregate = handlers.GetOrAdd(topic, new List<Delegate>())
                let instance = ActivatorUtilities.GetServiceOrCreateInstance(provider, type!)
                let create = converter.GetType().GetMethod("Create") ?? throw new ArgumentException("Converter method 'Create' not found")
                let handler = (Delegate) create.MakeGenericMethod(contractType).Invoke(converter, new object[] { ActionMessageBlock.CreateDelegate(instance, method) })
                let options = new SubscriptionOptions {Offset = offset, Bias = bias, TopicFormat = format}
                let ___ = fun(scope.Dispose)
                let ____ = fun(() => aggregate.Add(handler))
                select subscribers.GetOrAdd(topic, _ => (IMessageSubscription) GetType()
                        .GetMethod(nameof(Subscribe), BindingFlags.Instance|BindingFlags.NonPublic)!
                        .MakeGenericMethod(contractType).Invoke(this, new object []
                {
                    topic, new SubscriptionOptions {Offset = offset, Bias = bias, TopicFormat = format}, aggregate
                }));
            
            _subscriptions.AddRange(collected.Distinct().ToList());
            
            if(_subscriptions.Any())
                _log.LogInformation("Created {Count} Kafka subscription(s)", _subscriptions.Count);

            return Task.CompletedTask;
        }

        private IMessageSubscription Subscribe<T>(string topic, SubscriptionOptions options, ICollection<Delegate> aggregate) where T : class
            => _kafka.Subscribe<T>(topic, x => AggregateAsync(aggregate, x), options);

        private static Task AggregateAsync<T>(IEnumerable<Delegate> aggregate, IMessage<T> message)
            => Task.WhenAll(aggregate.Select(x => ((Func<IMessage<T>, Task>) x)(message)));

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _subscriptions.ForEach(x => x.Unsubscribe());
            return Task.CompletedTask;
        }
    }
}