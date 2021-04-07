using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Attributes;
using AspNetCore.Kafka.Data;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MoreLinq;

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
            static bool Exec(Action a) { a(); return true; }
            
            using var scope = _factory.CreateScope();
            var provider = scope.ServiceProvider;
            var handlers = new ConcurrentDictionary<string, List<Delegate>>();
            var subscribers = new ConcurrentDictionary<string, Func<IMessageSubscription>>();

            var types =
                    new[] {Assembly.GetEntryAssembly(), Assembly.GetExecutingAssembly()}.Concat(_serviceConfiguration.Assemblies)
                    .ToImmutableHashSet()
                    .SelectMany(x => x.GetTypes())
                    .Where(x => x.IsClass && !x.IsAbstract && !x.IsInterface)
                    .Where(x => x.GetCustomAttribute<MessageAttribute>() != null || x.Name.EndsWith("MessageHandler"))
                    .ToList();

            var methods = types.SelectMany(x => x
                .GetMethods(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.InvokeMethod))
                .Where(x => x.GetCustomAttribute<MessageAttribute>() != null)
                .ToList();

            var buffers = methods
                .Select(x => x.GetCustomAttribute<MessageAttribute>())
                .Zip(methods.Select(x => TopicDefinition.FromType(ActionMessageBlock.GetContractType(x))))
                .Select(x => new { Topic = x.Second?.Topic ?? x.First?.Topic, x.First!.Buffer })
                .OrderByDescending(x => x.Buffer)
                .DistinctBy(x => x.Topic)
                .ToDictionary(x => x.Topic, x => x.Buffer);
                
            var collected = from method in methods
                let type = method.DeclaringType
                let contractType = ActionMessageBlock.GetContractType(method) ?? throw new ArgumentException($"Unsupported handler type {type}.{method}")
                let messageType = typeof(IMessage<>).MakeGenericType(contractType)
                let _ = Exec(() => _log.LogInformation("Found message handler {Class}.{Method}", type!.Name, method.Name))
                let blockInfo = method.GetCustomAttribute<MessageBlockAttribute>() ?? new MessageBlockAttribute(typeof(ActionMessageBlock))
                let argument = blockInfo!.ArgumentType is not null ? provider.GetRequiredService(blockInfo!.ArgumentType) : blockInfo
                let block = ActivatorUtilities.CreateInstance(provider, blockInfo!.ConverterType, argument)
                let definition = method.GetCustomAttribute<MessageAttribute>()
                let baseDefinition = TopicDefinition.FromType(contractType)
                let topic = definition!.Topic ?? baseDefinition?.Topic ?? throw new ArgumentException($"Missing topic name for {type!.Name}.{method.Name}")
                let format = baseDefinition?.Format ?? definition!.Format
                let buffer = buffers.GetValueOrDefault(topic)
                let __ = Exec(() => _log.LogInformation(
                    "Message(Topic: {Topic}, Format: '{Format}', Id: '{Id}', Offset: '{Offset}', Bias: '{Bias}', Buffer: '{Buffer}', {Block}", 
                    topic, format, definition!.Id, definition!.Offset, definition!.Bias, buffer, block))
                let aggregate = handlers.GetOrAdd(topic, new List<Delegate>())
                let instance = ActivatorUtilities.GetServiceOrCreateInstance(provider, type!)
                let create = block.GetType().GetMethod("Create") ?? throw new ArgumentException("Converter method 'Create' not found")
                let handler = (Delegate) create.MakeGenericMethod(contractType).Invoke(block, new object[] { ActionMessageBlock.CreateDelegate(instance, method) })
                let ___ = Exec(() => aggregate.Add(handler))
                select subscribers.GetOrAdd(topic, _ => () => (IMessageSubscription) GetType()
                        .GetMethod(nameof(Subscribe), BindingFlags.Instance|BindingFlags.NonPublic)!
                        .MakeGenericMethod(contractType).Invoke(this, new object []
                {
                    topic, 
                    new SubscriptionOptions {Offset = definition!.Offset, Bias = definition!.Bias, TopicFormat = format, Buffer = buffer}, 
                    aggregate
                }));

            _subscriptions.AddRange(collected.ToList().Distinct().Select(x => x()));
            
            if(_subscriptions.Any())
                _log.LogInformation("Created {Count} Kafka subscription(s)", _subscriptions.Count);

            return Task.CompletedTask;
        }

        private IMessageSubscription Subscribe<T>(string topic, SubscriptionOptions options, ICollection<Delegate> aggregate) where T : class =>
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