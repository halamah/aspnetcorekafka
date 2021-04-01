using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Attributes;
using AspNetCore.Kafka.Core;
using AspNetCore.Kafka.Data;
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
        private readonly ILogger<ConsumerHostedService> _logger;
        private readonly List<IMessageSubscription> _subscriptions = new();
        
        public ConsumerHostedService(
            IServiceScopeFactory factory,
            KafkaServiceConfiguration serviceConfiguration,
            IKafkaConsumer kafka, 
            ILogger<ConsumerHostedService> logger)
        {
            _factory = factory;
            _serviceConfiguration = serviceConfiguration;
            _kafka = kafka;
            _logger = logger;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var assemblies = Enumerable.ToHashSet(new[] {Assembly.GetEntryAssembly(), Assembly.GetExecutingAssembly()}).Concat(_serviceConfiguration.Assemblies);

            var methods = assemblies
                .SelectMany(x => x.GetTypes())
                .Where(x => x.IsClass && !x.IsAbstract && !x.IsInterface)
                .Where(x => x.GetCustomAttribute<MessageAttribute>() != null || x.Name.EndsWith("MessageHandler"))
                .SelectMany(x => x.GetMethods(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.InvokeMethod))
                .Where(x => x.GetCustomAttribute<MessageAttribute>() != null)
                .ToList();

            var handlers = new ConcurrentDictionary<string, List<Delegate>>();
            var subscribers = new ConcurrentDictionary<string, Func<IMessageSubscription>>();

            _logger.LogInformation("Found {Count} Kafka handler(s)", methods.Count);
            
            foreach (var method in methods)
            {
                using var scope = _factory.CreateScope();
                var provider = scope.ServiceProvider;
                
                var type = method.DeclaringType;
                var definition = method.GetCustomAttribute<MessageAttribute>();
                var converterAttribute = method.GetCustomAttribute<MessageConverterAttribute>() ?? 
                                         new MessageConverterAttribute(typeof(DefaultMessageConverter));
                var converter = converterAttribute.CreateInstance(provider, method) as IMessageConverter;

                if (converter is null)
                    throw new ArgumentException("Message converter initialization failure");

                var typeDefinition = TopicTypeDefinition.FromType(converter!.MessageType);
                definition!.Format = typeDefinition?.Format ?? definition!.Format;
                definition.Topic = typeDefinition?.Topic ?? definition!.Topic;
                
                if (string.IsNullOrEmpty(definition.Topic))
                    throw new ArgumentException($"Missing topic name for {type!.Name}.{method.Name}");
                
                if(converter!.Info is var converterInfo and not null)
                    _logger.LogInformation("Topic {Topic} converter: {ConverterInfo}", definition.Topic, converterInfo);

                var instance = ActivatorUtilities.GetServiceOrCreateInstance(provider, type!);
                var subscriber = _kafka.GetType().GetMethod(nameof(_kafka.Subscribe))!.MakeGenericMethod(converter.MessageType);
                var actualHandlerType = typeof(Func<,>).MakeGenericType(converter.TargetType, method.ReturnType);
                var sinkHandlerType = typeof(Func<,>).MakeGenericType(converter.PayloadType, method.ReturnType);
                var actualDelegate = Delegate.CreateDelegate(actualHandlerType, instance, method);

                var parameter = Expression.Parameter(converter.PayloadType);
                
                var sinkCall = Expression.Call(
                    Expression.Constant(converter),
                    converter.GetType().GetMethod(
                        nameof(IMessageConverter.HandleAsync), 
                        BindingFlags.Public | BindingFlags.Instance)!,
                    Expression.Constant(actualDelegate),
                    parameter);

                var sinks = handlers.GetOrAdd(definition.Topic, new List<Delegate>()); 
                
                var aggregateCall = Expression.Call(
                    Expression.Constant(this),
                    GetType().GetMethod(nameof(AggregateAsync), BindingFlags.NonPublic | BindingFlags.Instance)!,
                    Expression.Constant(sinks),
                    parameter);

                var sink = Expression.Lambda(sinkHandlerType, sinkCall, parameter).Compile();
                var aggregate = Expression.Lambda(sinkHandlerType, aggregateCall, parameter).Compile();

                sinks.Add(sink);

                subscribers.GetOrAdd(definition.Topic, () => (IMessageSubscription) subscriber.Invoke(_kafka, new object[]
                    {
                        definition.Topic,
                        aggregate,
                        new SubscriptionOptions
                        {
                            Offset = definition.Offset,
                            Bias = definition.Bias,
                            TopicFormat = definition.Format
                        }
                    })
                );
            }

            _subscriptions.AddRange(subscribers.Values.Select(x => x()).ToList());
            
            if(_subscriptions.Any())
                _logger.LogInformation("Created {Count} Kafka subscription(s)", _subscriptions.Count);

            return Task.CompletedTask;
        }

        private async Task AggregateAsync(IEnumerable<Delegate> sinks, IMessage payload) =>
            await Task.WhenAll(sinks.Select(x => (Task) x.DynamicInvoke(payload))).ConfigureAwait(false);

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _subscriptions.ForEach(x => x.Unsubscribe());
            return Task.CompletedTask;
        }
    }
}