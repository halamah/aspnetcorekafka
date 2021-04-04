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
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using static LanguageExt.Prelude;

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
            var handlers = new ConcurrentDictionary<string, List<IMessageConverter>>();
            var subscribers = new ConcurrentDictionary<string, Func<IMessageSubscription>>();

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
                let _ = fun(() => _logger.LogInformation("Found message handler {Class}.{Method}", type!.Name, method.Name)) ()
                let instance = ActivatorUtilities.GetServiceOrCreateInstance(provider, x.DeclaringType!)
                let converterInfo = x.GetCustomAttribute<MessageConverterAttribute>() ?? new MessageConverterAttribute(typeof(DefaultMessageConverter))
                let converter = converterInfo.CreateInstance(provider, instance, method) as IMessageConverter ?? throw new ArgumentException("Message converter initialization failure")
                let definition = x.GetCustomAttribute<MessageAttribute>()
                let typeDefinition = TopicDefinition.FromType(converter.MessageType)
                let topic = definition?.Topic ?? typeDefinition?.Topic ?? throw new ArgumentException($"Missing topic name for {type!.Name}.{method.Name}")
                let format = definition?.Format ?? typeDefinition?.Format ?? TopicFormat.String
                let offset = definition?.Offset ?? typeDefinition?.Offset ?? TopicOffset.Stored
                let bias = definition?.Bias ?? typeDefinition?.Bias ?? 0
                let subscriber = _kafka.GetType().GetMethod(nameof(_kafka.Subscribe))!.MakeGenericMethod(converter.MessageType)
                let parameter = Expression.Parameter(converter.PayloadType)
                let aggregate = handlers.GetOrAdd(topic, new List<IMessageConverter>())
                let aggregateCall = Expression.Call(
                    Expression.Constant(this),
                    GetType().GetMethod(nameof(AggregateAsync), BindingFlags.NonPublic | BindingFlags.Instance)!,
                    Expression.Constant(aggregate),
                    parameter)
                let handlerType = typeof(Func<,>).MakeGenericType(converter.PayloadType, method.ReturnType)
                let handler = Expression.Lambda(handlerType, aggregateCall, parameter).Compile()
                let subscription = subscribers.GetOrAdd(topic, () => (IMessageSubscription) subscriber.Invoke(_kafka, new object[]
                    {
                        topic,
                        handler,
                        new SubscriptionOptions {Offset = offset, Bias = bias, TopicFormat = format}
                    })
                )
                let __ = fun(() => aggregate.Add(converter)) () 
                let ___ = fun(scope.Dispose) ()
                select subscription();
            
            _subscriptions.AddRange(collected);
            
            if(_subscriptions.Any())
                _logger.LogInformation("Created {Count} Kafka subscription(s)", _subscriptions.Count);

            return Task.CompletedTask;
        }

        private async Task AggregateAsync(IEnumerable<IMessageConverter> aggregate, IMessage payload) =>
            await Task.WhenAll(aggregate.Select(x => x.HandleAsync(payload))).ConfigureAwait(false);

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _subscriptions.ForEach(x => x.Unsubscribe());
            return Task.CompletedTask;
        }
    }
}