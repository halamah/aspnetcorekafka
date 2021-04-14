using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Attributes;
using AspNetCore.Kafka.Client.Consumer;
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
        private readonly IKafkaConsumer _consumer;
        private readonly ILogger<ConsumerHostedService> _log;
        private readonly List<IMessageSubscription> _subscriptions = new();
        
        public ConsumerHostedService(
            IServiceScopeFactory factory,
            KafkaServiceConfiguration serviceConfiguration,
            IKafkaConsumer consumer, 
            ILogger<ConsumerHostedService> log)
        {
            _factory = factory;
            _serviceConfiguration = serviceConfiguration;
            _consumer = consumer;
            _log = log;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
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

            var duplicate = methods.GroupBy(x => x.GetSubscriptionOptions().Topic).FirstOrDefault(x => x.Count() > 1)?.Key;

            if (!string.IsNullOrEmpty(duplicate))
                throw new InvalidOperationException($"Duplicate subscription for topic {duplicate}");

            _subscriptions.AddRange(from method in methods
                let contractType = method.GetContractType()
                let messageType = method.GetParameters().Single().ParameterType
                let type = method.DeclaringType
                let instance = instances.GetOrAdd(type, ActivatorUtilities.GetServiceOrCreateInstance(provider, type))
                let target = method.GetSubscriptionOptions()
                select (IMessageSubscription)
                    GetType().GetMethod(nameof(Subscribe), BindingFlags.NonPublic | BindingFlags.Instance)!
                        .MakeGenericMethod(contractType)
                        .Invoke(this, new[] {target.Topic, target.Options, instance, method, method.GetCustomAttributes()}));

            _log.LogInformation("Created {Count} Kafka subscription(s)", _subscriptions.Count);

            return Task.CompletedTask;
        }

        private IMessageSubscription Subscribe<TContract>(
            string topic,
            SubscriptionOptions options,
            object instance,
            MethodInfo method,
            IEnumerable<Attribute> attributes) where TContract : class
        {
            T GetAttribute<T>() where T : class => attributes.FirstOrDefault(x => x is T) as T;

            IMessagePipeline Buffer(IMessagePipeline<IMessage<TContract>, IMessage<TContract>> p)
            {
                if (GetAttribute<BufferAttribute>() is var x and not null)
                    return Batch(p.Buffer(x.Size));
                
                return Batch(p);
            }
            
            IMessagePipeline Batch(IMessagePipeline<IMessage<TContract>, IMessage<TContract>> p)
            {
                if (GetAttribute<BatchAttribute>() is var x and not null)
                    return Action(p.Batch(x.Size, x.Time));
                
                return Action(p);
            }
            
            IMessagePipeline Action<T>(IMessagePipeline<IMessage<TContract>, T> p) where T : IMessageOffset
            {
                var sourceType = typeof(T);
                var contactType = typeof(TContract);
                var parameter = Expression.Parameter(sourceType);
                var destinationType = method.GetParameters().Single().ParameterType;

                var call = destinationType != contactType
                    ? Expression.Call(Expression.Constant(instance), method,
                        Expression.Convert(parameter, destinationType))
                    : Expression.Call(Expression.Constant(instance), method,
                        Expression.Property(parameter, nameof(IMessage<TContract>.Value)));
                
                var lambda = Expression.Lambda<Func<T, Task>>(call, parameter).Compile();

                return Commit((IMessagePipeline<IMessage<TContract>, IMessageOffset>) p.Action(_log, lambda));
            }
            
            IMessagePipeline Commit(IMessagePipeline<IMessage<TContract>, IMessageOffset> p)
            {
                return GetAttribute<CommitAttribute>() is var x and not null ? p.Commit() : p;
            }
            
            var pipeline = _consumer.Pipeline<TContract>(topic, options);

            /*
            if (GetAttribute<PipelineAttribute>() is var p and not null)
            {
                var type = p.Type ?? throw new ArgumentNullException(nameof(pipeline));

                if (type.IsGenericTypeDefinition)
                    type = type.MakeGenericType(typeof(TContract));

                var builder =
                    (IMessagePipelineFactory<TContract>) ActivatorUtilities.GetServiceOrCreateInstance(provider, type);

                return builder.Build(pipeline).Subscribe();
            }*/
            
            return Buffer(pipeline).Subscribe();
        }
        
        public Task StopAsync(CancellationToken cancellationToken)
        {
            _subscriptions.ForEach(x => x.Unsubscribe());
            return Task.CompletedTask;
        }
    }
}