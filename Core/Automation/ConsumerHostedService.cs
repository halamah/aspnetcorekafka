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
using AspNetCore.Kafka.Client.Consumer.Pipeline;
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
            
            var subscriptions = methods.SelectMany(x => x.GetSubscriptionDefinitions()).ToList();

            var duplicate = subscriptions.GroupBy(x => x.Topic).FirstOrDefault(x => x.Count() > 1)?.Key;

            if (!string.IsNullOrEmpty(duplicate))
                throw new InvalidOperationException($"Duplicate subscription for topic {duplicate}");

            _subscriptions.AddRange(from subscription in subscriptions
                let contractType = subscription.MethodInfo.GetContractType()
                let messageType = subscription.MethodInfo.GetParameters().Single().ParameterType
                let type = subscription.MethodInfo.DeclaringType
                let instance = instances.GetOrAdd(type, ActivatorUtilities.GetServiceOrCreateInstance(provider, type))
                select (IMessageSubscription)
                    GetType().GetMethod(nameof(Subscribe), BindingFlags.NonPublic | BindingFlags.Instance)!
                        .MakeGenericMethod(contractType)
                        .Invoke(this, new[] {subscription.Topic, subscription.Options, instance, subscription.MethodInfo}));

            _log.LogInformation("Created {Count} Kafka subscription(s)", _subscriptions.Count);

            return Task.CompletedTask;
        }

        private IMessageSubscription Subscribe<TContract>(
            string topic,
            SubscriptionOptions options,
            object instance,
            MethodInfo method) where TContract : class
        {
            var info = $"{method.DeclaringType!.Name}.{method.Name}";
            
            T GetAttribute<T>() where T : class => method.GetCustomAttributes().FirstOrDefault(x => x is T) as T;

            IMessagePipelineSource<TContract> Buffer(IMessagePipeline<TContract, IMessage<TContract>> p)
            {
                if (GetAttribute<BufferAttribute>() is var x and not null)
                {
                    info += $" buffer({x.Size})";
                    return Partitioned(p.Buffer(x.Size));
                }

                return Partitioned(p);
            }
            
            IMessagePipelineSource<TContract> Partitioned(IMessagePipeline<TContract, IMessage<TContract>> p)
            {
                if (GetAttribute<PartitionedAttribute>() is var x and not null)
                {
                    info += $" partitioned({x.MaxDegreeOfParallelism})";
                    return Batch(p.Partitioned(x.MaxDegreeOfParallelism));
                }

                return Batch(p);
            }
            
            IMessagePipelineSource<TContract> Batch(IMessagePipeline<TContract, IMessage<TContract>> p)
            {
                if (GetAttribute<BatchAttribute>() is var x and not null)
                {
                    info += $" batch({x.Size}, {x.Time})";
                    return Action(p.Batch(x.Size, x.Time));
                }

                return Action(p);
            }
            
            IMessagePipelineSource<TContract> Action<T>(IMessagePipeline<TContract, T> p) where T : IMessageOffset
            {
                info += $" action()";
                
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

                return Commit((IMessagePipeline<TContract, IMessageOffset>) p.Action(_log, lambda));
            }
            
            IMessagePipelineSource<TContract> Commit(IMessagePipeline<TContract, IMessageOffset> p)
            {
                info += $" commit()";
                
                return GetAttribute<CommitAttribute>() is var x and not null ? p.Commit() : p;
            }
            
            var pipeline = Buffer(_consumer.Pipeline<TContract>());

            _log.LogInformation("Subscription info: {Topic} => {Info}", topic, info);
            
            return pipeline.Subscribe(topic, options);
        }
        
        public Task StopAsync(CancellationToken cancellationToken)
        {
            _subscriptions.ForEach(x => x.Unsubscribe());
            return Task.CompletedTask;
        }
    }
}