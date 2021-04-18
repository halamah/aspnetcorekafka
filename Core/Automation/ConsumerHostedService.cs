using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Client.Consumer.Pipeline;
using AspNetCore.Kafka.Data;
using Microsoft.Extensions.Configuration;
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
        private readonly IConfiguration _config;
        
        public ConsumerHostedService(
            IServiceScopeFactory factory,
            KafkaServiceConfiguration serviceConfiguration,
            IKafkaConsumer consumer, 
            ILogger<ConsumerHostedService> log,
            IConfiguration config)
        {
            _factory = factory;
            _serviceConfiguration = serviceConfiguration;
            _consumer = consumer;
            _log = log;
            _config = config;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            try
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

                var definitions = methods.SelectMany(x => x.GetSubscriptionDefinitions(_config)).ToList();

                var duplicate = definitions.GroupBy(x => x.Topic).FirstOrDefault(x => x.Count() > 1)?.Key;

                if (!string.IsNullOrEmpty(duplicate))
                    throw new InvalidOperationException($"Duplicate subscription for topic {duplicate}");

                _subscriptions.AddRange(from definition in definitions
                    let contractType = definition.MethodInfo.GetContractType()
                    let messageType = definition.MethodInfo.GetParameters().Single().ParameterType
                    let type = definition.MethodInfo.DeclaringType
                    let instance = instances.GetOrAdd(type,
                        ActivatorUtilities.GetServiceOrCreateInstance(provider, type))
                    select (IMessageSubscription)
                        GetType().GetMethod(nameof(Subscribe), BindingFlags.NonPublic | BindingFlags.Instance)!
                            .MakeGenericMethod(contractType)
                            .Invoke(this, new[] {definition, instance}));

                _log.LogInformation("Created {Count} Kafka subscription(s)", _subscriptions.Count);
            }
            catch (TargetInvocationException e) when (e.InnerException is not null)
            {
                ExceptionDispatchInfo.Capture(e.InnerException).Throw();
            }

            return Task.CompletedTask;
        }

        private IMessageSubscription Subscribe<TContract>(SubscriptionDefinition definition, object instance) where TContract : class
        {
            var info = $"{definition.MethodInfo.DeclaringType!.Name}.{definition.MethodInfo.Name}";

            T GetBlock<T>() where T : class => definition.Blocks.FirstOrDefault(x => x is T) as T;

            IMessagePipelineSource<TContract> Buffer(IMessagePipeline<TContract, IMessage<TContract>> p)
            {
                if (GetBlock<BufferAttribute>() is var x and not null)
                {
                    info += $" buffer({x.Size})";
                    return Partitioned(p.Buffer(x.Size));
                }

                return Partitioned(p);
            }
            
            IMessagePipelineSource<TContract> Partitioned(IMessagePipeline<TContract, IMessage<TContract>> p)
            {
                if (GetBlock<PartitionedAttribute>() is var x and not null)
                {
                    info += $" partitioned({x.MaxDegreeOfParallelism})";
                    return Batch(p.Partitioned(x.MaxDegreeOfParallelism));
                }

                return Batch(p);
            }
            
            IMessagePipelineSource<TContract> Batch(IMessagePipeline<TContract, IMessage<TContract>> p)
            {
                if (GetBlock<BatchAttribute>() is var x and not null)
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
                var destinationType = definition.MethodInfo.GetParameters().Single().ParameterType;

                var call = destinationType != contactType
                    ? Expression.Call(Expression.Constant(instance), definition.MethodInfo,
                        Expression.Convert(parameter, destinationType))
                    : Expression.Call(Expression.Constant(instance), definition.MethodInfo,
                        Expression.Property(parameter, nameof(IMessage<TContract>.Value)));
                
                var lambda = Expression.Lambda<Func<T, Task>>(call, parameter).Compile();

                return Commit((IMessagePipeline<TContract, IMessageOffset>) p.Action(_log, lambda));
            }
            
            IMessagePipelineSource<TContract> Commit(IMessagePipeline<TContract, IMessageOffset> p)
            {
                if (GetBlock<CommitAttribute>() is var x and not null)
                {
                    info += $" commit()";
                    return p.Commit();
                }

                return p;
            }
            
            var pipeline = Buffer(_consumer.Message<TContract>());

            _log.LogInformation("Subscription info: {Topic} => {Info}", definition.Topic, info);
            
            return pipeline.Subscribe(definition.Topic, definition.Options);
        }
        
        public Task StopAsync(CancellationToken cancellationToken)
        {
            _subscriptions.ForEach(x => x.Unsubscribe());
            return Task.CompletedTask;
        }
    }
}