using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Automation
{
    public class SubscriptionService : ISubscriptionService
    {
        private readonly KafkaServiceConfiguration _serviceConfiguration;
        private readonly IServiceProvider _provider;
        private readonly ILogger _log;
        private readonly IConfiguration _config;
        private readonly List<IMessageSubscription> _subscriptions = new();
        private readonly ConcurrentDictionary<Type, object> _instances = new();

        public SubscriptionService(
            ILogger<SubscriptionService> log,
            KafkaServiceConfiguration serviceConfiguration,
            IConfiguration config, 
            IServiceProvider provider)
        {
            _log = log;
            _serviceConfiguration = serviceConfiguration;
            _config = config;
            _provider = provider;
        }

        public IReadOnlyCollection<IMessageSubscription> Subscriptions => _subscriptions;

        public Task<IEnumerable<IMessageSubscription>> SubscribeConfiguredAssembliesAsync()
            => SubscribeFromAssembliesAsync(
                _serviceConfiguration.Assemblies.Concat(new[] {Assembly.GetEntryAssembly()}));

        public Task<IEnumerable<IMessageSubscription>> SubscribeFromAssembliesAsync(
            IEnumerable<Assembly> assemblies,
            Func<Type, bool> filter = null)
            => SubscribeFromTypesAsync(assemblies.GetMessageHandlerTypes(), filter);

        public Task<IEnumerable<IMessageSubscription>> SubscribeFromTypesAsync(IEnumerable<Type> types, Func<Type, bool> filter = null)
        {
            try
            {
                var methods = types.Where(x => filter?.Invoke(x) ?? true).GetMessageHandlerMethods();
                var definitions = methods.SelectMany(x => x.GetSubscriptionDefinitions(_config)).ToList();
                var duplicate = definitions.GroupBy(x => x.Topic).FirstOrDefault(x => x.Count() > 1)?.Key;

                if (!string.IsNullOrEmpty(duplicate))
                    throw new InvalidOperationException($"Duplicate subscription for topic {duplicate}");

                var subscriptionEnumerable = from definition in definitions
                    let contractType = definition.MethodInfo.GetContractType()
                    let messageType = definition.MethodInfo.GetParameters().Single().ParameterType
                    let type = definition.MethodInfo.DeclaringType
                    select (IMessageSubscription)
                        GetType().GetMethod(nameof(Subscribe), BindingFlags.NonPublic | BindingFlags.Instance)!
                            .MakeGenericMethod(contractType)
                            .Invoke(this, new[] {definition, GetServiceOrCreateInstance(type)});

                var subscriptions = subscriptionEnumerable.ToList();

                _log.LogInformation("Created {Count} Kafka subscription(s) from assemblies", subscriptions.Count);

                return Task.FromResult(subscriptions.AsEnumerable());
            }
            catch (TargetInvocationException e) when (e.InnerException is not null)
            {
                ExceptionDispatchInfo.Capture(e.InnerException).Throw();
            }

            throw new InvalidOperationException("Must not be reached");
        }

        public void Register(IMessageSubscription subscription)
        {
            _subscriptions.Add(subscription);
            
            _log.LogInformation("Total subscriptions count is {Count}", _subscriptions.Count);
        }

        public Task Shutdown() =>
            Task.Run(() => WaitHandle.WaitAll(_subscriptions.Select(x => x.Unsubscribe()).ToArray()));

        public object GetServiceOrCreateInstance(Type type) 
            => _instances.GetOrAdd(type, ActivatorUtilities.GetServiceOrCreateInstance(_provider, type));

        private IMessageSubscription Subscribe<TContract>(SubscriptionDefinition definition, object instance) where TContract : class
        {
            var info = $"{definition.MethodInfo.DeclaringType!.Name}.{definition.MethodInfo.Name}";

            T GetBlock<T>() where T : class => definition.Blocks.LastOrDefault(x => x is T) as T;

            IMessagePipelineSource<TContract> Buffer(IMessagePipeline<TContract, IMessage<TContract>> p)
            {
                if (GetBlock<BufferAttribute>() is var x and not null)
                {
                    info += $" => buffer({x.Size})";
                    return Parallel(p.Buffer(x.Size));
                }

                return Parallel(p);
            }
            
            IMessagePipelineSource<TContract> Parallel(IMessagePipeline<TContract, IMessage<TContract>> p)
            {
                if (GetBlock<ParallelAttribute>() is var x and not null)
                {
                    info += $" => parallel({x.By}, {x.DegreeOfParallelism})";
                    return Batch(p.AsParallel(x.By, x.DegreeOfParallelism));
                }

                return Batch(p);
            }
            
            IMessagePipelineSource<TContract> Batch(IMessagePipeline<TContract, IMessage<TContract>> p)
            {
                if (GetBlock<BatchAttribute>() is var x and not null)
                {
                    info += $" => batch({x.Size}, {x.Time})";
                    return Action(p.Batch(x.Size, x.Time));
                }

                return Action(p);
            }
            
            IMessagePipelineSource<TContract> Action<T>(IMessagePipeline<TContract, T> p) where T : IMessageOffset
            {
                var policy = GetBlock<FailureAttribute>();
                
                info += $" => action({policy?.Behavior ?? Failure.Retry})";
                
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

                return policy is not null
                    ? Commit((IMessagePipeline<TContract, IMessageOffset>) p.Action(lambda, policy.Behavior))
                    : Commit((IMessagePipeline<TContract, IMessageOffset>) p.Action(lambda));
            }
            
            IMessagePipelineSource<TContract> Commit(IMessagePipeline<TContract, IMessageOffset> p)
            {
                if (GetBlock<CommitAttribute>() is not null)
                {
                    info += " => commit()";
                    return p.Commit();
                }

                return p;
            }
            
            var pipeline = Buffer(_provider.GetRequiredService<IKafkaConsumer>().Message<TContract>());

            _log.LogInformation("Subscription info: {Topic}: {Info}", definition.Topic, info);
            
            return pipeline.Subscribe(definition.Topic, definition.Options);
        }
    }
}