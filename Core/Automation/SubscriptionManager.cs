using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Automation.Pipeline;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Automation
{
    public class SubscriptionManager : ISubscriptionManager
    {
        private readonly KafkaServiceConfiguration _serviceConfiguration;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly IKafkaConsumer _consumer;
        private readonly ILogger _log;
        private readonly IConfiguration _config;

        public SubscriptionManager(
            ILogger<SubscriptionManager> log,
            KafkaServiceConfiguration serviceConfiguration,
            IConfiguration config, 
            IServiceScopeFactory scopeFactory, 
            IKafkaConsumer consumer)
        {
            _log = log;
            _serviceConfiguration = serviceConfiguration;
            _config = config;
            _scopeFactory = scopeFactory;
            _consumer = consumer;
        }

        public Task<IEnumerable<IMessageSubscription>> SubscribeFromAssembliesAsync()
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
                    select (IMessageSubscription)
                        GetType().GetMethod(nameof(Subscribe), BindingFlags.NonPublic | BindingFlags.Instance)!
                            .MakeGenericMethod(contractType)
                            .Invoke(this, new object[] {definition});

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

        private IMessageSubscription Subscribe<TContract>(SubscriptionDefinition definition) where TContract : class
        {
            var info = $"{definition.MethodInfo.DeclaringType!.Name}.{definition.MethodInfo.Name}";
            var scope = _scopeFactory.CreateScope();
                
            T GetPolicy<T>() where T : class => definition.Policies.LastOrDefault(x => x is T) as T;

            IMessagePipeline<TContract> Where(IMessagePipeline<TContract, IMessage<TContract>> p)
            {
                if (GetPolicy<OptionsAttribute>() is var x and not null && x.Flags.IsSet(Option.SkipNullMessages))
                {
                    info += $" => message is not null";
                    return Buffer(p.Where(message => message is not null));
                }

                return Buffer(p);
            }
            
            IMessagePipeline<TContract> Buffer(IMessagePipeline<TContract, IMessage<TContract>> p)
            {
                if (GetPolicy<BufferAttribute>() is var x and not null)
                {
                    info += $" => buffer({x.Size})";
                    return Parallel(p.Buffer(x.Size));
                }

                return Parallel(p);
            }
            
            IMessagePipeline<TContract> Parallel(IMessagePipeline<TContract, IMessage<TContract>> p)
            {
                if (GetPolicy<ParallelAttribute>() is var x and not null)
                {
                    info += $" => parallel({x.By}, {x.DegreeOfParallelism})";
                    return Batch(p.AsParallel(x.By, x.DegreeOfParallelism));
                }

                return Batch(p);
            }
            
            IMessagePipeline<TContract> Batch(IMessagePipeline<TContract, IMessage<TContract>> p)
            {
                if (GetPolicy<BatchAttribute>() is var x and not null)
                {
                    info += $" => batch({x.Size}, {x.Time})";
                    return Action(p.Batch(x.Size, x.Time));
                }

                return Action(p);
            }
            
            IMessagePipeline<TContract> Action<T>(IMessagePipeline<TContract, T> p) where T : ICommittable
            {
                var flags = GetPolicy<OptionsAttribute>();
                
                info += $" => action({flags?.Flags & ~Option.SkipNullMessages})";
                
                var sourceType = typeof(T);
                var contactType = typeof(TContract);
                var parameter = Expression.Parameter(sourceType);
                var destinationType = definition.MethodInfo.GetParameters().Single().ParameterType;

                var instanceExpression = Expression.Convert(Expression.Call(
                    typeof(ActivatorUtilities).GetMethods().FirstOrDefault(x =>
                        x.Name == nameof(ActivatorUtilities.GetServiceOrCreateInstance) && !x.IsGenericMethod)!,
                    Expression.Constant(scope.ServiceProvider),
                    Expression.Constant(definition.MethodInfo.DeclaringType)), definition.MethodInfo.DeclaringType);
                    
                var call = destinationType != contactType
                    ? Expression.Call(instanceExpression, definition.MethodInfo,
                        Expression.Convert(parameter, destinationType))
                    : Expression.Call(instanceExpression, definition.MethodInfo,
                        Expression.Property(parameter, nameof(IMessage<TContract>.Value)));
                
                var lambda = Expression.Lambda<Func<T, Task>>(call, parameter).Compile();

                return Commit(
                    (IMessagePipeline<TContract, ICommittable>) p.Action(lambda, flags?.Flags ?? Option.None));
            }
            
            IMessagePipeline<TContract> Commit(IMessagePipeline<TContract, ICommittable> p)
            {
                if (GetPolicy<CommitAttribute>() is not null)
                {
                    info += " => commit()";
                    return p.Commit();
                }

                return p;
            }
            
            var pipeline = Where(_consumer.Message<TContract>());

            _log.LogInformation("Subscription info: {Topic}: {Info}", definition.Topic, info);
            
            return pipeline.Subscribe(definition.Topic, definition.Options);
        }
    }
}