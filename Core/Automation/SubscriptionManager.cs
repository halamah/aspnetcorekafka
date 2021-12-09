using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Automation.Pipeline;
using AspNetCore.Kafka.Data;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Automation
{
    public class SubscriptionManager : ISubscriptionManager
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly IKafkaConsumer _consumer;
        private readonly ILogger _log;
        private readonly IConfiguration _config;

        public SubscriptionManager(
            ILogger<SubscriptionManager> log,
            IConfiguration config, 
            IServiceScopeFactory scopeFactory, 
            IKafkaConsumer consumer)
        {
            _log = log;
            _config = config;
            _scopeFactory = scopeFactory;
            _consumer = consumer;
        }

        public Task<IEnumerable<IMessageSubscription>> SubscribeFromAssembliesAsync(
            IEnumerable<Assembly> assemblies,
            Func<Type, bool> filter = null) => SubscribeFromTypesAsync(assemblies.GetMessageHandlerTypes(), filter);

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

                var subscriptions = subscriptionEnumerable.Where(x => x is not null).ToList();

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
            T GetPolicy<T>() where T : class => definition.Policies.LastOrDefault(x => x is T) as T;
            
            var info = $"{definition.MethodInfo.DeclaringType!.Name}.{definition.MethodInfo.Name}";
            var scope = _scopeFactory.CreateScope();
            var offset = GetPolicy<OffsetAttribute>();
            var offsetInfo = string.Empty;

            if (offset is not null)
                offsetInfo = offset.Value.DateOffset?.ToString() ?? $"{offset.Value.Offset} + {offset.Value.Bias}"; 

            IMessagePipeline<TContract> State(IMessagePipeline<TContract, IMessage<TContract>> p)
            {
                if (GetPolicy<StateAttribute>() is var x and not null && x.State == MessageState.Disabled)
                {
                    info += $" => disabled";
                    return null;
                }

                return Where(p);
            }
            
            IMessagePipeline<TContract> Where(IMessagePipeline<TContract, IMessage<TContract>> p)
            {
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
                var retry = GetPolicy<RetryAttribute>()?.Options;
                var optionsText = string.Empty;

                if (retry is not null)
                    optionsText = "with retry " +
                                  (retry.Count >= 0 ? $"{retry.Count} times" : "infinite") +
                                  (retry.Delay > 0 ? $" after {retry.Delay}ms" : string.Empty);
                
                info += $" => action({optionsText})";
                
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
                    (IMessagePipeline<TContract, ICommittable>) p.Action(lambda, retry));
            }
            
            IMessagePipeline<TContract> Commit(IMessagePipeline<TContract, ICommittable> p)
            {
                if (GetPolicy<CommitAttribute>() is not null)
                {
                    info += " => commit";
                    return p.Commit();
                }

                return p;
            }
            
            var pipeline = State(_consumer.Message<TContract>());

            _log.LogInformation("Subscription info: {Topic}[{Offset}]: {Info}", definition.Topic, offsetInfo, info);
            
            return pipeline?.Subscribe(definition.Topic, definition.Options);
        }
    }
}