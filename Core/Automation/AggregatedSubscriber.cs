using System;
using System.CodeDom;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Attributes;
using AspNetCore.Kafka.Data;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.VisualBasic;
using Microsoft.VisualBasic.CompilerServices;

namespace AspNetCore.Kafka.Automation
{
    internal interface ISubscriber
    {
        IMessageSubscription Subscribe();
    }

    internal class AggregatedSubscriber<T> : ISubscriber where T : class
    {
        private readonly ILogger _log;
        private readonly IServiceProvider _provider;
        private readonly IKafkaConsumer _consumer;
        private readonly IEnumerable<MethodInfo> _methods;
        private readonly ConcurrentDictionary<Type, object> _instanceCache;
        
        private readonly List<ITargetBlock<IMessage<T>>> _targets = new();

        public AggregatedSubscriber(
            ILogger<AggregatedSubscriber<T>> log,
            IServiceProvider provider,
            IKafkaConsumer consumer,
            ConcurrentDictionary<Type, object> instanceCache,
            IEnumerable<MethodInfo> methods)
        {
            _log = log;
            _provider = provider;
            _consumer = consumer;
            _instanceCache = instanceCache;
            _methods = methods;
        }

        public IMessageSubscription Subscribe()
        {
            var topicOptions = _methods.Select(x => x.GetSubscriptionOptions()).ToList();
            var target = topicOptions.Select(x => new
                {
                    Topic = x.Topic,
                    Options = x.Options with
                    {
                        Buffer = topicOptions.Select(o => o.Options.Buffer).Max()
                    }
                })
                .First();
            
            foreach (var method in _methods)
            {
                _targets.Add(GetTarget(method, out var flow));   
                _log.LogInformation("Kafka handler: {Method} => {Flow}", 
                        Regex.Replace(method.ToString() ?? string.Empty, @".* (.*)\(.*\.(.*)`[0-9]*\[.*\.(.*)\]\)", "$1($2<$3>)"),
                    //string.Join(" ", method.ToString()?.Split(' ').Skip(1) ?? Array.Empty<string>()), 
                    string.Join(", ", flow));
            }

            return _consumer.Subscribe<T>(target.Topic, HandleAsync, target.Options);
        }

        private async Task HandleAsync(IMessage<T> message)
            => await Task.WhenAll(_targets.Select(x => x.SendAsync(message)));
        
        private ITargetBlock<IMessage<T>> GetTarget(MethodInfo method, out string[] info)
        {
            var attributes = method.GetCustomAttributes<MessageBlockAttribute>().ToList();
            var instance = _instanceCache.GetOrAdd(method.DeclaringType,
                ActivatorUtilities.GetServiceOrCreateInstance(_provider, method.DeclaringType!));

            var beforeBlocks = attributes
                .Where(x => x.Stage == BlockStage.Transform || x.Stage == BlockStage.Message)
                .OrderBy(x => x.Stage)
                .Select(x => Resolve(x, _provider))
                .ToArray();
            var afterBlocks = attributes
                .Where(x => x.Stage == BlockStage.Commit)
                .Select(x => Resolve(x, _provider))
                .ToArray();

            info = beforeBlocks.Concat(afterBlocks).Select(x => x.ToString()).ToArray();
                
            var before = beforeBlocks.Select(x => x.CreateBlock<T>()).ToArray();
            var after = afterBlocks.Select(x => x.CreateBlock<T>()).ToArray();

            var finalType = before.Select(x =>
                x.GetBlockMessageType(typeof(ISourceBlock<>))).LastOrDefault() ?? typeof(IMessage<T>);

            return (ITargetBlock<IMessage<T>>) GetType().GetMethod(
                    nameof(Link),
                    BindingFlags.Static | BindingFlags.NonPublic)!
                .MakeGenericMethod(finalType).Invoke(null, new object[]
                {
                    before,
                    method.CreateDelegate(instance),
                    after
                });
        }

        private static ITargetBlock<IMessage<T>> Link<TFinal>(IDataflowBlock[] before, Func<TFinal, Task> action, IDataflowBlock[] after)
        {
            var handleBlock = new TransformBlock<TFinal, TFinal>(async x =>
                {
                    await action(x);
                    return x;
                },
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = 1,
                    EnsureOrdered = true
                });

            var finalBlock = new ActionBlock<IMessageOffset>(x => { }, new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = 1,
                EnsureOrdered = true
            });

            var pipeline = before.Concat(new[] {handleBlock}).Concat(after).Concat(new[] {finalBlock}).ToArray();
            
            var _ = pipeline.Skip(1).Aggregate(pipeline.First(), (target, next) =>
            {
                target.GetType().GetMethod(nameof(ISourceBlock<int>.LinkTo))!.Invoke(target, new object[] {next, new DataflowLinkOptions()});
                return next;
            });

            return (ITargetBlock<IMessage<T>>) pipeline.First();
        }

        private static IMessageBlock Resolve(MessageBlockAttribute block, IServiceProvider provider)
        {
            var argument = block.ArgumentType is not null
                ? provider.GetService(block.ArgumentType)
                  ?? Versioned.CallByName(
                      provider.GetRequiredService(typeof(IOptions<>).MakeGenericType(block.ArgumentType)),
                      "Value",
                      CallType.Get)
                : block;

            argument ??= new InvalidOperationException($"Null options provided for {block}");

            try
            {
                return (IMessageBlock) ActivatorUtilities.CreateInstance(provider, block.Type, argument);
            }
            catch (InvalidOperationException)
            {
                return (IMessageBlock) ActivatorUtilities.CreateInstance(provider, block.Type);
            }
        }
    }
}