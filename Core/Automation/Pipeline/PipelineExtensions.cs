using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Data;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Automation.Pipeline
{
    public static class PipelineExtensions
    {
        public static IMessagePipeline<TSource, TDestination> Action<TSource, TDestination>(
            this IMessagePipeline<TSource, TDestination> pipeline,
            Action<TDestination> handler,
            RetryOptions retryOptions = null,
            CancellationToken cancellationToken = default) where TDestination : IStorable
        {
            return pipeline.Action(x => { handler(x); return Task.CompletedTask; }, retryOptions, cancellationToken);
        }

        public static IMessagePipeline<TContract, TDestination> Action<TContract, TDestination>(
            this IMessagePipeline<TContract, TDestination> pipeline,
            Func<TDestination, Task> handler,
            RetryOptions retryOptions = null,
            CancellationToken cancellationToken = default) where TDestination : IStorable
        {
            return pipeline.Block(() => new TransformBlock<TDestination, TDestination>(async message =>
                {
                    var stopWatch = new Stopwatch();

                    async Task InvokeInterceptors(Exception exception = null)
                    {
                        foreach (var interceptor in pipeline.Consumer.Interceptors)
                        {
                            try
                            {
                                await interceptor.ConsumeAsync(new KafkaInterception
                                    {
                                        Messages = message.Messages.Select(msg => new InterceptedMessage(msg)),
                                        Exception = exception,
                                        Metrics = new InterceptionMetrics { ProcessingTime = stopWatch.Elapsed }
                                    })
                                    .ConfigureAwait(false);
                            }
                            catch (Exception e)
                            {
                                pipeline.Consumer.Log.LogError(e, "Message interception failure");
                            }
                        }
                    }

                    stopWatch.Start();
                    
                    var retries = retryOptions?.Count ?? 1;

                    for (var i = 0; i < retries + 1 || retries < 0; ++i)
                    {
                        try
                        {
                            await handler(message).ConfigureAwait(false);
                            await InvokeInterceptors().ConfigureAwait(false);
                            return message;
                        }
                        catch (Exception e)
                        {
                            pipeline.Consumer.Log.LogError(e, "Message handler failure");

                            if (i == 0)
                                await InvokeInterceptors(e).ConfigureAwait(false);

                            await Task.Delay(retryOptions?.Delay ?? 0, cancellationToken).ConfigureAwait(false);
                        }
                    }
                    
                    pipeline.Consumer.Log.LogError("Message skipped after {RetriesCount} retries", retries);
                    return message;
                },
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = 1,
                    EnsureOrdered = true
                }));
        }

        public static IMessagePipeline<TContract, IMessage<TContract>> AsParallel<TContract>(
            this IMessagePipeline<TContract, IMessage<TContract>> pipeline,
            By by = By.Partition,
            int degreeOfParallelism = -1)
        {
            if(by != By.Partition)
                throw new ArgumentException("Only parallel By.Partition supported");
            
            if(degreeOfParallelism == 0)
                throw new ArgumentException("DegreeOfParallelism cannot be zero");
            
            return degreeOfParallelism == 1
                ? pipeline
                : new ParallelMessagePipeline<TContract, IMessage<TContract>>(pipeline, by, degreeOfParallelism);
        }

        public static IMessagePipeline<TSource, TDestination> Buffer<TSource, TDestination>(
            this IMessagePipeline<TSource, TDestination> pipeline, 
            int size)
        {
            if (size < 1)
                throw new ArgumentException("Buffer size must be greater than 0");

            return pipeline.Block(() => new BufferBlock<TDestination>(new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = size,
                EnsureOrdered = true
            }));
        }

        public static IMessagePipeline<TSource, IStorable> Commit<TSource>(this IMessagePipeline<TSource, IStorable> pipeline)
            => pipeline.Block(() => new TransformBlock<IStorable, IStorable>(x =>
                {
                    x.Commit();
                    return x;
                },
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = 1,
                    EnsureOrdered = true
                }));

        public static IMessagePipeline<TSource, IStorable> Store<TSource>(this IMessagePipeline<TSource, IStorable> pipeline)
            => pipeline.Block(() => new TransformBlock<IStorable, IStorable>(x =>
                {
                    x.Store();
                    return x;
                },
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = 1,
                    EnsureOrdered = true
                }));
        
        public static IMessagePipeline<TSource, TResult> Select<TSource, TDestination, TResult>(
            this IMessagePipeline<TSource, TDestination> pipeline, Func<TDestination, TResult> transform)
        {
            return pipeline.Block(() => new TransformBlock<TDestination, TResult>(transform, new ExecutionDataflowBlockOptions
            {
                EnsureOrdered = true,
                BoundedCapacity = 1
            }));
        }
        
        public static IMessagePipeline<TContract, IMessage<TContract>> Where<TContract>(
            this IMessagePipeline<TContract, IMessage<TContract>> pipeline, Func<TContract, bool> predicate)
        {
            return pipeline.Block(() => new TransformManyBlock<IMessage<TContract>, IMessage<TContract>>(
                message => !predicate(message.Value)
                    ? Enumerable.Empty<IMessage<TContract>>()
                    : new[] {message},
                new ExecutionDataflowBlockOptions
                {
                    EnsureOrdered = true,
                    BoundedCapacity = 1
                }));
        }
        
        public static IMessagePipeline<TContract, IMessage<TContract>> Where<TContract>(
            this IMessagePipeline<TContract, IMessage<TContract>> pipeline, Func<TContract, Task<bool>> predicate)
        {
            return pipeline.Block(() => new TransformManyBlock<IMessage<TContract>, IMessage<TContract>>(
                async message => ! await predicate(message.Value).ConfigureAwait(false)
                    ? Enumerable.Empty<IMessage<TContract>>()
                    : new[] {message},
                new ExecutionDataflowBlockOptions
                {
                    EnsureOrdered = true,
                    BoundedCapacity = 1
                }));
        }
        
        public static IMessagePipeline<TSource, TResult> Select<TSource, TDestination, TResult>(
            this IMessagePipeline<TSource, TDestination> pipeline, Func<TDestination, Task<TResult>> transform)
        {
            return pipeline.Block(() => new TransformBlock<TDestination, TResult>(transform, new ExecutionDataflowBlockOptions
            {
                EnsureOrdered = true,
                BoundedCapacity = 1
            }));
        }
        
        public static IMessagePipeline<T, IMessageEnumerable<T>> Batch<T>(
            this IMessagePipeline<T, IMessage<T>> pipeline,
            int size,
            int time)
        {
            return pipeline.Batch(size, TimeSpan.FromMilliseconds(time));
        }
        
        public static IMessagePipeline<T, IMessageEnumerable<T>> Batch<T>(
            this IMessagePipeline<T, IMessage<T>> pipeline,
            int size, 
            TimeSpan time)
        {
            if (size <= 1)
                throw new ArgumentException("Batch size must be greater than 1");

            return pipeline.Block(() =>
            {
                var batch = new BatchBlock<IMessage<T>>(size, new GroupingDataflowBlockOptions
                {
                    EnsureOrdered = true,
                    BoundedCapacity = size
                });

                var timer = time.TotalMilliseconds > 0
                    ? new Timer(_ => batch.TriggerBatch(), null, Timeout.Infinite, Timeout.Infinite)
                    : null;
                
                void Track() => timer?.Change(time, time);

                var transform = new TransformBlock<IMessage<T>[], IMessageEnumerable<T>>(x =>
                    {
                        Track();
                        return new KafkaMessageEnumerable<T>(x);
                    },
                    new ExecutionDataflowBlockOptions
                    {
                        EnsureOrdered = true,
                        BoundedCapacity = 1
                    });
            
                batch.LinkTo(transform, new DataflowLinkOptions {PropagateCompletion = true });
                
                Track();
                
                return DataflowBlock.Encapsulate(batch, transform);
            });
        }

        public static IMessagePipeline<TContract, IMessage<TContract>> Message<TContract>(this IKafkaConsumer consumer)
            => new MessagePipeline<TContract, IMessage<TContract>>(consumer);

        public static IMessageSubscription Subscribe<TContract, T>(
            this IMessagePipeline<TContract, T> pipeline,
            SourceOptions options = null) where TContract : class => pipeline.Subscribe(null, options);
        
        public static IMessageSubscription Subscribe<TContract>(
            this IMessagePipeline<TContract> pipeline,
            string topic,
            SourceOptions options = null)
        {
            var sink = new PipelineSink<TContract>(pipeline);

            var subscription = pipeline.Consumer.Subscribe<TContract>(
                string.IsNullOrEmpty(topic) ? TopicDefinition.FromType<TContract>().Topic : topic, 
                x => sink.SendAsync(x), options);

            subscription.Revoke += () => sink.CompleteAsync();
            
            return subscription;
        }

        /*
        public static IObservable<IMessage<T>> SubscribeObservable<T>(
            this IMessagePipeline<T, IMessage<T>> pipeline,
            SourceOptions options = null)
            => pipeline.SubscribeObservable(null, options);
        
        public static IObservable<IMessage<T>> SubscribeObservable<T>(
            this IMessagePipeline<T, IMessage<T>> pipeline,
            string topic = null,
            SourceOptions options = null)
        {
            var sink = new PipelineSink<T>(pipeline);
            var output = (ISourceBlock<IMessage<T>>) propagator.Output;
            
            pipeline.Consumer.Subscribe<T>(
                string.IsNullOrEmpty(topic) ? TopicDefinition.FromType<T>().Topic : topic, 
                x => propagator.Input.SendAsync(x), options);
            
            return output.AsObservable();
        }*/
    }
}